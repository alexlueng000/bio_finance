from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime
from loguru import logger

from schemas import PurchaseItem


# === 成本结转底表：查询 / 更新 / 删除 ===

def query_estimate_records(product_code: str) -> List[Dict[str, Any]]:
    """
    查【成本结转底表】中：
      - product_code = 给定产品
      - status = 暂估
    要求：按销项票日期升序（FIFO）返回。
    """
    raise NotImplementedError


def update_cost_record(cost_id: str, data: Dict[str, Any]) -> None:
    """更新一条成本结转记录"""
    raise NotImplementedError


def delete_cost_record(cost_id: str) -> None:
    """删除一条成本结转记录"""
    raise NotImplementedError


# 你已经有的：插入成本结转记录
def insert_cost_record(data: Dict[str, Any]) -> None:
    raise NotImplementedError


# === 进项票库存 ===
def insert_inventory_record(data: Dict[str, Any]) -> None:
    """在【进项票库存】新增一条库存记录"""
    raise NotImplementedError


# === 发票统计（进项/销项通用） ===
def insert_invoice_stat(data: Dict[str, Any]) -> None:
    raise NotImplementedError


# === 产品主数据：进项累计 ===
def inc_product_input_qty(product_code: str, delta_qty: Decimal) -> None:
    """产品主数据中 '进项票总数量' += delta_qty"""
    raise NotImplementedError


def offset_estimates_for_product(
    product_code: str,
    qty_input: Decimal,
    invoice_info: Dict[str, Any],
) -> Decimal:
    """
    用本次进项票数量冲销【成本结转底表】中该产品的“暂估”记录。
    规则：
      - 按销项票日期正序（FIFO）
      - 若 进项数量 >= 暂估数量：原暂估记录状态改为“结转成本”，数量不变
      - 若 进项数量 <  暂估数量：拆成两条：结转成本(=进项数量) + 暂估(=原数量-进项数量)
    invoice_info 会写入所有被更新/新建的“结转成本”记录里，用于回填 J~N 字段（进项票号、供应商等）。

    返回值：本次进项票中，实际用于“结转成本”的数量（用于后面算剩余入库存量）。
    """

    remain = qty_input
    used_total = Decimal("0")

    estimates = query_estimate_records(product_code=product_code) or []
    logger.info(
        "Offset estimates: product_code={}, qty_input={}, estimate_rows={}",
        product_code, qty_input, len(estimates)
    )

    if not estimates:
        # 没有任何暂估记录 → 不冲销，全部入库存
        return Decimal("0")

    for est in estimates:
        if remain <= 0:
            break

        est_id = est["id"]
        est_qty = Decimal(str(est["qty"]))  # 你成本结转表里数量字段的 code，先用 qty 做占位
        est_status = est.get("record_type") or est.get("status", "")

        if est_status not in ("暂估", "Estimate"):
            continue  # 安全起见，只处理暂估

        if remain >= est_qty:
            # 情况①：进项 >= 暂估 → 整条转为结转成本
            new_data = est.copy()
            new_data.update(invoice_info)
            new_data["record_type"] = "结转成本"  # 或你实际用的字段名
            # 数量不变
            update_cost_record(est_id, new_data)

            used_total += est_qty
            remain -= est_qty

        else:
            # 情况②：进项 < 暂估 → 拆分为 结转成本 + 暂估
            # 先删掉原记录
            delete_cost_record(est_id)

            # 结转成本部分
            cost_part = est.copy()
            cost_part.update(invoice_info)
            cost_part["record_type"] = "结转成本"
            cost_part["qty"] = str(remain)
            insert_cost_record(cost_part)

            # 剩余暂估部分
            left_part = est.copy()
            left_part["record_type"] = "暂估"
            left_part["qty"] = str(est_qty - remain)
            # 暂估部分一般不回填进项票信息，你可以按需要决定要不要带 invoice_info
            insert_cost_record(left_part)

            used_total += remain
            remain = Decimal("0")
            break

    logger.info(
        "Offset estimates done: product_code={}, used_total={}, remain_for_stock={}",
        product_code, used_total, qty_input - used_total
    )
    return used_total


def process_purchase_item(
    item: PurchaseItem,
    *,
    invoice_no: str,
    invoice_date: datetime,
    has_related_sales: bool,
    supplier_name: Optional[str] = None,
) -> None:
    """
    单条进项票产品明细处理逻辑：
      1）写【发票统计】（进项）
      2）更新【产品主数据】进项票总数量
      3）若 has_related_sales = False → 全量入【进项票库存】
      4）若 has_related_sales = True →
           a. 用本次数量冲销/拆分【成本结转底表】中的“暂估”
           b. 剩余数量（若有）一次性追加到【进项票库存】
    """

    product_code = item.textField_mi8pp1wf   # 产品编号
    product_name = item.textField_mi8pp1we   # 产品名称
    qty_input: Decimal = item.numberField_mi8pp1wg  # 本次进项数量
    unit_price: Decimal = item.numberField_mi8pp1wh or Decimal("0")

    logger.info(
        "Process purchase item: product_code={}, name={}, qty_input={}, has_related_sales={}",
        product_code, product_name, qty_input, has_related_sales
    )

    # ==== 1. 发票统计（进项） ====
    insert_invoice_stat({
        "invoice_no": invoice_no,
        "invoice_date": invoice_date.isoformat(),
        "invoice_type": "进项",
        "supplier_name": supplier_name,
        "product_code": product_code,
        "product_name": product_name,
        "qty": str(qty_input),
        "unit_price": str(unit_price),
        "amount": str(qty_input * unit_price),
    })

    # ==== 2. 产品主数据：进项票总数量累加 ====
    inc_product_input_qty(product_code, qty_input)

    # ==== 3. 如果没有对应销项（未申请销项票） → 全量入库存 ====
    if not has_related_sales:
        logger.info(
            "No related sales for product_code={}, qty={} → 全部入进项库存",
            product_code, qty_input
        )
        insert_inventory_record({
            "product_code": product_code,
            "product_name": product_name,
            "invoice_no_in": invoice_no,
            "invoice_date_in": invoice_date.isoformat(),
            "orig_qty": str(qty_input),
            "used_qty": "0",
            "remain_qty": str(qty_input),
            "status": "未使用",
            # 你库存表还有什么字段，在这里一起补
        })
        return

    # ==== 4. 已有销项 → 先冲销暂估，再把剩余数量入库存 ====
    invoice_info_for_cost = {
        # 用于回填成本结转表 J~N 字段的进项信息：
        # 这些 key 要换成你成本结转底表的字段 code
        "input_invoice_no": invoice_no,
        "input_invoice_date": invoice_date.isoformat(),
        "supplier_name": supplier_name,
    }

    used_for_cost = offset_estimates_for_product(
        product_code=product_code,
        qty_input=qty_input,
        invoice_info=invoice_info_for_cost,
    )

    remain_for_stock = qty_input - used_for_cost

    logger.info(
        "Purchase item result: product_code={}, qty_input={}, used_for_cost={}, remain_for_stock={}",
        product_code, qty_input, used_for_cost, remain_for_stock
    )

    # 剩余数量（可能是 0）入进项库存
    if remain_for_stock > 0:
        insert_inventory_record({
            "product_code": product_code,
            "product_name": product_name,
            "invoice_no_in": invoice_no,
            "invoice_date_in": invoice_date.isoformat(),
            "orig_qty": str(remain_for_stock),
            "used_qty": "0",
            "remain_qty": str(remain_for_stock),
            "status": "未使用",
        })

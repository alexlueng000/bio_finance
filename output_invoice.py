from decimal import Decimal
from typing import List, Dict, Any
import json
import requests

from loguru import logger


from schemas import SalesItem
from yida_client import get_dingtalk_access_token

from config import input_invoice_inventory_table, cost_carry_forward_table
from config import SEARCH_REQUEST_URL, UPDATE_INSTANCE_URL, INSERT_INSTANCE_URL


# === 进项票库存 ===
# 根据产品编号获取进项票库存中的该产品的所有记录
def get_inventory_for_product(product_code: str) -> List[Dict[str, Any]]:

    product_search_conditions = {
        "textField_mhlqrhyy": product_code
    }

    access_token = get_dingtalk_access_token()
    
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json"
    }

    print(product_search_conditions)

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",             # 固定为 APP（宜搭应用）
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",  # 宜搭 System Token
        "formUuid": input_invoice_inventory_table,
        # "pageSize": 20,
        # "pageNumber": 1,
        "dataCreateFrom": 0,          # 可选：0=全部；1=我创建；2=我参与
        "userId": "203729096926868966",           # 这里换成有权限访问该宜搭应用/表单的用户
        # 搜索条件
        "searchFieldJson": json.dumps(product_search_conditions, ensure_ascii=False),
     }

    try:
        resp = requests.post(SEARCH_REQUEST_URL, headers=headers, data=json.dumps(body))
        # resp.raise_for_status()
        data = resp.json()
        print(data)
        return data

    except requests.HTTPError as e:
        print(f"❌ HTTP错误：{e}，响应：{getattr(e.response, 'text', '')}")
    except Exception as e:
        print(f"❌ 请求失败：{e}")


# 更新
def update_inventory_row(inv_id: str, used_qty: Decimal, remain_qty: Decimal, status: str) -> None:
    """
    更新【进项票库存】记录：
      - numberField_mhlqrhyt: 已结转数量
      - numberField_mhlqrhyu: 剩余可用数量
      - radioField_mhlqrhyv: 状态（未使用/部分使用/已用完）
    inv_id 就是 formInstanceId（比如 FINST-OC666271AG41K8WQPKUMJDU0...）
    """

    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    # 只更新你关心的几个字段即可，其他字段宜搭会按原有数据保留
    form_data = {
        "numberField_mhlqrhyt": float(used_qty),      # 已结转数量
        "numberField_mhlqrhyu": float(remain_qty),    # 剩余可用数量
        "radioField_mhlqrhyv": status,                # 未使用 / 部分使用 / 已用完
    }

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "formUuid": input_invoice_inventory_table,
        "formInstanceId": inv_id,
        # "targetTenantId": YIDA_TENANT_ID,     # 如果你现在没这个值，可以先去掉这一行试；报错再补
        "userId": "203729096926868966",
        "updateFormDataJson": json.dumps(form_data, ensure_ascii=False),
    }

    logger.info(
        "[update_inventory_row] inv_id={}, used_qty={}, remain_qty={}, status={}, body={}",
        inv_id, used_qty, remain_qty, status, body,
    )

    try:
        resp = requests.put(UPDATE_INSTANCE_URL, headers=headers, data=json.dumps(body))
        resp.raise_for_status()
        data = resp.json()
        logger.info("[update_inventory_row] success, resp={}", data)
    except requests.exceptions.HTTPError as e:
        # 打印一下钉钉返回的错误 body，方便你调字段名
        logger.error("[update_inventory_row] HTTPError: {}, body={}", e, getattr(e.response, "text", ""))
        raise
    except Exception as e:
        logger.error("[update_inventory_row] failed: {}", e)
        raise

def new_cost_record(date, product_name, batch_no, customer, invoice_type, invoice_no, qty, sales_order_no, status):

    data = {
        "dateField_mh8x8uxc": date, # 开票日期
        "textField_mh8x8uwz": product_name, # 品名
        "textField_mh8x8ux0": batch_no, # 批次号
        "textField_mh8x8ux1": customer, # 客户
        "textField_mh8x8ux8": invoice_type, # 发票类别
        "textField_mh8x8ux9": invoice_no, # 发票号
        "textField_mh8x8uxa": qty, # 数量
        "textField_mh8x8uxb": sales_order_no, # 销售订单号
        "textField_mh8x8uxk": status, # 状态
    }

    return data


def build_cost_records_from_sales(items: list[SalesItem]) -> list[dict]:
    records: list[dict] = []
    for item in items:
        # 直接用毫秒时间戳
        date_ms = item.dateField_mhd23657

        qty_str = str(item.numberField_m7ecqbog)

        record = new_cost_record(
            date=date_ms,
            product_name=item.textField_ll5xce5e,
            batch_no=item.textField_m7ecqboh,
            customer=item.textField_mhd23658,
            invoice_type=item.textField_mhd23659 or "",
            invoice_no=item.textField_mhd2365a,
            qty=qty_str,
            sales_order_no=item.textField_mhd23655,
            status="未结转",
        )
        records.append(record)

    return records


# === 成本结转底表 ===
# 按产品明细生成一条结转成本记录

def insert_cost_record(records: List[Dict[str, Any]]) -> None:
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    body = {
        "noExecuteExpression": True,
        "asynchronousExecution": False,
        "keepRunningAfterException": True,
        "formUuid": cost_carry_forward_table,
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "userId": "203729096926868966",
        # 👇 关键：这里必须是“字符串列表”
        "formDataJsonList": [
            json.dumps(r, ensure_ascii=False) for r in records
        ],
    }

    logger.info("[insert_cost_record] request body=%s", body)

    resp = requests.post(INSERT_INSTANCE_URL, headers=headers, data=json.dumps(body))
    text = resp.text
    logger.info(
        "[insert_cost_record] http_status=%s, raw_body=%s",
        resp.status_code,
        text,
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            err_json = resp.json()
        except Exception:
            err_json = None
        logger.error(
            "[insert_cost_record] HTTPError status=%s, body_text=%s, body_json=%s",
            resp.status_code,
            text,
            err_json,
        )
        raise


# 处理一条销项票明细
def process_sales_item(item: SalesItem) -> None:
    """
    单条销项明细处理逻辑：
      1）查进项票库存（FIFO）
      2）按库存情况拆分为 结转成本 / 暂估
      3）写【成本结转底表】
      4）同步扣减【进项票库存】
      5）写【发票统计】
      6）更新【产品主数据】销项票总数量
    """

    # ==== 基础字段 ====
    # 产品编号 —— 如果你改了字段名，这里一起改
    product_code = item.textField_mhd4ta0f
    product_name = item.textField_ll5xce5e   # 品名

    apply_qty: Decimal = item.numberField_m7ecqbog  # 本次销项数量（瓶）

    sales_invoice_no = item.textField_mhd2365a      # 销项票发票号
    sales_invoice_date = item.dateField_mhd23657    # datetime
    customer_name = item.textField_mhd23658         # 客户名称

    logger.info(
        "Process sales item: product_code={}, name={}, apply_qty={}",
        product_code, product_name, apply_qty
    )

    # ==== 1. 查询该产品的进项票库存（只取 remain_qty > 0，按进项票日期升序 → FIFO）====
    inventory_rows = get_inventory_for_product(product_code) or []
    available_qty = sum(Decimal(str(r["remain_qty"])) for r in inventory_rows)

    logger.info(
        "Inventory for {}: available_qty={}, rows={}",
        product_code, available_qty, len(inventory_rows)
    )

    # ==== 2. 根据库存情况决定 结转成本数量 / 暂估数量 ====
    if not inventory_rows:
        # 完全没有进项库存 → 全部暂估
        logger.warning(
            "No input inventory for product_code={}, apply_qty={} → 全部暂估",
            product_code, apply_qty
        )
        cost_qty = Decimal("0")
        estimate_qty = apply_qty
    else:
        if available_qty >= apply_qty:
            # 库存充足：全额结转成本
            cost_qty = apply_qty
            estimate_qty = Decimal("0")
        else:
            # 库存不足：库存部分结转成本 + 其余暂估
            cost_qty = available_qty
            estimate_qty = apply_qty - available_qty

    # ==== 3. 成本结转底表：结转成本记录 ====
    if cost_qty > 0:
        insert_cost_record({
            "product_code": product_code,
            "product_name": product_name,
            "sales_invoice_no": sales_invoice_no,
            "sales_invoice_date": sales_invoice_date.isoformat(),
            "qty": str(cost_qty),
            "record_type": "结转成本",
            # 下面这些字段你根据成本结转底表的实际字段再补：
            # "customer_name": customer_name,
            # "sales_order_no": item.XXX,
            # "unit_cost": str(item.numberField_mims71hm),
            # "total_cost": ...
        })

        # ==== 3.1 FIFO 扣减库存 ====
        remaining_to_consume = cost_qty

        for row in inventory_rows:
            if remaining_to_consume <= 0:
                break

            row_remain = Decimal(str(row["remain_qty"]))
            row_used = Decimal(str(row.get("used_qty", "0")))

            use_here = min(row_remain, remaining_to_consume)

            new_used = row_used + use_here
            new_remain = row_remain - use_here

            if new_used == 0:
                status = "未使用"
            elif new_remain == 0:
                status = "已用完"
            else:
                status = "部分使用"

            update_inventory_row(
                inv_id=row["id"],
                used_qty=new_used,
                remain_qty=new_remain,
                status=status,
            )

            remaining_to_consume -= use_here

    # ==== 4. 成本结转底表：暂估记录 ====
    if estimate_qty > 0:
        insert_cost_record({
            "product_code": product_code,
            "product_name": product_name,
            "sales_invoice_no": sales_invoice_no,
            "sales_invoice_date": sales_invoice_date.isoformat(),
            "qty": str(estimate_qty),
            "record_type": "暂估",
            # 同样可带 customer_name / sales_order_no 等
        })


    logger.info(
        "Finished sales item: product_code={}, apply_qty={}, cost_qty={}, estimate_qty={}",
        product_code, apply_qty, cost_qty, estimate_qty
    )

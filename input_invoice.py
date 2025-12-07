from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime
from loguru import logger
import json
import requests

from schemas import PurchaseItem

from config import input_invoice_inventory_table, cost_carry_forward_table
from config import UPDATE_INSTANCE_URL, SEARCH_REQUEST_URL, INSERT_INSTANCE_URL
from yida_client import get_dingtalk_access_token

from utils import new_cost_record, insert_cost_record


# === 成本结转底表：查询 / 更新 / 删除 ===
def query_estimate_records(product_code: str) -> List[Dict[str, Any]]:
    """
    查【成本结转底表】中：
      - product_code = 给定产品
      - status = 暂估
    要求：按销项票日期升序（FIFO）返回。
    """
    record_search_conditions = {
        "textField_mhd56jjz": product_code,
        "textField_mh8x8uxk": "暂估"
    }

    access_token = get_dingtalk_access_token()
    
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json"
    }

    print(record_search_conditions)

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",             # 固定为 APP（宜搭应用）
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",  # 宜搭 System Token
        "formUuid": cost_carry_forward_table,
        # "pageSize": 20,
        # "pageNumber": 1,
        "dataCreateFrom": 0,          # 可选：0=全部；1=我创建；2=我参与
        "userId": "203729096926868966",           # 这里换成有权限访问该宜搭应用/表单的用户
        # 搜索条件
        "searchFieldJson": json.dumps(record_search_conditions, ensure_ascii=False),
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


def update_cost_record(cost_id: str, data: Dict[str, Any]) -> None:
    """
    将成本结转底表中指定记录从“暂估”更新为“结转成本/已收票”。
    支持同步修改数量、金额等字段。
    用于进项到票后对原暂估记录进行状态回写。
    """
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    # 只更新你关心的几个字段即可，其他字段宜搭会按原有数据保留
    form_data = {
        "textField_mh8x8uxk": "已收票",
        "dateField_mh8x8uxt": data["进项票-开票日期"],
        "textField_mh8x8uxr": data["关联进项票发票号"],
        "textField_mh8x8uxs": data["采购订单号"],
    }

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "formUuid": cost_carry_forward_table,
        "formInstanceId": cost_id,
        # "targetTenantId": YIDA_TENANT_ID,     # 如果你现在没这个值，可以先去掉这一行试；报错再补
        "userId": "203729096926868966",
        "updateFormDataJson": json.dumps(form_data, ensure_ascii=False),
    }

    logger.info(
        "[update_cost_record更新成本结转底表] cost_id={}, body={}",
        cost_id, body,
    )

    try:
        resp = requests.put(UPDATE_INSTANCE_URL, headers=headers, data=json.dumps(body))
        resp.raise_for_status()
        data = resp.json()
        logger.info("[update_cost_record更新成本结转底表] success, resp={}", data)
    except requests.exceptions.HTTPError as e:
        # 打印一下钉钉返回的错误 body，方便你调字段名
        logger.error("[update_cost_record更新成本结转底表] HTTPError: {}, body={}", e, getattr(e.response, "text", ""))
        raise
    except Exception as e:
        logger.error("[update_cost_record更新成本结转底表] failed: {}", e)
        raise

# 新建进项票库存数据
# -------- new_inventory_record：真正映射到宜搭字段 --------
def new_inventory_record(
    product_code: str,
    product_name: str,
    qty: Decimal,
    unit_price: Decimal,
    invoice_info: Dict[str, Any],
) -> Dict[str, Any]:
    """
    组装一条【进项票库存】记录（字典），字段名全部是这张表在宜搭里的唯一标识。
    """

    qty_str = str(qty)
    unit_price_str = str(unit_price)

    invoice_no = invoice_info.get("invoice_no", "")
    invoice_date_ms = invoice_info.get("invoice_date_ms")   # 进项票日期（毫秒）
    spec = invoice_info.get("spec") or "--"
    category = invoice_info.get("category") or "--"
    unit = invoice_info.get("unit") or "--"
    origin_link = invoice_info.get("origin_link", "")

    return {
        # 剩余可用数量
        "numberField_mhlqrhys": qty_str,
        # 已结转数量
        "numberField_mhlqrhyt": "0",
        # 发票-产品数量
        "numberField_mhlqrhyu": qty_str,
        # 原流程链接
        "textField_mhlqrhyo": origin_link,
        # 状态
        "radioField_mhlqrhyv": "未使用",
        # 发票号码
        "textField_mhlqrz3": invoice_no,
        # 进项开票日期
        "dateField_mhlqrz2": invoice_date_ms,
        # 产品名称
        "textField_mhlqrhyx": product_name,
        # 采购单价
        "numberField_mhlqrz1": unit_price_str,
        # 产品规格
        "textField_mhlqrz4": spec,
        # 产品分类
        "textField_mhlqrz5": category,
        # 单位
        "textField_mhlqrz6": unit,
        # 产品编号
        "textField_mhlqrhyy": product_code,
    }


def insert_inventory_record(record: Dict[str, Any]) -> None:
    """
    将一条【进项票库存】记录写入宜搭。
    """
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "formUuid": input_invoice_inventory_table,
        "userId": "203729096926868966",
        # "formDataJson": json.dumps(record, ensure_ascii=False),
        "formDataJsonList": [
            json.dumps(record, ensure_ascii=False)
        ],
    }

    logger.info("[insert_inventory_record] request body={}", body)

    resp = requests.post(INSERT_INSTANCE_URL, headers=headers, data=json.dumps(body))
    text = resp.text
    logger.info(
        "[insert_inventory_record] http_status={}, raw_body={}",
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
            "[insert_inventory_record] HTTPError status={}, body_text={}, body_json={}",
            resp.status_code,
            text,
            err_json,
        )
        raise


# -------- offset_estimates_for_product 里新增库存那一段 --------
def _append_inventory_by_new_record(
    product_code: str,
    qty_for_inventory: Decimal,
    invoice_info: Dict[str, Any],
) -> None:
    """
    没有暂估，或者冲销完还有剩余时，用本次进项剩余数量生成一条【进项票库存】记录。
    """
    product_name = invoice_info.get("product_name", "")
    unit_price = invoice_info.get("unit_price") or Decimal("0")

    logger.warning(
        "[_append_inventory_by_new_record] code={}, qty={}, spec={}, category={}, unit={}",
        product_code,
        qty_for_inventory,
        invoice_info.get("spec"),
        invoice_info.get("category"),
        invoice_info.get("unit"),
    )

    record = new_inventory_record(
        product_code=product_code,
        product_name=product_name,
        qty=qty_for_inventory,
        unit_price=unit_price,
        invoice_info=invoice_info,
    )

    logger.warning("[_append_inventory_by_new_record] record={}", record)
    insert_inventory_record(record)



def offset_estimates_for_product(
    product_code: str,
    qty_input: Decimal,
    invoice_info: Dict[str, Any],
) -> Decimal:
    """
    用本次进项票数量冲销【成本结转底表】中该产品的“暂估”记录，
    并将剩余数量追加到【进项票库存】。

    返回值：本次进项中实际用于冲销暂估的数量。
    """

    remaining = Decimal(qty_input or 0)
    if remaining <= 0:
        logger.info(
            "[offset_estimates_for_product] qty_input<=0, skip. product_code={}, qty_input={}",
            product_code, qty_input,
        )
        return Decimal("0")

    # 1. 查询该产品的所有“暂估”记录
    raw = query_estimate_records(product_code)
    if not raw:
        logger.info(
            "[offset_estimates_for_product] no estimate records, product_code={}",
            product_code,
        )
        # 没有暂估，后面直接把全部数量入库存
        remain_for_inventory = remaining
        if remain_for_inventory > 0:
            _append_inventory_by_new_record(product_code, remain_for_inventory, invoice_info)
        return Decimal("0")

    # 解析宜搭返回结构
    records: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        if isinstance(raw.get("data"), list):
            records = raw["data"]
        elif isinstance(raw.get("result"), dict) and isinstance(raw["result"].get("data"), list):
            records = raw["result"]["data"]
        elif isinstance(raw.get("result"), dict) and isinstance(raw["result"].get("list"), list):
            records = raw["result"]["list"]
        elif isinstance(raw.get("body"), list):
            records = raw["body"]
    elif isinstance(raw, list):
        records = raw

    if not records:
        logger.info(
            "[offset_estimates_for_product] estimate list empty after extraction, raw={}",
            raw,
        )
        remain_for_inventory = remaining
        if remain_for_inventory > 0:
            _append_inventory_by_new_record(product_code, remain_for_inventory, invoice_info)
        return Decimal("0")

    # 2. 按销项票日期 FIFO
    def _get_sales_date(rec: Dict[str, Any]) -> int:
        v = rec.get("dateField_mh8x8uxc")
        try:
            return int(v)
        except Exception:
            return 0

    records_sorted = sorted(records, key=_get_sales_date)

    used_total = Decimal("0")
    logger.info(
        "[offset_estimates_for_product] start offset, product_code={}, qty_input={}, estimate_records=%d",
        product_code, qty_input, len(records_sorted),
    )

    for rec in records_sorted:
        if remaining <= 0:
            break

        cost_id = rec.get("formInstanceId") or rec.get("id")
        if not cost_id:
            logger.warning("[offset_estimates_for_product] record without id: {}", rec)
            continue

        try:
            est_qty = Decimal(str(rec.get("textField_mh8x8uxa") or "0"))
        except Exception:
            logger.error("[offset_estimates_for_product] invalid est qty for record={}", rec)
            continue

        if est_qty <= 0:
            continue

        sales_date = rec.get("dateField_mh8x8uxc")
        product_name = rec.get("textField_mh8x8uwz", "")
        batch_no = rec.get("textField_mh8x8ux0", "")
        customer = rec.get("textField_mh8x8ux1", "")
        invoice_type = rec.get("textField_mh8x8ux8", "")
        sales_invoice_no = rec.get("textField_mh8x8ux9", "")
        sales_order_no = rec.get("textField_mh8x8uxb", "")

        logger.info(
            "[offset_estimates_for_product] record id={}, est_qty={}, remaining={}",
            cost_id, est_qty, remaining,
        )

        # 情况①：进项数量 >= 暂估数量 → 原记录改为“已收票”
        if remaining >= est_qty:
            used_here = est_qty
            remaining -= used_here
            used_total += used_here

            update_data = dict(invoice_info or {})
            update_data["textField_mh8x8uxk"] = "已收票"

            logger.info(
                "[offset_estimates_for_product] full offset: cost_id={}, used={}, remaining={}",
                cost_id, used_here, remaining,
            )
            update_cost_record(cost_id, update_data)

        else:
            # 情况②：进项数量 < 暂估数量 → 拆分
            used_here = remaining
            remaining = Decimal("0")
            used_total += used_here

            remain_est = est_qty - used_here

            # ②-a 原记录改为剩余暂估
            update_data_est = {
                "textField_mh8x8uxa": str(remain_est),
                "textField_mh8x8uxk": "暂估",
            }
            logger.info(
                "[offset_estimates_for_product] partial offset: cost_id={}, used={}, remain_est={}",
                cost_id, used_here, remain_est,
            )
            update_cost_record(cost_id, update_data_est)

            # ②-b 新增一条“已收票”记录
            new_data = new_cost_record(
                date=sales_date,
                product_name=product_name,
                batch_no=batch_no,
                customer=customer,
                invoice_type=invoice_type,
                invoice_no=sales_invoice_no,
                qty=str(used_here),
                sales_order_no=sales_order_no,
                status="已收票",
            )
            new_data.update(invoice_info or {})

            logger.info(
                "[offset_estimates_for_product] create new cost_record for used part: {}",
                new_data,
            )
            insert_cost_record([new_data])
            break  # 进项数量已经用完

    # 3. 剩余数量入【进项票库存】
    remain_for_inventory = qty_input - used_total
    if remain_for_inventory > 0:
        _append_inventory_by_new_record(product_code, remain_for_inventory, invoice_info)

    logger.info(
        "[offset_estimates_for_product] finish: product_code={}, qty_input={}, used_total={}, remain_for_inventory={}",
        product_code, qty_input, used_total, remain_for_inventory,
    )

    return used_total


def process_purchase_item(
    item: PurchaseItem,
    *,
    invoice_no: str,
    invoice_date: datetime,
) -> None:
    """
    单条进项票产品明细处理逻辑：
      调用 offset_estimates_for_product：
        - 若该产品有“暂估”记录 → 冲销/拆分 + 剩余数量入【进项票库存】
        - 若该产品没有“暂估”记录 → 本次进项数量全量入【进项票库存】
    """

    product_code = item.textField_mi8pp1wf          # 产品编号
    product_name = item.textField_mi8pp1we          # 产品名称
    qty_input: Decimal = item.numberField_mi8pp1wg  # 本次进项数量
    unit_price: Decimal = item.numberField_mi8pp1wh or Decimal("0")

    spec = item.textField_mi8pp1wi                  # 产品规格
    category = item.textField_mi8pp1wj              # 产品分类
    unit = item.textField_mi8pp1wk                  # 单位

    invoice_date_ms = int(invoice_date.timestamp() * 1000)

    invoice_info = {
        "product_name": product_name,
        "unit_price": unit_price,
        "invoice_no": invoice_no,
        "invoice_date_ms": invoice_date_ms,
        "spec": spec,
        "category": category,
        "unit": unit,
        "origin_link": "",
    }

    logger.warning(
        "[process_purchase_item] code={}, qty={}, spec={}, category={}, unit={}",
        product_code, qty_input, spec, category, unit
    )

    used_for_cost = offset_estimates_for_product(
        product_code=product_code,
        qty_input=qty_input,
        invoice_info=invoice_info,
    )

    logger.info(
        "Purchase item result: product_code=%s, qty_input=%s, used_for_cost=%s, remain_for_stock=%s",
        product_code, qty_input, used_for_cost, qty_input - used_for_cost
    )
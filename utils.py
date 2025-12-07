from typing import List, Dict, Any
import json
import requests
from loguru import logger
from yida_client import get_dingtalk_access_token
from config import cost_carry_forward_table, INSERT_INSTANCE_URL



# 新建结转成本底表数据
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

def _json_default(o):
    # 所有 Decimal → 字符串（或者 float，看你业务习惯）
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


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
            json.dumps(r, ensure_ascii=False, default=_json_default)
            for r in records
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

from typing import List, Dict, Any, Optional
from decimal import Decimal
import json
import requests
from loguru import logger
from yida_client import get_dingtalk_access_token
from config import cost_carry_forward_table, product_info_table, INSERT_INSTANCE_URL, SEARCH_REQUEST_URL, UPDATE_INSTANCE_URL


def _json_default(o):
    # 所有 Decimal → 字符串（或者 float，看你业务习惯）
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

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

    logger.info("[insert_cost_record成本结转底表] request body={}", body)

    resp = requests.post(INSERT_INSTANCE_URL, headers=headers, data=json.dumps(body))
    text = resp.text
    logger.info(
        "[insert_cost_record成本结转底表] http_status={}, raw_body={}",
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
            "[insert_cost_record] HTTPError status={}, body_text={}, body_json={}",
            resp.status_code,
            text,
            err_json,
        )
        raise


# 新建产品信息表数据 
def new_product_info(product_name, product_no, input_total, output_total):
    data = {
        "textField_miyahqml": product_name,
        "textField_miyahqmm": product_no,
        "textField_miyahqmn": input_total,
        "textField_miyahqmk": output_total,
    }

    return data 



# 查询产品信息
def get_product_info(product_no: str) -> Optional[Dict[str, Any]]:
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    logger.info("[get_product_info产品信息表] product_no={}", product_no)

    body = {
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "formUuid": product_info_table,
        "userId": "203729096926868966",
        "searchFieldJson": json.dumps(
            {"textField_miyahqmm": product_no},  # 产品编号
            ensure_ascii=False
        ),
    }

    resp = requests.post(SEARCH_REQUEST_URL, headers=headers, data=json.dumps(body))
    text = resp.text
    logger.info("[get_product_info产品信息表] http_status={}, raw_body={}",
                resp.status_code, text)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            err_json = resp.json()
        except Exception:
            err_json = None
        logger.error(
            "[get_product_info产品信息表] HTTPError status={}, body_text={}, body_json={}",
            resp.status_code,
            text,
            err_json,
        )
        raise

    # ✅ 正常返回：找到第一条记录 / 找不到返回 None
    data = (resp.json().get("result") or {}).get("data") or []
    return data[0] if data else None

    
# 新增产品信息表数据
def insert_product_into(product: Dict[str, Any]) -> None:
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    body = {
        "noExecuteExpression": True,
        "asynchronousExecution": False,
        "keepRunningAfterException": True,
        "formUuid": product_info_table,
        "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
        "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
        "userId": "203729096926868966",
        # ✅ 注意：这里是“列表里包一条 JSON 字符串”
        "formDataJsonList": [
            json.dumps(product, ensure_ascii=False, default=_json_default),
        ],
    }

    logger.info("[insert_product_into产品信息表] request body={}", body)

    resp = requests.post(INSERT_INSTANCE_URL, headers=headers, data=json.dumps(body))
    text = resp.text
    logger.info(
        "[insert_product_into产品信息表] http_status={}, raw_body={}",
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
            "[insert_product_into产品信息表] HTTPError status={}, body_text={}, body_json={}",
            resp.status_code,
            text,
            err_json,
        )
        raise



# 更新产品信息表
def update_product_info_table(product_no: str, invoice_type: str, invoice_number: int) -> None:
    """
    更新【产品信息表】中的记录。
    product_no    产品编号
    invoice_type  票据类型："进项票"/"销项票"
    invoice_number 新增数量
    """
    access_token = get_dingtalk_access_token()
    headers = {
        "x-acs-dingtalk-access-token": access_token,
        "Content-Type": "application/json",
    }

    # ============= 1. 先查已有记录 =============
    instance = get_product_info(product_no)

    if instance:
        form_instance_id = instance.get("formInstanceId")
        form_data_old = instance.get("formData") or {}

        product_name = form_data_old.get("textField_miyahqml") or ""  # 产品名称
        input_total = int(form_data_old.get("textField_miyahqmn") or 0)   # 进项累计
        output_total = int(form_data_old.get("textField_miyahqmk") or 0)  # 销项累计
    else:
        # 没有记录 → 视为新建
        form_instance_id = None
        product_name = ""      # 如果有产品字典，这里可以反查；现在先为空
        input_total = 0
        output_total = 0

    # ============= 2. 按票据类型累加数量 =============
    if invoice_type == "进项票":
        input_total += int(invoice_number)
    elif invoice_type == "销项票":
        output_total += int(invoice_number)
    else:
        raise ValueError(f"未知的 invoice_type: {invoice_type!r}，只能是 '进项票' 或 '销项票'")

    # 构造新的表单数据
    form_data = new_product_info(
        product_name=product_name,
        product_no=product_no,
        input_total=input_total,
        output_total=output_total,
    )

    if form_instance_id:
        # ============= 3A. 更新已有记录 =============
        body = {
            "appType": "APP_JSXMR8UNH0GRZUNHO3Y2",
            "systemToken": "RUA667B1BS305G1LK1HTH4U1WJS73Z1RVKBHMC29",
            "formUuid": product_info_table,
            "formInstanceId": form_instance_id,
            "userId": "203729096926868966",
            "updateFormDataJson": json.dumps(form_data, ensure_ascii=False),
        }

        logger.info(
            "[update_product_info_table更新产品信息表] update product_no={}, form_instance_id={}, form_data={}",
            product_no, form_instance_id, form_data,
        )

        try:
            resp = requests.put(UPDATE_INSTANCE_URL, headers=headers, data=json.dumps(body))
            resp.raise_for_status()
            logger.info("[update_product_info_table更新产品信息表] update success, resp={}", resp.json())
        except requests.exceptions.HTTPError as e:
            logger.error("[update_product_info_table更新产品信息表] HTTPError on update: {}, body={}",
                         e, getattr(e.response, "text", ""))
            raise
        except Exception as e:
            logger.error("[update_product_info_table更新产品信息表] update failed: {}", e)
            raise

    else:
        # ============= 3B. 新建记录 =============
        logger.info(
            "[update_product_info_table新建产品] create product_no={}, form_data={}",
            product_no, form_data,
        )
        insert_product_into(form_data)

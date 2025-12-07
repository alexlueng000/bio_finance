# app.py
import os
import json
from typing import Dict, Any, Optional
from datetime import datetime

from urllib.parse import parse_qs, unquote

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import JSONResponse
from loguru import logger
from dotenv import load_dotenv

from yida_client import get_dingtalk_access_token

from schemas import PurchaseList, SalesList

from output_invoice import process_sales_item
from input_invoice import process_purchase_item


# 加载 .env 里的配置（可选）
load_dotenv()

# 简单的签名/授权校验（宜搭那边你可以在 Header 里带这个）
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

app = FastAPI(title="Yida Invoice Callback Service")


@app.on_event("startup")
async def startup_event():
    logger.info("=== Yida Callback Service started ===")
    if WEBHOOK_TOKEN:
        logger.info("Webhook token enabled")
    else:
        logger.warning("WEBHOOK_TOKEN 未配置，目前接口无鉴权（仅测试用）")


@app.get("/")
async def health_check():
    """健康检查，方便你在浏览器/监控里看服务在不在"""
    return {"status": "ok"}

@app.get("/test-token")
def test_token():
    token = get_dingtalk_access_token()
    return {"token": token}


# 进项票录入接口（进项管理申请）
@app.post("/get_purchase_list")
async def get_purchase_list(request: Request):
    # ① 原始 body（URL 编码 + 字符串）
    raw_body = (await request.body()).decode("utf-8")
    logger.warning("【① Raw Body 原始内容】\n{}", raw_body)

    # ② 解析 URL form
    form = parse_qs(raw_body)
    logger.warning("【② Parsed Form 解析后】\n{}", form)

    raw_items = form.get("purchase_items", ["[]"])[0]

    # ③ URL decode 后的 JSON 字符串
    raw_items = unquote(raw_items)
    logger.warning("【③ Decoded JSON String 解码后 JSON 字符串】\n{}", raw_items)

    # ④ JSON 解析为 Python 列表
    items = json.loads(raw_items)
    logger.warning("【④ Python Parsed JSON 解析后的列表】\n{}", items)

    # ⑤ Pydantic 校验
    pl = PurchaseList(purchase_items=items)

    logger.warning("【⑤ Pydantic Model Parsed Items】")
    for i, item in enumerate(pl.purchase_items):
        logger.warning("Item #{}: {}", i, item.model_dump())

    # =============================
    # 🔥 真正执行业务逻辑
    # =============================

    for item in pl.purchase_items:

        # --- 从明细行取发票号 ---
        invoice_no = item.textField_miu32cdl or ""

        # --- 日期从毫秒转 datetime ---
        if item.dateField_miu32cdo:
            invoice_date = datetime.fromtimestamp(item.dateField_miu32cdo / 1000)
        else:
            invoice_date = datetime.now()

        logger.warning(
            "【执行 process_purchase_item】：product_code={}, qty={}, invoice_no={}, invoice_date={}",
            item.textField_mi8pp1wf,
            item.numberField_mi8pp1wg,
            invoice_no,
            invoice_date,
        )

        process_purchase_item(
            item,
            invoice_no=invoice_no,
            invoice_date=invoice_date,
        )

    return {
        "message": "进项票处理完成",
        "count": len(pl.purchase_items),
    }

# 销项票录入接口（开票管理申请）
@app.post("/get_sales_list")
async def get_sales_list(
    sales_list: Optional[str] = Form(None),
    request: Request = None,
):
    """
    销项票录入接口（开票管理申请）：
    - 宜搭回调：application/x-www-form-urlencoded，字段名为 sales_list
    - Swagger 调试：直接在 Form 里粘 JSON 数组
    """

    # 1. 优先走 Form（Swagger / 正常表单）
    if sales_list is not None:
        raw_items = sales_list
        logger.info("[get_sales_list开票管理申请] from Form sales_list=%s", raw_items)
    else:
        # 2. 兜底：老的 raw body 解析（目前宜搭就是这么传的）
        raw_body = (await request.body()).decode("utf-8")
        # logger.info("[Raw Body UTF-8] %s", raw_body)

        form = parse_qs(raw_body)
        # logger.info("[Parsed Form] %s", form)

        raw_items = form.get("sales_list", ["[]"])[0]
        logger.info("[get_sales_list开票管理申请] from Body sales_list=%s", raw_items)

    # 一般不需要，但留着不犯错
    raw_items = unquote(raw_items)

    try:
        items = json.loads(raw_items)
    except Exception as e:
        logger.error("[get_sales_list开票管理申请] json.loads failed: %s, raw_items=%s", e, raw_items)
        return {"ok": False, "msg": "invalid sales_list json"}

    logger.info("[Final Parsed JSON] %s", items)

    try:
        sl = SalesList(sales_items=items)
    except Exception as e:
        logger.error("[get_sales_list开票管理申请] SalesList validation failed: %s", e)
        return {"ok": False, "msg": "invalid sales_items schema"}

    # 逐条处理
    for item in sl.sales_items:
        process_sales_item(item)

    return {"ok": True, "count": len(sl.sales_items)}

# 方便直接 python app.py 跑，不一定非要用命令行
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8001)),
        reload=True,
    )

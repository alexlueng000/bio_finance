# app.py
import os
import json
from typing import Dict, Any

from urllib.parse import parse_qs, unquote

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
from dotenv import load_dotenv

from yida_client import get_dingtalk_access_token

from schemas import PurchaseList, SalesList

from output_invoice import process_sales_item, build_cost_records_from_sales, insert_cost_record




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

@app.post("/yida/invoice-input-callback")
async def invoice_input_callback(request: Request):
    """
    进项票录入审批通过后，宜搭调用的回调入口。
    后面所有业务逻辑都从这里发散。
    """
    # 1. 简单鉴权（可选，但强烈建议上线前必须开）
    if WEBHOOK_TOKEN:
        token = request.headers.get("X-Webhook-Token")
        if token != WEBHOOK_TOKEN:
            logger.warning(f"无效的 Webhook Token: {token}")
            raise HTTPException(status_code=401, detail="Unauthorized")

    # 2. 解析请求体
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception as e:
        logger.exception("解析 JSON 失败")
        raise HTTPException(status_code=400, detail="Invalid JSON") from e

    # 3. 打日志，先看清宜搭到底给你什么
    logger.info("收到宜搭回调: {}", payload)

    # 理论上这里你会有 bizObjectId / formUuid / spaceId 等
    # 先从 payload 里抢这几个字段，如果没有就记日志但不强依赖
    biz_object_id = payload.get("bizObjectId") or payload.get("bizObjectID")
    form_uuid = payload.get("formUuid")
    space_id = payload.get("spaceId")

    logger.info(
        "解析到关键字段 bizObjectId={}, formUuid={}, spaceId={}",
        biz_object_id,
        form_uuid,
        space_id,
    )

    # 4. TODO：这里后面会接你的业务逻辑
    #    比如：
    #    from logic_invoice import process_invoice_input
    #    process_invoice_input(biz_object_id=biz_object_id, form_uuid=form_uuid, raw_payload=payload)
    #
    # 现在先啥都不做，只是回 success，让宜搭那边流程能走完。

    return JSONResponse({"success": True, "msg": "callback received"})


# 进项票录入接口
@app.post("/get_purchase_list")
async def get_purchase_list(request: Request):
    raw_body = (await request.body()).decode("utf-8")
    logger.info("[Raw Body UTF-8] {}", raw_body)

    # ⬇⬇⬇ 关键：解析表单格式 ⬇⬇⬇
    form = parse_qs(raw_body)
    # form 结构变成：
    # { "purchase_items": ["[{...},{...}]" ] }

    logger.info("[Parsed Form] {}", form)

    # 取出 purchase_items 字符串
    raw_items = form.get("purchase_items", ["[]"])[0]

    # URL decode 一下
    raw_items = unquote(raw_items)

    logger.info("[Decoded JSON String] {}", raw_items)

    # 最后解析 JSON 数组
    items = json.loads(raw_items)    
    logger.info("[Final Parsed JSON] {}", items)

    # 用你原来的模型校验
    pl = PurchaseList(purchase_items=items)

    return {
        "count": len(pl.purchase_items),
        "items": pl.purchase_items,
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
        logger.info("[get_sales_list] from Form sales_list=%s", raw_items)
    else:
        # 2. 兜底：老的 raw body 解析（目前宜搭就是这么传的）
        raw_body = (await request.body()).decode("utf-8")
        logger.info("[Raw Body UTF-8] %s", raw_body)

        form = parse_qs(raw_body)
        logger.info("[Parsed Form] %s", form)

        raw_items = form.get("sales_list", ["[]"])[0]
        logger.info("[get_sales_list] from Body sales_list=%s", raw_items)

    # 一般不需要，但留着不犯错
    raw_items = unquote(raw_items)

    try:
        items = json.loads(raw_items)
    except Exception as e:
        logger.error("[get_sales_list] json.loads failed: %s, raw_items=%s", e, raw_items)
        return {"ok": False, "msg": "invalid sales_list json"}

    logger.info("[Final Parsed JSON] %s", items)

    try:
        sl = SalesList(sales_items=items)
    except Exception as e:
        logger.error("[get_sales_list] SalesList validation failed: %s", e)
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

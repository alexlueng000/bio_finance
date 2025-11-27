# app.py
import os
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
from dotenv import load_dotenv

from yida_client import get_dingtalk_access_token



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


# 方便直接 python app.py 跑，不一定非要用命令行
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )

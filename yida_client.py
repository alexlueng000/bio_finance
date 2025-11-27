# yida_client.py
import os
import json
import time
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# token 缓存文件
TOKEN_DIR = "./.cache"
TOKEN_FILE = os.path.join(TOKEN_DIR, "dingtalk_token.json")

# 读取配置
APP_KEY = os.getenv("DINGTALK_APP_KEY")
APP_SECRET = os.getenv("DINGTALK_APP_SECRET")

# 这里你必须先确保 .env 里配置好：
# DINGTALK_APP_KEY=xxx
# DINGTALK_APP_SECRET=xxx

def get_dingtalk_access_token() -> str:
    """获取宜搭/DingTalk accessToken，带本地缓存，通用版本"""

    if not APP_KEY or not APP_SECRET:
        raise RuntimeError("❌ 环境变量缺失：请设置 DINGTALK_APP_KEY / DINGTALK_APP_SECRET")

    # 确保缓存目录存在
    os.makedirs(TOKEN_DIR, exist_ok=True)

    # 1. 尝试读缓存文件
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            if time.time() < data.get("expires_at", 0):
                logger.info("🔁 使用缓存的 accessToken")
                return data["access_token"]
        except Exception:
            logger.warning("⚠️ token 缓存损坏，将重新获取")

    # 2. 重新从钉钉获取 token
    url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "appKey": APP_KEY,
        "appSecret": APP_SECRET
    }

    try:
        logger.info("🌐 正在请求新的 accessToken...")
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        access_token = data.get("accessToken")
        expire_in = data.get("expireIn", 7200)

        if not access_token:
            logger.error(f"❌ Token 获取失败：{data}")
            raise RuntimeError(f"Token 获取失败：{data}")

        # 提前 60 秒过期，避免边界问题
        cache = {
            "access_token": access_token,
            "expires_at": time.time() + expire_in - 60
        }

        with open(TOKEN_FILE, "w") as f:
            json.dump(cache, f)

        logger.info("✅ 成功获取新的 accessToken")
        return access_token

    except requests.RequestException as e:
        logger.exception("❌ 获取钉钉 accessToken 失败")
        raise RuntimeError("无法从钉钉获取 accessToken") from e

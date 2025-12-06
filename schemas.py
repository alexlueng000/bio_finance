# app/schemas.py
from datetime import datetime

from pydantic import BaseModel, EmailStr, ConfigDict, field_validator
from typing import Optional, List, Union
from decimal import Decimal

# 进项票-产品明细
class PurchaseItem(BaseModel):
    # “产品名称*”
    textField_mi8pp1we: str
    # “产品编号*”
    textField_mi8pp1wf: str
    # “发票-产品数量*”
    numberField_mi8pp1wg: Decimal
    # “采购单价”
    numberField_mi8pp1wh: Optional[Decimal] = None
    # “产品规格*”
    textField_mi8pp1wi: str
    # “产品分类*”
    textField_mi8pp1wj: str
    # “单位*”
    textField_mi8pp1wk: str

    # “采购订单号” —— 可以为空
    textField_miu32cdn: Optional[str] = None
    # “采购发票号” —— 可以为空
    textField_miu32cdl: Optional[str] = None
    # “采购开票日期” —— 宜搭没填就不给，你就不能强制 int
    dateField_miu32cdo: Optional[int] = None

    class Config:
        extra = "allow"

    @field_validator("numberField_mi8pp1wh", mode="before")
    def empty_price_to_none(cls, v):
        if v in ("", None):
            return None
        return v

    @field_validator("dateField_miu32cdo", mode="before")
    def empty_date_to_none(cls, v):
        # 宜搭如果传 "" 或 null，统一变成 None
        if v in ("", None):
            return None
        return v


class PurchaseList(BaseModel):
    purchase_items: List[PurchaseItem]



# 销项票-产品明细
class SalesItem(BaseModel):
    # 品名
    textField_ll5xce5e: str
    # 批次号
    textField_m7ecqboh: str    
    # 规格
    textField_ll5xzsm0: str
    # 单价（元/瓶）
    numberField_m7ecqbof: Decimal
    # 数量 （瓶）
    numberField_m7ecqbog: Decimal
    # 总价
    numberField_m7ecqboe: Decimal
    # 产品编号
    textField_mhd4ta0f: str
    # 销售订单号
    textField_mhd23655: str
    # 销项票-开票日期（宜搭给的是毫秒时间戳）
    dateField_mhd23657: int   # ← 直接按 int 收，不要写 datetime
    # 客户名称
    textField_mhd23658: str
    # 发票类型
    textField_mhd23659: str
    # 销项票发票号
    textField_mhd2365a: str
    # 成本单价（不含税）
    numberField_mims71hm: Optional[Decimal] = None
    # 成本总价（不含税）
    numberField_mims71hn: Optional[Decimal] = None

    @field_validator("numberField_mims71hm", "numberField_mims71hn", mode="before")
    @classmethod
    def empty_str_to_none(cls, v):
        # "" 或 None 统一成 None，其他交给 Decimal 正常解析
        if v == "" or v is None:
            return None
        return v


class SalesList(BaseModel):
    sales_items: List[SalesItem]
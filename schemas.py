# app/schemas.py
from datetime import datetime

from pydantic import BaseModel, EmailStr, ConfigDict 
from typing import Optional, List, Union
from decimal import Decimal

class PurchaseItem(BaseModel):
    # “选择产品” —— 这里通常是一个引用产品表的 ID
    # product_id: str
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

    class Config:
        extra = "allow"  # 允许宜搭传入的额外字段不报错


class PurchaseList(BaseModel):
    purchase_items: List[PurchaseItem]
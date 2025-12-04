from decimal import Decimal

from output_invoice import (
    get_inventory_for_product,
    update_inventory_row,
)

def test_get_inventory(product_code: str):
    print("=== 测试 get_inventory_for_product ===")
    resp = get_inventory_for_product(product_code)

    # 先打印原始返回，确认函数到底返回了什么
    print("原始返回：", resp)

    if not resp:
        print("⚠️ get_inventory_for_product 返回了 None 或空对象，先把函数改成 return 响应结果。")
        return None

    # 你的结构是 {'data': [...], 'currentPage': 1, 'totalCount': 1}
    rows = resp.get("data", [])
    print("库存条数 =", len(rows))

    for idx, row in enumerate(rows, 1):
        print(f"\n--- 库存记录 #{idx} ---")
        print("formInstanceId:", row.get("formInstanceId"))
        print("formData:", row.get("formData"))

    return rows


def test_update_first_inventory(product_code: str):
    """
    1. 查询指定产品的库存记录
    2. 取第一条，按 “消耗 10 个” 的逻辑调用 update_inventory_row
    """
    rows = test_get_inventory(product_code)
    if not rows:
        print("⚠️ 没有可用库存记录，结束。")
        return

    first = rows[0]
    inv_id = first["formInstanceId"]
    fd = first.get("formData", {})

    # 按你贴出来的字段名解析
    orig_qty = Decimal(str(fd.get("numberField_mhlqrhys", 0)))  # 原始数量
    used_qty = Decimal(str(fd.get("numberField_mhlqrhyt", 0)))  # 已结转数量
    remain_qty = Decimal(str(fd.get("numberField_mhlqrhyu", 0)))  # 剩余可用数量

    print(f"\n--- 准备更新库存记录 {inv_id} ---")
    print("原始数量 orig_qty =", orig_qty)
    print("已结转数量 used_qty =", used_qty)
    print("剩余数量 remain_qty =", remain_qty)

    # 模拟本次再结转 10 个
    consume = Decimal("10")
    new_used = used_qty + consume
    new_remain = orig_qty - new_used

    # 简单算一下状态
    if new_used == 0:
        status = "未使用"
    elif new_remain <= 0:
        status = "已用完"
        new_remain = Decimal("0")
    else:
        status = "部分使用"

    print("\n>>> 调用 update_inventory_row：")
    print("inv_id     =", inv_id)
    print("new_used   =", new_used)
    print("new_remain =", new_remain)
    print("status     =", status)

    update_inventory_row(
        inv_id=inv_id,
        used_qty=new_used,
        remain_qty=new_remain,
        status=status,
    )

    print("\n✅ 调用完成，请到宜搭界面检查这条【进项票库存】记录是否被更新。")


if __name__ == "__main__":
    test_update_first_inventory("CPXX202511040132")

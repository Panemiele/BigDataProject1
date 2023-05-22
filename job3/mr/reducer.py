#!/usr/bin/env python3
"""reducer.py"""

import sys
import itertools

user_Products = {}
all_Users = {}

for line in sys.stdin:
    user_id, prod_id = line.split("\t")
    try:
        if user_id not in user_Products:
            user_Products[user_id] = set()
        if user_id in user_Products:
            user_Products[user_id].add(prod_id)
    except ValueError:
        pass

for i in user_Products:
    if len(user_Products[i]) >= 3:
        all_Users[i] = user_Products[i]

groups = []
for user in all_Users:
    group_added = False
    for group in groups:
        common_products = user_Products[user].intersection(group["common_products"])
        if len(common_products) >= 3:
            group["user_id"].add(user)
            group["common_products"].update(common_products)
            group_added = True
            break
    if not group_added:
        group = {"user_id": {user}, "common_products": set(user_Products[user])}
        groups.append(group)

for i, group in enumerate(groups, start=1):
    if len(group["user_id"])>1:
        print(f"Group {i}:", "Users_Id:", group["user_id"], "Products_Id:", set(group["common_products"]))

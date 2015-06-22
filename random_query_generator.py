__author__ = 'Nispand'

import random

def get_lang():
    Lang = ["English","Spanish"]
    id = random.randint(0,1)
    print type(Lang[id])
    return Lang[id]

def get_segment_id():
    seg_id = random.randint(1,103214)
    #print seg_id
    return seg_id

def get_contract_id():
    contract_id = random.randint(28,3361)
    if contract_id > 99 and contract_id < 1000:
        contract_id = "0"+str(contract_id)

    if contract_id >27 and contract_id <100:
        contract_id = "00" + str(contract_id)

    #print contract_id
    return  contract_id

def get_plan_id():
    return random.randint(1,220)

def get_contract_year():
    return 2013

def get_tier_level():
    return random.randint(1,978)

def get_tier_type_desc():
    str_values = ["Mail order gap ","Mail order ","In Network gap ","In Network "]
    days = ["30 days","60 days","90 days"]
    tier = str(str_values[random.randint(0,3)]) + str(days[random.randint(0,2)])
    return tier

def get_sentence_sort_order():
    order = random.randint(1,2564)
    return order

def get_category_code():
    code = random.randint(1,33)
    return code
"""
print get_lang()
print get_segment_id()
print get_contract_id()
print get_plan_id()
print get_contract_year()
print get_tier_level()
print get_tier_type_desc()
print get_sentence_sort_order()
print get_category_code()
"""


fields = ["Lang","segment_id","contract_id","plan_id","contract_year","tier_level","tier_type_desc","sentences_sort_order","category_code"]
query = "Select "
no_f = random.randint(1,9)
print no_f
if no_f == 9 :
    query = query + "*"
elif no_f == 1:
    f_id = random.randint(1,9)
    query = query + fields[f_id]
else :
    f_id = random.sample(range(0,no_f),no_f)
    for i in range(0,no_f-1):
        query = query + fields[f_id[i]] + ","
    query = query + fields[f_id[i+1]]

query = query + " from mytable"
print query

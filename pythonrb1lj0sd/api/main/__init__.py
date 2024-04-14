# coding:utf-8
# author:ila
import os
from flask import Blueprint
from utils.py_file_check import pyFileCheck

main_bp = Blueprint('main', __name__, static_folder='static')

import_str = 'from . import  '
view_tuple = set()
exclude_list = [
    # "common.py",
    "__init__.py"
]
dir = os.path.join(os.getcwd(), "api/main")
dir = dir.replace("unit_test/", '') if "unit_test/" in dir else dir
for i in os.listdir(dir):
    if i not in exclude_list and pyFileCheck(i) == True:
        current = i.split(".", 1)[0]
        view_tuple.add(current)
import_str += ','.join(view_tuple)
print(import_str)
exec(import_str)

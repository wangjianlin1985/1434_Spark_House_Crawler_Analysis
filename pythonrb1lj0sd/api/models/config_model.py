# coding:utf-8
__author__ = "ila"

import time
from api.models.models import Base_model
from api.exts import db


class config(Base_model):
    __doc__ = '''param表'''
    __tablename__ = 'config'

    id = db.Column(db.BigInteger, primary_key=True,autoincrement=False)
    name = db.Column(db.VARCHAR(50), nullable=False, comment='参数名')
    value = db.Column(db.VARCHAR(255), nullable=False, comment='参数值')
    url = db.Column(db.VARCHAR(255), nullable=False, comment='url')

# coding:utf-8
__author__ = 'ila'

from datetime import datetime
from api.models.models import Base_model
from api.exts import db


# 个人信息
class users(Base_model):
    __doc__ = 'admin表'
    __tablename__ = 'users'

    id = db.Column(db.BigInteger, primary_key=True)
    username = db.Column(db.String(100), nullable=False, info='用户名')
    password = db.Column(db.String(100), nullable=False, info='密码')
    role = db.Column(db.String(100), server_default=db.FetchedValue(), info='角色')
    addtime = db.Column(db.DateTime, nullable=False, default=datetime.now,server_default=db.FetchedValue(), info='新增时间')
    image = db.Column(db.VARCHAR(255), nullable=False, info='头像')

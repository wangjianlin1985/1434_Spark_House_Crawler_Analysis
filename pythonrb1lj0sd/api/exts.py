# coding:utf-8
from flask_sqlalchemy import SQLAlchemy
from configs import configs

db = SQLAlchemy(
    session_options={
        # "autoflush": False,
        "autocommit": False,
        # "expire_on_commit": False
    }
)
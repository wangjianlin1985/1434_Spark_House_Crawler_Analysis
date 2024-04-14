# coding:utf-8
import random
from datetime import datetime
from sqlalchemy import text,TIMESTAMP

from api.models.models import Base_model
from api.exts import db
from sqlalchemy.dialects.mysql import DOUBLE,LONGTEXT
# 个人信息
class fangyuanxinxi(Base_model):
    __doc__ = u'''fangyuanxinxi'''
    __tablename__ = 'fangyuanxinxi'



    __authTables__={}
    __authPeople__='否'
    __authSeparate__='否'
    __thumbsUp__='否'
    __intelRecom__='否'
    __browseClick__='否'
    __foreEndListAuth__='否'
    __foreEndList__='否'
    __isAdmin__='否'
    id = db.Column(db.BigInteger, primary_key=True,autoincrement=False,comment='主键')
    addtime = db.Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), server_onupdate=text('CURRENT_TIMESTAMP'))
    title=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='标题' )
    picture=db.Column( db.Text,  nullable=True, unique=False,comment='图片' )
    tags=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='标签' )
    totalprice=db.Column( db.Float,default=0,  nullable=True, unique=False,comment='总价(万)' )
    unitprice=db.Column( db.Float,default=0,  nullable=True, unique=False,comment='单价(元/平)' )
    huxing=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='户型' )
    louceng=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='楼层' )
    chaoxiang=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='朝向' )
    zhuangxiu=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='装修' )
    mianji=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='面积' )
    xiaoqu=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='小区' )
    quyu=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='区域' )
    detailurl=db.Column( db.Text,  nullable=True, unique=False,comment='详情地址' )

class fangjiayuce(Base_model):
    __doc__ = u'''fangjiayuce'''
    __tablename__ = 'fangjiayuce'



    __authTables__={}
    __authPeople__='否'
    __authSeparate__='否'
    __thumbsUp__='否'
    __intelRecom__='否'
    __browseClick__='否'
    __foreEndListAuth__='否'
    __foreEndList__='否'
    __isAdmin__='否'
    id = db.Column(db.BigInteger, primary_key=True,autoincrement=False,comment='主键')
    addtime = db.Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), server_onupdate=text('CURRENT_TIMESTAMP'))
    fangjiayuce=db.Column( db.Float,default=0,  nullable=True, unique=False,comment='房价预测/万元' )
    yucenianfen=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='预测年份' )

class systemintro(Base_model):
    __doc__ = u'''systemintro'''
    __tablename__ = 'systemintro'



    __authTables__={}
    id = db.Column(db.BigInteger, primary_key=True,autoincrement=False,comment='主键')
    addtime = db.Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), server_onupdate=text('CURRENT_TIMESTAMP'))
    title=db.Column( db.VARCHAR(255), nullable=False, unique=False,comment='标题' )
    subtitle=db.Column( db.VARCHAR(255),  nullable=True, unique=False,comment='副标题' )
    content=db.Column( db.Text, nullable=False, unique=False,comment='内容' )
    picture1=db.Column( db.Text,  nullable=True, unique=False,comment='图片1' )
    picture2=db.Column( db.Text,  nullable=True, unique=False,comment='图片2' )
    picture3=db.Column( db.Text,  nullable=True, unique=False,comment='图片3' )


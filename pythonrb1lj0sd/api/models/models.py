# coding:utf-8
__author__ = "ila"

import logging, hashlib, copy,time,re
from flask import session
from sqlalchemy import String,Integer,Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import desc,text
# from sqlalchemy.orm import load_only
from sqlalchemy.sql import func,and_,or_
from sqlalchemy import between
from ..exts import db
def dict_to_hive_insert(table_name, data):
    columns = ','.join(data.keys())
    values = ','.join([f"'{value}'" if isinstance(value, str) else str(value) for value in data.values()])
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    return insert_statement

def Commit():
    '''
    commit提交方法
    :return:
    '''
    error=None
    try:
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        error=str(e)
    return error

class Base_model(db.Model):
    __abstract__ = True

    def __Create(self, data):
        '''
        插入数据公共方法
        :param user:User
        :return:
        '''
        #判断当前表名前两个字符是否为"wd",并且当前表有userid字段,20200301
        if 'userid' in self.__table__.columns and self.__tablename__[:2]=='wd':
            data['userid'] = session.get('params').get('id')
        db.session.add(data)
        return Commit()

    def create(self, data):
        '''
        插入数据
        :param user:User
        :return:
        '''
        return self.__Create(data, data)

    def __Retrieve(self, model):
        '''
        检索全部数据公共方法
        :return:
        '''
        datas=model.query.all()
        try:
            data = [i.to_dict() for i in datas]
        except:
            data = datas
        return data

    def retrieve(self, model):
        '''
        检索全部数据
        :return:
        '''
        return self.__Retrieve(model, model)

    def __Update(self, data):
        '''
        更新某条记录公共方法
        :param user:User
        :return:
        '''
        db.session.add(data)
        return Commit()

    def update(self, data):
        '''
        更新某条记录
        :param user:User
        :return:
        '''
        return self.__Update(data, data)

    def __Delete(self, ids):
        '''
        删除记录：先查询，再删除查询结果公共方法
        :param user:
        :return:
        '''
        self.query.filter(self.id.in_(ids)).delete(synchronize_session=False)
        return Commit()

    def delete(self, ids):
        '''
        删除记录：先查询，再删除查询结果
        :param user:
        :return:
        '''
        return self.__Delete(self, ids)

    def __DeleteByParams(self,model, params):
        '''
        删除记录：先查询，再删除查询结果公共方法
        :param user:
        :return:
        '''
        self.query.filter_by(**params).delete(synchronize_session=False)
        return Commit()

    def deletebyparams(self, model,params):
        '''
        删除记录：先查询，再删除查询结果
        :param user:
        :return:
        '''
        return self.__DeleteByParams(self,model, params)
        
    def __CreateByReq(self, model, req):
        '''
        根据请求参数创建对应模型记录的公共方法
        :param model:
        :param req:
        :return:
        '''
        try:
            hive_sql=dict_to_hive_insert(model.__tablename__,req)
            from utils.hive_func import hive_func
            hive_func(model.__tablename__,[hive_sql])
        except:
            pass
        if  model.__tablename__!= 'users':
            req['id']=int(float(time.time())*1000)


        column_list = []
        for col in self.__table__.columns:
            if str(col.type).lower() == "integer":
                column_list.append(col.name)
        for k, v in req.items():
            if k in column_list and v == '':
                req[k] = 0

        column_list = []
        for col in self.__table__.columns:
            if str(col.type).lower() == "float":
                column_list.append(col.name)
        for k, v in req.items():
            if k in column_list and v == '':
                req[k] = 0.0

        column_list = []
        for col in self.__table__.columns:
            if 'varchar' in str(col.type).lower():
                column_list.append(col.name)
        for k, v in req.items():
            if k in column_list and v == '':
                req[k] = ""

        for col in self.__table__.columns:
            if str(col.type).lower() == "date" and (req[col.name]=="" or req[col.name] is None):
                del req[col.name]

        column_list = []
        for col in self.__table__.columns:
            if str(col.type).lower() == "datetime":
                column_list.append(col.name)
        for k, v in req.items():
            if k in column_list and v == '':
                req[k] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        userid = False
        for col in self.__table__.columns:
           if str( col.name)=='userid':
                if col.nullable==False:
                    userid=True

        if userid==True :
            if  req.get("userid") =="" or  req.get("userid") == None:
                req['userid']=session.get("params").get('id')
                
        for col in self.__table__.columns:
            if str(col.name) not  in req.keys()  :
                if col.nullable==False:
                    if "VARCHAR" in str(col.type) or "CHAR" in str(col.type):
                        req[str(col.name)]=""
        m = model()
        columns = self.getallcolumn(model, model)
        for k, v in req.items():
            if k in columns:
                setattr(m, k, v)
        db.session.add(m)
        return Commit()

    def createbyreq(self, model, req):
        '''
        根据请求参数创建对应模型记录
        :param model:
        :param req:
        :return:
        '''
        return self.__CreateByReq(model, model, req)

    def __GetById(self, model, id):
        '''
        根据id获取数据公共方法
        :param id:
        :return:
        '''
        data= model.query.filter_by(id=id).all()
        try:
            datas=[i if i.items else i.to_dict() for i in data]
        except:
            try:
                datas = [i.to_dict() for i in data]
            except:
                datas = data
        return datas
    def getbyid(self, model, id):
        '''
        根据id获取数据
        :param model:
        :param id:
        :return:
        '''
        return self.__GetById(self,model, id)

    # def __GetByIds(self, model, ids):
    #     return self.query.filter(self.id.in_(ids)).all()
    def __GetByParams(self, model, params):
        # 根据接口传参来创建查询的公共方法
        try:
            __loginUser__=model.__loginUser__
        except:
            __loginUser__=None

        if __loginUser__!=None:
            if params.get('username'):
                params[model.__loginUser__]=copy.deepcopy(params.get('username'))
                del params['username']

        if model.__tablename__ != 'users':
            if params.get('password'):
                params['mima'] = copy.deepcopy(params.get('password'))
                del params['password']

        members = [attr for attr in dir(model) if not attr.startswith("__")]
        paramscopy = copy.deepcopy(params)
        for key in paramscopy.keys():
            if key not in members:
                del params[key]

        m = model.query.filter_by(**params).all()
        #one,two,three
        try:
            mm=[self.model_to_dict(item) for item in m]
        except:
            try:
                mm = [self.model_to_dict(item) for item in m]
            except:
                mm = m
        return mm

    def getbyparams(self, model, params):
        # 根据接口传参来创建查询
        return self.__GetByParams(model, model, params)

    def __UpdateByParams(self, model, params):
        '''
        根据接口传参更新对应id记录的公共方法
        :param model:
        :param params:
        :return:
        '''
        id_ = copy.deepcopy(params['id'])
        del params['id']


        column_list = []
        for col in self.__table__.columns:
            column_list.append(col.name)
        newParams={}
        for k,v in params.items():
            if k in column_list:
                ret = re.findall("\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2}", str(v))
                if len(ret) > 0:
                    newParams[k] = ret[0]
                else:
                    if params[k] is not None and not (v =="" and not isinstance(self.__table__.columns[k].type,String)):
                        newParams[k] = v
                
        model.query.filter_by(id=int(id_)).update(
            newParams
        )
        return Commit()

    def updatebyparams(self, model, params):
        '''
        根据接口传参更新对应id记录
        :param params:
        :return:
        '''
        return self.__UpdateByParams(model, model, params)

    def __Page(self, model, params, or_clauses):
        '''
        刷表专用
        http://ip:port/Pythonrb1lj0sd/${tableName}/page
        page 当前页
        pagesize 每页记录的长度
        sort 排序字段,写死在这,如果刷表出错，立马崩溃
        order 升序（默认asc）或者降序（desc）
        :param req_dict:
        :return:
        '''
        sort = copy.deepcopy(params.get('sort'))
        order = copy.deepcopy(params.get('order'))
        page = copy.deepcopy(params.get('page')) if params.get('page') != None else 1
        limit = copy.deepcopy(params.get('limit')) if params.get('limit') != None else 666
        try:
            del params['sort']
        except:
            pass
        try:
            del params['order']
        except:
            pass
        try:
            del params['page']
        except:
            pass
        try:
            del params['limit']
        except:
            pass
            
        try:
            __sort__=model.__sort__
        except:
            __sort__=None

          #手工实现模糊搜索orz
        keys = []
        fuzzy_key,fuzzy_val,contain_str=None,None,''
        for k,v in params.items():
            if self.is_iterable(v) and "%" in v:
                fuzzy_key=copy.deepcopy(k)
                fuzzy_val=copy.deepcopy(v)
                fuzzy_val=fuzzy_val.replace("%","")
                if fuzzy_key != None:
                    contain_str +='.filter(model.{}.like("%{}%"))'.format(fuzzy_key,fuzzy_val)
                    keys.append(fuzzy_key)
            if k.endswith('start'):
                fuzzy_key=copy.deepcopy(k)
                keys.append(fuzzy_key)
                fuzzy_key=fuzzy_key.replace('start', '')
                fuzzy_val=copy.deepcopy(v)
                if fuzzy_key != None:
                    contain_str +='.filter(model.{}.__ge__({}))'.format(fuzzy_key,fuzzy_val)
                fuzzy_key = None
            if k.endswith('end'):
                fuzzy_key=copy.deepcopy(k)
                keys.append(fuzzy_key)
                fuzzy_key=fuzzy_key.replace('end', '')
                fuzzy_val=copy.deepcopy(v)
                if fuzzy_key != None:
                    contain_str +='.filter(model.{}.__le__({}))'.format(fuzzy_key,fuzzy_val)
                fuzzy_key = None

        for key in keys:
            del params[key]
        # if fuzzy_key!=None:
            # del params[fuzzy_key]

         #__authSeparate__此属性为真，params添加userid，只查询个人数据
        try:
            __authSeparate__ = model.__sort__
        except:
            __authSeparate__ = None

        if __authSeparate__ and __authSeparate__!="否":
            if "userid"  in self.getallcolumn(model,model) and session.get("params")!=None:
                params["userid"]=session.get("params").get("id")

        if  "userid" not  in self.getallcolumn(model,model) and "userid" in params.keys():
            del params["userid"]

        if sort!=None or __sort__!=None:
            if sort==None:
                sort=__sort__
            sort_list = sort.split(",")
            if order==None:
                order=""
            order_list = order.split(",")
            order_str=""
            for index,sort_item in enumerate(sort_list):
                if(order_list[index] == 'desc'):
                    order_str+="desc(text(\""+sort_item+"\"))"
                else:
                    order_str+="text(\""+sort_item+"\")"
                if index < len(sort_list)-1:
                    order_str +=","
            eval_str = '''model.query.filter_by(
                **params
            ){}.filter(or_clauses).order_by('''.format(contain_str)+order_str+''').paginate(
                page=int(page),
                per_page=int(limit),
                error_out=False
            )'''
            datas=eval(eval_str)
        else:
            datas=eval('''model.query.filter_by(
                **params
            ){}.filter(or_clauses).paginate(
                page=int(page),
                per_page=int(limit),
                error_out=False
            )'''.format(contain_str))

        try:
            data = [i if i.items else i.to_dict() for i in datas.items]
        except:
            try:
                data = [i.to_dict() for i in datas.items]
            except:
                data = datas.items
        #转真实类型
        for item in data:
            for k,v in item.items():
                if isinstance(self.__table__.columns[k].type, Integer) and v is not None and v!="":
                    item[k]=int(v)
                elif isinstance(self.__table__.columns[k].type, Float) and v is not None and v!="":
                    item[k]=float(v)
        newData=[]
        if fuzzy_key is  None:
            newData=data
        else:
            for i in data:
               for k,v in i.items():
                   if k==fuzzy_key and fuzzy_val in v:
                       newData.append(i)
        if session.get("tablename") == 'users':
            return newData, datas.page, datas.pages, datas.total, datas.per_page
        newDataa = []
        if hasattr( self,"__authTables__") and self.__authTables__!={}:
            par_keys=session.get("params",{}).keys()
            authtables_keys=self.__authTables__.keys()
            list1=list(set(par_keys).intersection(set(authtables_keys)))
            if len(list1)>0:
                for i in newData:
                   if  i.get(list1[0])==session.get("params").get(list1[0]):
                       newDataa.append(i)
            else:
                newDataa=newData
        else:
            newDataa=newData
        newDataa=newData
        return newDataa,datas.page,datas.pages,datas.total,datas.per_page

    def page(self, model, params, or_clauses=or_(*[])):
        return self.__Page(self, model, params, or_clauses)

    def __GetByColumn(self, model, column, params):
        # data1= model.query.options(load_only(column)).all()
        data1=model.query.filter_by(**params).with_entities(column).all()

        data_list = []
        for i in data1:
            data_list.append(i[0])
        return data_list


    def getbyColumn(self, model, column, params):
        '''
        获取某表的某个字段的内容列表，去重
        :param model:
        :param column:
        :return:
        '''
        return self.__GetByColumn(self, model, column, params)

    def __GetBetweenParams(self, model, columnName, params):
        '''

        :param model:
        :param params:
        :return:
        '''
        remindStart = copy.deepcopy(params.get("remindStart"))
        remindEnd = copy.deepcopy(params.get("remindEnd"))
        del params["remindStart"]
        del params["remindEnd"]

        contain_str = '.filter(model.{}.between(remindStart, remindEnd))'.format(columnName)

        #todo where是否合法
        datas= eval('''model.query.filter_by(**params){}'''.format(contain_str)) 
        
        try:
            data = [i if i.items else i.to_dict() for i in datas]
        except:
            try:
                data = [i.to_dict() for i in datas]
            except:
                data = datas

        return data

    def getbetweenparams(self, model, columnName, params):
        '''
        区域内查询
        :param model:
        :param params:
        :return:
        '''

        return self.__GetBetweenParams( self,model, columnName, params)

    def __GetComputedByColumn(self, model, columnName):
        return db.session.query(
            func.sum(getattr(model, columnName)).label("sum"),
            func.max(getattr(model, columnName)).label("max"),
            func.min(getattr(model, columnName)).label("min"),
            func.avg(getattr(model, columnName)).label("avg"),
        ).all()

    def getcomputedbycolumn(self, model, columnName):
        '''
        求和最大最小平均值
        :param model:
        :param columnName:
        :return:
        '''
        return self.__GetComputedByColumn(self, model, columnName)

    def __GroupByColumnName(self, model, columnName, params):
        datas = db.session.query(getattr(model, columnName),func.count(getattr(model, columnName))).group_by(getattr(model, columnName)).filter_by(**params).all()
        try:
            data = [i.to_dict() for i in datas]
        except:
            data = datas
        data=[{columnName:x[0],"total":float(x[1]) if x[1] is not None else 0} for x in data]
        return data

    def groupbycolumnname(self, model, columnName, params={}):
        '''
        类别统计
        :param model:
        :param params:
        :return:
        '''
        return self.__GroupByColumnName(self, model, columnName, params)
        
    def __GetValueByxyColumnName(self,model,xColumnName,yColumnName,params):
        '''
        内部函数
        :param model:
        :param xColumnName:
        :param yColumnName:
        :return:
        '''
        datas=db.session.query(getattr(model,xColumnName),func.sum(getattr(model,yColumnName))).group_by(getattr(model,xColumnName)).filter_by(**params).all()
        try:
            data = [i.to_dict() for i in datas]
        except:
            data = datas
        data = [{xColumnName: x[0], "total": float(x[1]) if x[1] is not None else 0} for x in data]
        return data

    def getvaluebyxycolumnname(self,model,xColumnName,yColumnName,params={}):
        '''

        :param model:
        :param xColumnName:
        :param yColumnName:
        :return:
        '''
        return self.__GetValueByxyColumnName(self,model,xColumnName,yColumnName,params)
    
    def getallcolumn(self, model):
        """
        把一条记录转成字典
        :returns dict:
        """
        return [c.name for c in model.__table__.columns]

    def to_dict(self):
        """
        把一条记录转成字典
        :returns dict:
        """
        data= {c.name: str(getattr(self, c.name)) for c in self.__table__.columns}
        dataa={}
        for k,v in data.items():
            if v==None or v=="None":
                dataa[k]=""
            else:
                dataa[k] = v


        return dataa

    def set_password(self, password) -> str:
        '''
        设置密码
        :param password:
        :return:
        '''
        m = hashlib.md5()
        m.update(password.encode("utf-8"))
        passwd = m.hexdigest()
        logging.warning("password.md5 : {}".format(passwd))
        return passwd

    def check_password(self, hash, password) -> bool:
        '''
        验证密码
        :param hash:
        :param password:
        :return:
        '''
        m = hashlib.md5()
        m.update(password.encode("utf-8"))
        return True if hash == m.hexdigest() else False

    def count(self, model, params):
        list_count = db.session.query(func.count(getattr(model, 'id'))).filter_by(**params).scalar()
        return list_count

    def model_to_dict(model):
        result = {}
        for column in model.__table__.columns:
            result[column.name] = getattr(model, column.name)
        return result

    # 是否可迭代
    def is_iterable(obj):
        try:
            iter(obj)
            return True  # 如果迭代成功，返回True
        except TypeError:
            return False  # 如果迭代引发TypeError异常，返回False
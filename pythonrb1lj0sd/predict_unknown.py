# coding:utf-8
__author__ = 'ila'

import time

import joblib
import pandas as pd
import pymysql
from sklearn.linear_model import LinearRegression

#scikit-learn
class PredictUnknown(object):
    def __init__(self):
        host = '127.0.0.1'
        port = 3306
        user = 'root'
        passwd = '123456'
        db = 'pythonrb1lj0sd'
        charset = 'utf8mb4'

        self.con = pymysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            database=db,
            charset=charset,
        )
        self.cur1 = self.con.cursor()
        self.cur2 = self.con.cursor()
        self.raw_data = {}
        self.dict_data = {}
        self.result_data = None
        self.df = None
        self.model = None
        self.raw_column_list = []
        self.column_list = []

    def process_data(self, sql):
        self.raw_data = {i: [] for i in self.raw_column_list}
        self.cur1.execute(sql)

        for data_set in self.cur1.fetchall():
            for idx, data in enumerate(data_set):
                self.raw_data[self.raw_column_list[idx]].append(data)

        # 将数据集转为 Pandas DataFrame
        self.result_data = pd.DataFrame({k: v for k, v in self.raw_data.items() if k in self.column_list})

        self.cur1.close()

    def train(self):
        print(self.column_list[:-1])
        # 提取特征和目标变量
        X = self.result_data[self.column_list[:-1]]  # 提取特征
        Y = self.result_data[self.column_list[-1]]  # 提取目标变量

        # 创建并训练线性回归模型
        model = LinearRegression()
        model.fit(X, Y)

        # 保存模型
        joblib.dump(model, "model.pkl")

        # 返回训练好的模型对象
        return model

    def predict(self, model, data):
        # 构建新的数据集
        new_data = pd.DataFrame(data)
        # 进行预测
        prediction = model.predict(new_data)

        return round(prediction[0], 2)

    def insert_data(self, data):
        columns = []
        values = []
        for k, v in data.items():
            columns.append(f"{k}")
            values.append(f"{v}")
        insert_sql = f'''insert into fangjiayuce({','.join(columns)})values('{"','".join(values)}') '''
        print(insert_sql)
        self.cur2.execute(insert_sql)
        self.con.commit()


if __name__ == "__main__":
    pu = PredictUnknown()
    pu.raw_column_list = ['id','unitprice', "totalprice"]
    pu.column_list = ['id','unitprice', "totalprice"]
    select_sql = f'select {",".join(pu.raw_column_list)} from fangyuanxinxi'

    pu.process_data(select_sql)
    model = pu.train()

    year=2024
    for idx, col in enumerate(pu.raw_data.get(pu.column_list[0])):
        datas = {}
        for col in pu.column_list[:-1]:
            datas[col] = [pu.raw_data.get(col)[idx]]

        print(datas)
        score = pu.predict(model, datas)
        print("predicted data: ", score)
        insert_data = {
            "addtime": time.strftime('%Y-%m-%d', time.localtime(time.time())),
            "fangjiayuce": score,
            "yucenianfen": f"{2024+1+idx}"
        }
        print(insert_data)
        pu.insert_data(insert_data)

        if idx>7:
            break

    pu.cur1.close()
    pu.cur2.close()
    pu.con.close()
    print("预测完毕,请刷新预测页面看结果")

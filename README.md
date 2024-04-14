开发软件环境： Spark大数据平台 + PyCharm  + Python3.7 + Scrapy爬虫 + 机器学习算法线性回归预测

数据库：mysql 5.7（一定要5.7版本）

网站框架：flask后端 + vue前端 + 大屏展示

    基于Spark大数据环境开发的一个二手房分析和预测系统，其中二手房的数据来源采用scrapy框架从网络爬取，爬取的数据保存到mysql数据库，然后利用spark环境对房屋信息进行统计分析，分析结果采用一个大屏展示，同时会吧分析结果保存到hdfs分布式文件系统里面，房屋的展示和分析网站后端接口采用flask框架开发，前端界面采用vue开发，实现了前后端分离模式。同时利用现在的房屋信息属性可以预测未来的房价，采用的人工智能机器学习算法的线性回归分析算法。
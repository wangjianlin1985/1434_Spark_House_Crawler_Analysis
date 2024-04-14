# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class FangyuanxinxiItem(scrapy.Item):
    # 标题
    title = scrapy.Field()
    # 图片
    picture = scrapy.Field()
    # 标签
    tags = scrapy.Field()
    # 总价(万)
    totalprice = scrapy.Field()
    # 单价(元/平)
    unitprice = scrapy.Field()
    # 户型
    huxing = scrapy.Field()
    # 楼层
    louceng = scrapy.Field()
    # 朝向
    chaoxiang = scrapy.Field()
    # 装修
    zhuangxiu = scrapy.Field()
    # 面积
    mianji = scrapy.Field()
    # 小区
    xiaoqu = scrapy.Field()
    # 区域
    quyu = scrapy.Field()
    # 详情地址
    detailurl = scrapy.Field()


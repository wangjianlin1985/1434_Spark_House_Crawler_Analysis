<template>
<div class="content" :style='{"padding":"30px","flexWrap":"wrap","display":"flex"}'>
	<!-- notice -->
	<!-- title -->
	<div class="text" :style='{"border":"0","margin":"50px auto","color":"#fff","textAlign":"center","width":"100%","fontSize":"24px","fontWeight":"bold"}'>欢迎使用 {{this.$project.projectName}}</div>
	<!-- statis -->
	<div :style='{"width":"50%","margin":"0","grid-template-columns":"repeat(2,1fr)","display":"grid","rowGap":"20px","order":"2"}'>
		<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"4px","alignItems":"center","background":"#fff","display":"flex","position":"relative","justifyContent":"center"}' v-if="isAuth('fangyuanxinxi','首页总数')">
			<div :style='{"alignItems":"center","top":"0","left":"0","background":"#5CE5FB","display":"flex","width":"80px","position":"absolute","justifyContent":"center","height":"80px"}'>
				<span class="icon iconfont icon-shijian18" :style='{"color":"#fff","fontSize":"34px"}'></span>
			</div>
			<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
				<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{fangyuanxinxiCount}}</div>
				<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>房源信息总数</div>
			</div>
		</div>
		<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"4px","alignItems":"center","background":"#fff","display":"flex","position":"relative","justifyContent":"center"}' v-if="isAuth('fangjiayuce','首页总数')">
			<div :style='{"alignItems":"center","top":"0","left":"0","background":"#5CE5FB","display":"flex","width":"80px","position":"absolute","justifyContent":"center","height":"80px"}'>
				<span class="icon iconfont icon-tongjitu09" :style='{"color":"#fff","fontSize":"34px"}'></span>
			</div>
			<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
				<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{fangjiayuceCount}}</div>
				<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>房价预测总数</div>
			</div>
		</div>
	</div>
	<!-- statis -->
	

	
	<!-- echarts -->
	<!-- 5 -->
	<div class="type5" :style='{"alignContent":"flex-start","padding":"0","margin":"20px 0 0","flexWrap":"wrap","display":"flex","width":"100%","position":"relative","justifyContent":"space-between","height":"auto","order":"3"}'>
		<div id="fangyuanxinxiChart1" class="echarts1" v-if="isAuth('fangyuanxinxi','首页统计')"></div>
		<div id="fangyuanxinxiChart2" class="echarts2" v-if="isAuth('fangyuanxinxi','首页统计')"></div>
		<div id="fangyuanxinxiChart3" class="echarts3" v-if="isAuth('fangyuanxinxi','首页统计')"></div>
		<div id="fangyuanxinxiChart4" class="echarts4" v-if="isAuth('fangyuanxinxi','首页统计')"></div>
		<div id="fangyuanxinxiChart5" class="echarts5" v-if="isAuth('fangyuanxinxi','首页统计')"></div>
	</div>
</div>
</template>
<script>
//5
import router from '@/router/router-static'
import * as echarts from 'echarts'
export default {
	data() {
		return {
            fangyuanxinxiCount: 0,
            fangjiayuceCount: 0,
			line: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#333","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#666","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(25,25,25,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#333","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(25,25,25,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"#333","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":20,"textStyle":{"textBorderWidth":0,"color":"#333","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"showSymbol":true,"symbol":"emptyCircle","symbolSize":4},"tooltip":{"backgroundColor":"#fff","textStyle":{"color":"#333"}},"title":{"borderType":"solid","padding":0,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":true,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#333","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":600,"textBorderColor":"#666","textShadowBlur":0},"shadowColor":"transparent"}},
			bar: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":12,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"color":"#666","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#666","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(25,25,25,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#333","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(25,25,25,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#00ff00","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":2,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"#333","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":2,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"barWidth":"auto","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"colorBy":"data","barCategoryGap":"20%"},"tooltip":{"backgroundColor":"#fff","textStyle":{"color":"#333"}},"title":{"borderType":"solid","padding":0,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":true,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#333","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":600,"textBorderColor":"#666","textShadowBlur":0},"shadowColor":"transparent"},"base":{"animate":false,"interval":2000}},
			pie: {"tooltip":{"backgroundColor":"#fff","textStyle":{"color":"#333"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":0,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":true,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#333","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":600,"textBorderColor":"#666","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":5,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#666","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":2,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":0,"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":2,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#666","color":"inherit","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#666","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#333","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel: {"tooltip":{"backgroundColor":"#fff","textStyle":{"color":"#333"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":true,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#666","textShadowColor":"transparent","fontSize":14,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":5,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":2,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":2,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			boardBase: {"funnelNum":8,"lineNum":8,"gaugeNum":8,"barNum":20,"pieNum":8},
			gauge: {"tooltip":{"backgroundColor":"#fff","textStyle":{"color":"#333"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"top":"top","left":"left","textStyle":{"fontSize":14,"lineHeight":24,"color":"#333","fontWeight":600}},"series":{"pointer":{"offsetCenter":[0,"10%"],"icon":"path://M2.9,0.7L2.9,0.7c1.4,0,2.6,1.2,2.6,2.6v115c0,1.4-1.2,2.6-2.6,2.6l0,0c-1.4,0-2.6-1.2-2.6-2.6V3.3C0.3,1.9,1.4,0.7,2.9,0.7z","width":8,"length":"80%"},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"opacity":0.5,"shadowBlur":1,"shadowColor":"#000"},"roundCap":true},"anchor":{"show":true,"itemStyle":{"color":"inherit"},"size":18,"showAbove":true},"emphasis":{"disabled":false},"progress":{"show":true,"roundCap":true,"overlap":true},"splitNumber":25,"detail":{"formatter":"{value}","backgroundColor":"inherit","color":"#fff","borderRadius":3,"width":20,"fontSize":14,"height":16},"title":{"fontSize":14},"animation":true}},
		};
	},
	mounted(){
		this.init();
		this.getfangyuanxinxiCount();
		this.fangyuanxinxiChat1();
		this.fangyuanxinxiChat2();
		this.fangyuanxinxiChat3();
		this.fangyuanxinxiChat4();
		this.fangyuanxinxiChat5();
		this.getfangjiayuceCount();
	},
	methods:{
		// 词云
		wordclouds(wordcloudData,echartsId) {
			let wordcloud = {"maskImage":"data:image/png;base64,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","option":{"tooltip":{"show":false},"backgroundColor":"transparent","series":[{"sizeRange":[9,32],"layoutAnimation":true,"shape":"circle","data":[{"name":"花鸟市场","value":1446},{"name":"汽车","value":928},{"name":"视频","value":906},{"name":"电视","value":825},{"name":"Lover Boy 88","value":514},{"name":"动漫","value":486},{"name":"音乐","value":53},{"name":"直播","value":163},{"name":"广播电台","value":86},{"name":"戏曲曲艺","value":17},{"name":"演出票务","value":6},{"name":"给陌生的你听","value":1},{"name":"资讯","value":1437},{"name":"商业财经","value":422},{"name":"娱乐八卦","value":353},{"name":"军事","value":331},{"name":"科技资讯","value":313},{"name":"社会时政","value":307},{"name":"时尚","value":43},{"name":"网络奇闻","value":15},{"name":"旅游出行","value":438},{"name":"景点类型","value":957},{"name":"国内游","value":927},{"name":"远途出行方式","value":908},{"name":"酒店","value":693},{"name":"关注景点","value":611},{"name":"旅游网站偏好","value":512},{"name":"出国游","value":382},{"name":"交通票务","value":312},{"name":"旅游方式","value":187},{"name":"旅游主题","value":163},{"name":"港澳台","value":104},{"name":"本地周边游","value":3},{"name":"小卖家","value":1331},{"name":"全日制学校","value":941},{"name":"基础教育科目","value":585},{"name":"考试培训","value":473},{"name":"语言学习","value":358},{"name":"留学","value":246},{"name":"K12课程培训","value":207},{"name":"艺术培训","value":194},{"name":"技能培训","value":104},{"name":"IT培训","value":87},{"name":"高等教育专业","value":63},{"name":"家教","value":48},{"name":"体育培训","value":23},{"name":"职场培训","value":5},{"name":"金融财经","value":1328},{"name":"银行","value":765},{"name":"股票","value":452},{"name":"保险","value":415},{"name":"贷款","value":253},{"name":"基金","value":211},{"name":"信用卡","value":180},{"name":"外汇","value":138},{"name":"P2P","value":116},{"name":"贵金属","value":98},{"name":"债券","value":93},{"name":"网络理财","value":92},{"name":"信托","value":90},{"name":"征信","value":76},{"name":"期货","value":76},{"name":"公积金","value":40},{"name":"银行理财","value":36},{"name":"银行业务","value":30},{"name":"典当","value":7},{"name":"海外置业","value":1},{"name":"汽车","value":1309},{"name":"汽车档次","value":965},{"name":"汽车品牌","value":900},{"name":"汽车车型","value":727},{"name":"购车阶段","value":461},{"name":"二手车","value":309},{"name":"汽车美容","value":260},{"name":"新能源汽车","value":173},{"name":"汽车维修","value":155},{"name":"租车服务","value":136},{"name":"车展","value":121},{"name":"违章查询","value":76},{"name":"汽车改装","value":62},{"name":"汽车用品","value":37},{"name":"路况查询","value":32},{"name":"汽车保险","value":28},{"name":"陪驾代驾","value":4},{"name":"网络购物","value":1275},{"name":"做我的猫","value":1088},{"name":"只想要你知道","value":907},{"name":"团购","value":837},{"name":"比价","value":201},{"name":"海淘","value":195},{"name":"移动APP购物","value":179},{"name":"支付方式","value":119},{"name":"代购","value":43},{"name":"体育健身","value":1234},{"name":"体育赛事项目","value":802},{"name":"运动项目","value":405},{"name":"体育类赛事","value":337},{"name":"健身项目","value":199},{"name":"健身房健身","value":78},{"name":"运动健身","value":77},{"name":"家庭健身","value":36},{"name":"健身器械","value":29},{"name":"办公室健身","value":3},{"name":"商务服务","value":1201},{"name":"法律咨询","value":508},{"name":"化工材料","value":147},{"name":"广告服务","value":125},{"name":"会计审计","value":115},{"name":"人员招聘","value":101},{"name":"印刷打印","value":66},{"name":"知识产权","value":32},{"name":"翻译","value":22},{"name":"安全安保","value":9},{"name":"公关服务","value":8},{"name":"商旅服务","value":2},{"name":"展会服务","value":2},{"name":"特许经营","value":1},{"name":"休闲爱好","value":1169},{"name":"收藏","value":412},{"name":"摄影","value":393},{"name":"温泉","value":230},{"name":"博彩彩票","value":211},{"name":"美术","value":207},{"name":"书法","value":139},{"name":"DIY手工","value":75},{"name":"舞蹈","value":23},{"name":"钓鱼","value":21},{"name":"棋牌桌游","value":17},{"name":"KTV","value":6},{"name":"密室","value":5},{"name":"采摘","value":4},{"name":"电玩","value":1},{"name":"真人CS","value":1},{"name":"轰趴","value":1},{"name":"家电数码","value":1111},{"name":"手机","value":885},{"name":"电脑","value":543},{"name":"大家电","value":321},{"name":"家电关注品牌","value":253},{"name":"网络设备","value":162},{"name":"摄影器材","value":149},{"name":"影音设备","value":133},{"name":"办公数码设备","value":113},{"name":"生活电器","value":67},{"name":"厨房电器","value":54},{"name":"智能设备","value":45},{"name":"个人护理电器","value":22},{"name":"服饰鞋包","value":1047},{"name":"服装","value":566},{"name":"饰品","value":289},{"name":"鞋","value":184},{"name":"箱包","value":168},{"name":"奢侈品","value":137},{"name":"母婴亲子","value":1041},{"name":"孕婴保健","value":505},{"name":"母婴社区","value":299},{"name":"早教","value":103},{"name":"奶粉辅食","value":66},{"name":"童车童床","value":41},{"name":"关注品牌","value":271},{"name":"宝宝玩乐","value":30},{"name":"母婴护理服务","value":25},{"name":"纸尿裤湿巾","value":16},{"name":"妈妈用品","value":15},{"name":"宝宝起名","value":12},{"name":"童装童鞋","value":9},{"name":"胎教","value":8},{"name":"宝宝安全","value":1},{"name":"宝宝洗护用品","value":1},{"name":"软件应用","value":1018},{"name":"系统工具","value":896},{"name":"理财购物","value":440},{"name":"生活实用","value":365},{"name":"影音图像","value":256},{"name":"社交通讯","value":214},{"name":"手机美化","value":39},{"name":"办公学习","value":28},{"name":"应用市场","value":23},{"name":"母婴育儿","value":14},{"name":"游戏","value":946},{"name":"手机游戏","value":565},{"name":"PC游戏","value":353},{"name":"网页游戏","value":254},{"name":"游戏机","value":188},{"name":"模拟辅助","value":166},{"name":"个护美容","value":942},{"name":"护肤品","value":177},{"name":"彩妆","value":133},{"name":"美发","value":80},{"name":"香水","value":50},{"name":"个人护理","value":46},{"name":"美甲","value":26},{"name":"SPA美体","value":21},{"name":"花鸟萌宠","value":914},{"name":"绿植花卉","value":311},{"name":"狗","value":257},{"name":"其他宠物","value":131},{"name":"水族","value":125},{"name":"猫","value":122},{"name":"动物","value":81},{"name":"鸟","value":67},{"name":"宠物用品","value":41},{"name":"宠物服务","value":26},{"name":"书籍阅读","value":913},{"name":"网络小说","value":483},{"name":"关注书籍","value":128},{"name":"文学","value":105},{"name":"报刊杂志","value":77},{"name":"人文社科","value":22},{"name":"建材家居","value":907},{"name":"装修建材","value":644},{"name":"家具","value":273},{"name":"家居风格","value":187},{"name":"家居家装关注品牌","value":140},{"name":"家纺","value":107},{"name":"厨具","value":47},{"name":"灯具","value":43},{"name":"家居饰品","value":29},{"name":"家居日常用品","value":10},{"name":"生活服务","value":883},{"name":"物流配送","value":536},{"name":"家政服务","value":108},{"name":"摄影服务","value":49},{"name":"搬家服务","value":38},{"name":"物业维修","value":37},{"name":"婚庆服务","value":24},{"name":"二手回收","value":24},{"name":"鲜花配送","value":3},{"name":"维修服务","value":3},{"name":"殡葬服务","value":1},{"name":"求职创业","value":874},{"name":"创业","value":363},{"name":"目标职位","value":162},{"name":"目标行业","value":50},{"name":"兼职","value":21},{"name":"期望年薪","value":20},{"name":"实习","value":16},{"name":"雇主类型","value":10},{"name":"星座运势","value":789},{"name":"星座","value":316},{"name":"算命","value":303},{"name":"解梦","value":196},{"name":"风水","value":93},{"name":"面相分析","value":47},{"name":"手相","value":32},{"name":"公益","value":90}],"keepAspect":false,"type":"wordCloud","rotationRange":[-90,90],"gridSize":8,"shrinkToFit":false,"top":"center","left":"center","width":"80%","emphasis":{"focus":"self","textStyle":{"textShadowColor":"#333","textShadowBlur":0}},"drawOutOfBound":false,"rotationStep":45,"textStyle":{"color":"function(){return\"rgb(\"+[Math.round(160*Math.random()),Math.round(160*Math.random()),Math.round(160*Math.random())].join(\",\")+\")\"}","fontWeight":500,"fontFamily":"sans-serif"},"height":"80%","maskImage":{}}]}}
			wordcloud = JSON.parse(JSON.stringify(wordcloud), (k, v) => {
			  if(typeof v == 'string' && v.indexOf('function') > -1){
				return eval("(function(){return "+v+" })()")
			  }
			  return v;
			})
			wordcloud.option.series[0].data=wordcloudData;
			
			this.myChart0 = echarts.init(document.getElementById(echartsId));
			let myChart = this.myChart0
			let img = wordcloud.maskImage
		
			if (img) {
				var maskImage = new Image();
				maskImage.src = img
				maskImage.onload = function() {
					wordcloud.option.series[0].maskImage = maskImage
					myChart.clear()
					myChart.setOption(wordcloud.option)
				}
			} else {
				delete wordcloud.option.series[0].maskImage
				myChart.clear()
				myChart.setOption(wordcloud.option)
			}
		},
		// 统计图动画
		myChartInterval(type, xAxisData, seriesData, myChart) {
			this.$nextTick(() => {
				setInterval(() => {
					let xAxis = xAxisData.shift()
					xAxisData.push(xAxis)
					let series = seriesData.shift()
					seriesData.push(series)
				
					if (type == 1) {
						myChart.setOption({
							xAxis: [{
								data: xAxisData
							}],
							series: [{
								data: seriesData
							}]
						});
					}
					if (type == 2) {
						myChart.setOption({
							yAxis: [{
								data: xAxisData
							}],
							series: [{
								data: seriesData
							}]
						});
					}
				}, 2000);
			})
		},
		init(){
			if(this.$storage.get('Token')){
			this.$http({
				url: `${this.$storage.get('sessionTable')}/session`,
				method: "get"
			}).then(({ data }) => {
				if (data && data.code != 0) {
				router.push({ name: 'login' })
				}
			});
			}else{
				router.push({ name: 'login' })
			}
		},
		getfangyuanxinxiCount() {
			this.$http({
				url: `fangyuanxinxi/count`,
				method: "get"
			}).then(({
				data
			}) => {
				if (data && data.code == 0) {
					this.fangyuanxinxiCount = data.data
				}
			})
		},
		fangyuanxinxiChat1() {
			this.$nextTick(()=>{

        this.$http({
			url: "group/fangyuanxinxi/xiaoqu",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].xiaoqu);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].xiaoqu
                    })
                }
                var option = {};
				this.wordclouds(pArray,'fangyuanxinxiChart1');
				
            }
        });
      })
    },

    fangyuanxinxiChat2() {
      this.$nextTick(()=>{

        var fangyuanxinxiChart2 = echarts.init(document.getElementById("fangyuanxinxiChart2"),'macarons');
        this.$http({
            url: "fangyuanxinxi/group/huxing",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
					if(this.boardBase&&i==this.boardBase.funnelNum){
						break;
					}
                    xAxis.push(res[i].huxing);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].huxing
                    })
                }
                var option = {};
				let titleObj = this.funnel.title
				titleObj.text = '户型'
				
				let legendObj = {
					data: xAxis,
				}
				legendObj = Object.assign(legendObj , this.funnel.legend)
				let tooltipObj = {trigger: 'item',formatter: '{b} : {c}'}
				tooltipObj = Object.assign(tooltipObj , this.funnel.tooltip?this.funnel.tooltip:{})
				let seriesObj = {
					name: '户型',
					data: pArray,
					type: 'funnel',
					left: '10%',
					top: 60,
					bottom: 60,
					width: '80%',
					minSize: '0%',
					maxSize: '100%',
				}
				seriesObj = Object.assign(seriesObj , this.funnel.series)
				const gridObj = this.funnel.grid
				option = {
					backgroundColor: this.funnel.backgroundColor,
					color: this.funnel.color,
				    title: titleObj,
				    legend: legendObj,
				    tooltip: tooltipObj,
				    series: seriesObj,
					grid: gridObj
				}
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart2.setOption(option);
				
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    fangyuanxinxiChart2.resize();
                };
            }
        });
      })
    },

    fangyuanxinxiChat3() {
      this.$nextTick(()=>{

        var fangyuanxinxiChart3 = echarts.init(document.getElementById("fangyuanxinxiChart3"),'macarons');
        this.$http({
            url: `fangyuanxinxi/value/title/totalprice`,
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
					if(this.boardBase&&i==this.boardBase.lineNum){
						break;
					}
                    xAxis.push(res[i].title);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].title
                    })
                }
                var option = {};
                let titleObj = this.line.title
				titleObj.text = '房屋总价'
				
				const legendObj = this.line.legend
				let tooltipObj = { trigger: 'item',formatter: '{b} : {c}'}
				tooltipObj = Object.assign(tooltipObj , this.line.tooltip?this.line.tooltip:{})
				
				let xAxisObj = this.line.xAxis
				xAxisObj.type = 'category'
				xAxisObj.boundaryGap = false
				xAxisObj.data = xAxis
                xAxisObj.axisLabel.rotate=70
				
				let yAxisObj = this.line.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
					data: yAxis,
					type: 'line',
				}
				seriesObj = Object.assign(seriesObj , this.line.series)
				const gridObj = this.line.grid
				option = {
					backgroundColor: this.line.backgroundColor,
					color: this.line.color,
					title: titleObj,
					legend: legendObj,
					tooltip: tooltipObj,
					xAxis: xAxisObj,
					yAxis: yAxisObj,
					series: [seriesObj],
					grid: gridObj
				};
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart3.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    fangyuanxinxiChart3.resize();
                };
            }
        });
      })
    },
    fangyuanxinxiChat4() {
      this.$nextTick(()=>{

        var fangyuanxinxiChart4 = echarts.init(document.getElementById("fangyuanxinxiChart4"),'macarons');
        this.$http({
            url: `fangyuanxinxi/value/title/unitprice`,
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
					if(this.boardBase&&i==this.boardBase.lineNum){
						break;
					}
                    xAxis.push(res[i].title);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].title
                    })
                }
                var option = {};
				let titleObj = this.line.title
				titleObj.text = '房屋单价'
				
				const legendObj = this.line.legend
				let tooltipObj = { trigger: 'item',formatter: '{b} : {c}'}
				tooltipObj = Object.assign(tooltipObj , this.line.tooltip?this.line.tooltip:{})
				
				let xAxisObj = this.line.xAxis
				xAxisObj.type = 'category'
				xAxisObj.boundaryGap = false
				xAxisObj.data = xAxis
                xAxisObj.axisLabel.rotate=70
				
				let yAxisObj = this.line.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
					data: yAxis,
					type: 'line',
					areaStyle: {},
					smooth: true
				}
				seriesObj = Object.assign(seriesObj , this.line.series)
				const gridObj = this.line.grid
				option = {
					backgroundColor: this.line.backgroundColor,
					color: this.line.color,
					title: titleObj,
					legend: legendObj,
					tooltip: tooltipObj,
					xAxis: xAxisObj,
					yAxis: yAxisObj,
					series: [seriesObj],
					grid: gridObj
				};
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart4.setOption(option);

                  //根据窗口的大小变动图表
                window.onresize = function() {
                    fangyuanxinxiChart4.resize();
                };
            }
        });
      })
    },
    fangyuanxinxiChat5() {
      this.$nextTick(()=>{

        var fangyuanxinxiChart5 = echarts.init(document.getElementById("fangyuanxinxiChart5"),'macarons');
        this.$http({
            url: `fangyuanxinxi/value/mianji/totalprice`,
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
					if(this.boardBase&&i==this.boardBase.barNum){
						break;
					}
                    xAxis.push(res[i].mianji);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].mianji
                    })
                }
                var option = {};
				let titleObj = this.bar.title
				titleObj.text = '房价与面积统计'
				
				const legendObj = this.bar.legend
				
				let tooltipObj = {trigger: 'item',formatter: '{b} : {c}'}
				tooltipObj = Object.assign(tooltipObj , this.bar.tooltip?this.bar.tooltip:{})
				let xAxisObj = this.bar.xAxis
				xAxisObj.type = 'category'
				xAxisObj.data = xAxis
                xAxisObj.axisLabel.rotate=40
				
				let yAxisObj = this.bar.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
						data: yAxis,
						type: 'bar'
					}
				seriesObj = Object.assign(seriesObj , this.bar.series)
				const gridObj = this.bar.grid
				option = {
					backgroundColor: this.bar.backgroundColor,
					color: this.bar.color,
					title: titleObj,
					legend: legendObj,
					tooltip: tooltipObj,
					xAxis: xAxisObj,
					yAxis: yAxisObj,
					series: [seriesObj],
					grid: gridObj
				};
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart5.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    fangyuanxinxiChart5.resize();
                };
            }
        });
      })
    },
		getfangjiayuceCount() {
			this.$http({
				url: `fangjiayuce/count`,
				method: "get"
			}).then(({
				data
			}) => {
				if (data && data.code == 0) {
					this.fangjiayuceCount = data.data
				}
			})
		},


  }
};
</script>
<style lang="scss" scoped>
    .cardView {
        display: flex;
        flex-wrap: wrap;
        width: 100%;

        .cards {
            display: flex;
            align-items: center;
            width: 100%;
            margin-bottom: 10px;
            justify-content: center;
            .card {
                width: calc(25% - 20px);
                margin: 0 10px;
                /deep/.el-card__body{
                    padding: 0;
                }
            }
        }
    }
	
	// 日历
	.calendar td .text {
				border-radius: 12px;
				flex-direction: column;
				background: #fff;
				display: flex;
				width: 100%;
				justify-content: center;
				align-items: center;
				height: 100%;
			}
	.calendar td .text:hover {
				background: rgba(78,110,242,.1);
			}
	.calendar td .text .new {
				color: #000;
				font-size: 24px;
				line-height: 1.4;
			}
	.calendar td .text .old {
				color: #666;
				font-size: 16px;
				line-height: 1.4;
			}
	.calendar td.festival .text {
				border-radius: 12px;
				flex-direction: column;
				background: rgba(235,51,51,.05);
				display: flex;
				width: 100%;
				justify-content: center;
				align-items: center;
				height: 100%;
			}
	.calendar td.festival .text:hover {
				background: rgba(78,110,242,.1);
			}
	.calendar td.festival .text .new {
				color: #000;
				font-size: 24px;
				line-height: 1.4;
			}
	.calendar td.festival .text .old {
				color: #666;
				font-size: 16px;
				line-height: 1.4;
			}
	.calendar td.other .text {
				border-radius: 12px;
				flex-direction: column;
				background: #fff;
				display: flex;
				width: 100%;
				justify-content: center;
				align-items: center;
				opacity: 0.3;
				height: 100%;
			}
	.calendar td.other .text:hover {
				background: rgba(78,110,242,.1);
			}
	.calendar td.other .text .new {
				color: #000;
				font-size: 24px;
				line-height: 1.4;
			}
	.calendar td.other .text .old {
				color: #666;
				font-size: 16px;
				line-height: 1.4;
			}
	.calendar td.today .text {
				border-radius: 12px;
				flex-direction: column;
				color: #fff;
				background: rgb(64, 158, 255);
				display: flex;
				width: 100%;
				justify-content: center;
				align-items: center;
				height: 100%;
			}
	.calendar td.today .text:hover {
				background: rgba(64, 158, 255,.5);
			}
	.calendar td.today .text .new {
				color: inherit;
				font-size: 24px;
				line-height: 1.4;
			}
	.calendar td.today .text .old {
				color: inherit;
				font-size: 16px;
				line-height: 1.4;
			}
	
	// echarts1
	.type1 .echarts1 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 100%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type1 .echarts1:hover {
			}
	// echarts2
	.type2 .echarts1 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type2 .echarts1:hover {
			}
	.type2 .echarts2 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type2 .echarts2:hover {
			}
	// echarts3
	.type3 .echarts1 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 100%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type3 .echarts1:hover {
			}
	.type3 .echarts2 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type3 .echarts2:hover {
			}
	.type3 .echarts3 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type3 .echarts3:hover {
			}
	// echarts4
	.type4 .echarts1 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type4 .echarts1:hover {
			}
	.type4 .echarts2 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type4 .echarts2:hover {
			}
	.type4 .echarts3 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type4 .echarts3:hover {
			}
	.type4 .echarts4 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type4 .echarts4:hover {
			}
	// echarts5
	.type5 .echarts1 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 100%;
				position: relative;
				transition: 0.3s;
				height: 400px;
				order: 5;
			}
	.type5 .echarts1:hover {
			}
	.type5 .echarts2 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type5 .echarts2:hover {
			}
	.type5 .echarts3 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type5 .echarts3:hover {
			}
	.type5 .echarts4 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type5 .echarts4:hover {
			}
	.type5 .echarts5 {
				border-radius: 8px;
				padding: 20px;
				margin: 10px 0;
				background: rgba(255,255,255,1);
				width: 49%;
				position: relative;
				transition: 0.3s;
				height: 400px;
			}
	.type5 .echarts5:hover {
			}
	
	.echarts-flag-2 {
	  display: flex;
	  flex-wrap: wrap;
	  justify-content: space-between;
	  padding: 10px 20px;
	  background: rebeccapurple;
	
	  &>div {
	    width: 32%;
	    height: 300px;
	    margin: 10px 0;
	    background: rgba(255,255,255,.1);
	    border-radius: 8px;
	    padding: 10px 20px;
	  }
	}
</style>

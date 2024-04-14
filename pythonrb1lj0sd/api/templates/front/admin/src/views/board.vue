<template>
	<div class="content" :style='{"alignContent":"flex-start","overflow":"hidden","flexWrap":"wrap","background":"url(http://codegen.caihongy.cn/20231223/2592f2b1d33d45d299f892501055ed24.jpg) no-repeat center top / 100% 100%","display":"flex","width":"100%","position":"relative","height":"auto"}'>
		<div :style='{"width":"100%","margin":"0 auto","position":"relative","background":"url(http://codegen.caihongy.cn/20231223/3329204d7f474f8598c2b6cf87835385.gif) no-repeat center bottom / auto auto","height":"60px"}'>
			<!-- 标题 -->
			<div :style='{"margin":"0 auto","color":"#35c3d6","textAlign":"center","width":"50%","lineHeight":"50px","fontSize":"20px","fontWeight":"600","height":"60px"}'>欢迎使用 {{this.$project.projectName}}</div>
			<!-- 时间 -->
			<div :style='{"top":"10px","color":"#333","display":"inline-block","lineHeight":"40px","fontSize":"14px","position":"absolute","right":"60px","height":"64px"}'>{{ dates }}</div>
		</div>
		<div :style='{"width":"100%","padding":"10px 10px 0","overflow":"hidden","display":"flex","height":"calc(100vh - 60px)"}'>
			<div :style='{"width":"30%","padding":"0 0px","flexWrap":"wrap","display":"flex"}'>
				<div class="tables">
					<div :style='{"padding":"0 0 0px","margin":"0 0 20px","borderColor":"#00cdd780","color":"#5ed8e8","textAlign":"center","display":"inline-block","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","borderWidth":"0","width":"100%","fontSize":"14px","lineHeight":"10px","borderStyle":"solid","height":"40px"}'>房源信息</div>
					<el-table :data="boardDataList" border :stripe="false" :style='{"width":"100%","padding":"0","borderColor":"rgba(0,205,215,.3)","borderStyle":"solid","borderWidth":"1px 0 0 0px","background":"none"}'>
						<el-table-column :resizable="true" :sortable="false" prop="title" label="标题"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="totalprice" label="总价(万)"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="unitprice" label="单价(元/平)"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="huxing" label="户型"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="louceng" label="楼层"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="chaoxiang" label="朝向"/>
						</el-table-column>
						<el-table-column :resizable="true" :sortable="false" prop="xiaoqu" label="小区"/>
						</el-table-column>
					</el-table>
				</div>
				<div class="echarts1">
					<div :style='{"width":"100%","padding":"0 16px","margin":"0 0 0px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"14px","lineHeight":"10px"}'>
							小区
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","background":"url() no-repeat center top / 100% 100%","height":"calc(100% - 40px)"}'>
						<div id="fangyuanxinxiChart1" style="width:100%;height:100%;"></div>
					</div>
				</div>
				<div class="echarts2">
					<div :style='{"width":"100%","padding":"0 16px","margin":"0","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"16px","lineHeight":"10px"}'>
							户型
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","height":"calc(100% - 40px)"}'>
						<div id="fangyuanxinxiChart2" style="width:100%;height:100%;"></div>
					</div>
				</div>
			</div>
			<div :style='{"width":"40%","padding":"0 20px","flexWrap":"wrap","display":"flex"}'>
				<div v-if="systemIntroductionDetail" :style='{"padding":"0","margin":"0 0 20px","flexWrap":"wrap","background":"none","display":"flex","width":"100%","height":"calc(50% - 180px)","order":"2"}'>
					<el-carousel :style='{"width":"50%","margin":"0 auto"}' trigger="click" indicator-position="inside" arrow="always" type="default" direction="horizontal" height="100%" :autoplay="false" :interval="3000" :loop="true">
						<el-carousel-item :style='{"borderRadius":"10px","width":"100%","height":"100%"}' v-if="systemIntroductionDetail.picture1">
							<el-image :style='{"width":"100%","objectFit":"cover","borderRadius":"50%","height":"100%"}' :src="$base.url+ systemIntroductionDetail.picture1" fit="cover"></el-image>
						</el-carousel-item>
						<el-carousel-item :style='{"borderRadius":"10px","width":"100%","height":"100%"}' v-if="systemIntroductionDetail.picture2">
							<el-image :style='{"width":"100%","objectFit":"cover","borderRadius":"50%","height":"100%"}' :src="$base.url+ systemIntroductionDetail.picture2" fit="cover"></el-image>
						</el-carousel-item>
						<el-carousel-item :style='{"borderRadius":"10px","width":"100%","height":"100%"}' v-if="systemIntroductionDetail.picture3">
							<el-image :style='{"width":"100%","objectFit":"cover","borderRadius":"50%","height":"100%"}' :src="$base.url+ systemIntroductionDetail.picture3" fit="cover"></el-image>
						</el-carousel-item>
					</el-carousel>
					<div :style='{"padding":"0 10px","margin":"0 0 0 20px","borderColor":"#fff","color":"#666","textIndent":"2em","overflow":"hidden","borderRadius":"20%","flex":1,"background":"#fff","borderWidth":"20px 40px","lineHeight":"1.6","fontSize":"14px","borderStyle":"solid","height":"100%"}' v-html="systemIntroductionDetail.content"></div>
				</div>
				<!-- 统计 -->
				<div :style='{"padding":"0","margin":"10px auto 20px","color":"#666","alignItems":"center","display":"flex","justifyContent":"center","alignContent":"center","background":"none","width":"98%","fontSize":"14px","height":"130px","order":"1","zIndex":"9"}'>

					<div :style='{"border":"4px groove #6dedfe","margin":"0 2% 0 0","alignItems":"center","borderRadius":"50% 60%","background":"url() no-repeat center top / 100% 100%","flexDirection":"column","display":"flex","width":"18%","justifyContent":"center","height":"100%"}'>
						<div :style='{"width":"12px","margin":"0 0 0 20px","background":"#2097db","display":"none","height":"12px"}'></div>
						<div :style='{"padding":"0 0px","alignItems":"center","flexDirection":"column-reverse","display":"flex"}'>
							<div :style='{"lineHeight":"32px","fontSize":"inherit","color":"inherit","height":"32px"}'>房源信息总数</div>
							<div :style='{"lineHeight":"32px","fontSize":"26px","color":"#333","fontWeight":"bold","height":"32px"}'>{{fangyuanxinxiCount}}</div>
						</div>
					</div>

					<div :style='{"border":"4px groove #6dedfe","margin":"0 2% 0 0","alignItems":"center","borderRadius":"50% 60%","background":"url() no-repeat center top / 100% 100%","display":"flex","width":"18%","justifyContent":"center","height":"100%"}'>
						<div :style='{"width":"12px","margin":"0 0 0 20px","background":"yellow","display":"none","height":"12px"}'></div>
						<div :style='{"padding":"0 0px","alignItems":"center","flexDirection":"column-reverse","display":"flex"}'>
							<div :style='{"lineHeight":"32px","fontSize":"inherit","color":"inherit","height":"32px"}'>房价预测总数</div>
							<div :style='{"lineHeight":"32px","fontSize":"26px","color":"#333","fontWeight":"bold","height":"32px"}'>{{fangjiayuceCount}}</div>
						</div>
					</div>
				</div>
				<div class="echarts3">
					<div :style='{"width":"100%","padding":"0 16px","margin":"0 0 0px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"14px","lineHeight":"10px"}'>
							房屋总价
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","height":"calc(100% - 40px)"}'>
						<div id="fangyuanxinxiChart3" style="width:100%;height:100%;"></div>
					</div>
				</div>
			</div>
			<div :style='{"width":"30%","padding":"0 0px","flexWrap":"wrap","display":"flex"}'>
				<div class="echarts4">
					<div :style='{"width":"100%","padding":"0 16px","margin":"0 0 0px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"14px","lineHeight":"10px"}'>
							房屋单价
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","height":"calc(100% - 40px)"}'>
						<div id="fangyuanxinxiChart4" style="width:100%;height:100%;"></div>
					</div>
				</div>
				<div class="echarts5">
					<div :style='{"width":"100%","padding":"0 16px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"14px","lineHeight":"10px"}'>
							房价与面积统计
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","height":"calc(100% - 40px)"}'>
						<div id="fangyuanxinxiChart5" style="width:100%;height:100%;"></div>
					</div>
				</div>
				<div class="echarts6">
					<div :style='{"width":"100%","padding":"0 16px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat left center,url(http://codegen.caihongy.cn/20240201/49e2bd5a42f94890935fc4619090a954.png) no-repeat right center,url(http://codegen.caihongy.cn/20240201/b8af7cb3d8d145a29f398fd9e69f0f89.png) repeat-x center center","height":"40px"}'>
						<div :style='{"color":"#5ed8e8","fontSize":"14px","lineHeight":"10px"}'>
							房价预测
						</div>
					</div>
					<div :style='{"width":"100%","padding":"0 16px","height":"calc(100% - 40px)"}'>
						<div id="fangjiayuceChart6" style="width:100%;height:100%;"></div>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>
<script>
import * as echarts from 'echarts'
import chinaJson from "@/components/echarts/china.json";
import router from '@/router/router-static'
export default {
	data() {
		return {
			line: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#33c2e6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#33c2e6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c2e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":[0,10,0,0],"itemGap":5,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"#33c2e6","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"emphasis":{"lineStyle":{"color":"#000"}},"symbol":"emptyCircle","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"inheirt","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"showSymbol":true,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#33c2e6","shadowBlur":0,"width":2,"type":"solid","opacity":1,"shadowColor":"#000"},"symbolSize":4},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":[5,0,0,10],"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"0","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#33c2e6","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"}},
			line2: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#89b825","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#89b82580","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#89b82580","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#89b825","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#89b825","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#89b825","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#557510","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#89b825","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"#557510","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"emphasis":{"lineStyle":{"color":"#000"}},"symbol":"emptyCircle","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"inheirt","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"showSymbol":true,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#89b825","shadowBlur":0,"width":2,"type":"solid","opacity":1,"shadowColor":"#000"},"symbolSize":4},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#557510","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"}},
			line3: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#75d38d","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d80","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d80","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#75d38d","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#75d38d","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"#75d38d","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"symbol":"emptyCircle","areaStyle":{"color":"rgba(117,211,141,.2)"},"showSymbol":true,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#75d38d","shadowBlur":0,"width":2,"type":"solid","opacity":1,"shadowColor":"#000"},"symbolSize":4,"emphasis":{"lineStyle":{"color":"#000"}},"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"inheirt","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"}},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#75d38d","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"}},
			line4: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#23baa6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#23baa6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#23baa6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"#23baa6","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"symbol":"emptyCircle","areaStyle":{"color":"rgba(35,186,166,.3)"},"showSymbol":true,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#23baa6","shadowBlur":0,"width":2,"type":"solid","opacity":1,"shadowColor":"#000"},"symbolSize":4,"emphasis":{"lineStyle":{"color":"#000"}},"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"inheirt","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"}},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#23baa6","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"}},
			line5: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":15,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#1c44ac","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac80","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac80","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#1c44ac","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#1c44ac","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"#1c44ac","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"symbol":"emptyCircle","areaStyle":{"color":"rgba(28,68,172,.4)"},"showSymbol":true,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#1c44ac","shadowBlur":0,"width":2,"type":"solid","opacity":1,"shadowColor":"#000"},"symbolSize":4,"emphasis":{"lineStyle":{"color":"#000"}},"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#666","shadowOffsetY":0,"color":"inheirt","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"}},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#1c44ac","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"}},
			bar: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":12,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#2a8288","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a828880","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a828880","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a8288","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#2a8288","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a8288","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a8288","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#2a8288","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#2c99de","#28bbed","#60d3e3","#98dcb3","#ffd650","#ffa373","#ff7e91","#30dc75"],"legend":{"padding":[0,10,0,0],"itemGap":5,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"grid":{"x":"20%","y":"15%","y2":"15%","x2":"5%"},"series":{"barWidth":"12px","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#2a8288","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"barBorderRadius":[20,20,0,0],"opacity":1,"shadowColor":"#000"},"colorBy":"data","color":{"x":0,"y":0,"y2":1,"x2":0,"colorStops":[{"offset":0,"color":"#b7eef1"},{"offset":0.3,"color":"#b7eef1"},{"offset":0.6,"color":"#21c9d4"},{"offset":1,"color":"#2a8288"}],"type":"linear"},"barCategoryGap":"20%"},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":[5,0,0,10],"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"0","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#2a8288","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"base":{"animate":false,"interval":2000}},
			bar2: {"backgroundColor":"transparent","yAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":6,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#33c3e6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"120px","fontSize":12,"lineHeight":24,"interval":1,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e680","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(250,250,250,0.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"xAxis":{"axisLabel":{"borderType":"solid","rotate":0,"padding":0,"shadowOffsetX":0,"margin":4,"backgroundColor":"transparent","borderColor":"#000","shadowOffsetY":0,"color":"#33c3e6","shadowBlur":0,"show":true,"inside":false,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"width":"","fontSize":12,"lineHeight":24,"shadowColor":"transparent","fontWeight":"normal","height":""},"axisTick":{"show":true,"length":5,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"inside":false},"splitLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":false},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"cap":"butt","color":"#33c3e6","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"rgba(0,0,0,.5)"},"show":true},"splitArea":{"show":false,"areaStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"rgba(255,255,255,.3)","opacity":1,"shadowBlur":10,"shadowColor":"rgba(0,0,0,.5)"}}},"color":["#2c99de","#28bbed","#60d3e3","#98dcb3","#ffd650","#ffa373","#ff7e91","#30dc75"],"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"grid":{"x":"20%","y":"15%","y2":"15%","x2":"5%"},"series":{"barWidth":"","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#33c3e6","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"barBorderRadius":[0,20,20,0],"opacity":1,"shadowColor":"#000"},"colorBy":"data","color":{"x":1,"y":0,"y2":0,"x2":0,"colorStops":[{"offset":0,"color":"#a0e8ba"},{"offset":0.5,"color":"#65f4ca"},{"offset":1,"color":"#33c3e6"}],"type":"linear"},"barCategoryGap":"40%"},"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#33c3e6","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"base":{"animate":false,"interval":2000}},
			pie: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#2c99de","#28bbed","#60d3e3","#98dcb3","#bbdc98","#2cb3a5","#6180da","#98b2dc"],"title":{"borderType":"solid","padding":[5,0,0,10],"shadowOffsetX":0,"backgroundColor":"rgba(0,0,0,0)","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":4,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#ccc","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":[0,10,0,0],"itemGap":5,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"80%","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#5fd3e2","shadowOffsetY":0,"color":"","borderRadius":0,"shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"right":"auto","label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#5fd3e2","color":"inherit","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","formatter":"{b} {d}%","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#5fd3e2","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#89dadd","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false},"top":"10%"}},
			pie2: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#2c99de","#28bbed","#60d3e3","#98dcb3","#bbdc98","#2cb3a5","#6180da","#98b2dc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#fff","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"inherit","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#89dadd","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			pie3: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#2c99de","#28bbed","#60d3e3","#98dcb3","#ffd650","#ffa373","#ff7e91","#30dc75"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":12,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":14,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#fff","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"inherit","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"#89dadd","shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#eee","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel2: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":1,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel3: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":1,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel4: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":1,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			funnel5: {"tooltip":{"backgroundColor":"#333","textStyle":{"color":"#fff"}},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"borderType":"solid","padding":2,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":false,"right":"auto","top":"auto","borderRadius":0,"left":"center","borderWidth":1,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":0,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"vertical","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":true,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"left","borderWidth":0,"width":"auto","itemWidth":12,"textStyle":{"textBorderWidth":0,"color":"inherit","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"#000","shadowOffsetY":0,"color":"","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"#000"},"label":{"borderType":"solid","rotate":0,"padding":0,"textBorderWidth":0,"backgroundColor":"transparent","borderColor":"#fff","color":"","show":true,"textShadowColor":"transparent","distanceToLabelLine":5,"ellipsis":"...","overflow":"none","borderRadius":0,"borderWidth":0,"fontSize":12,"lineHeight":18,"textShadowOffsetX":0,"position":"outside","textShadowOffsetY":0,"textBorderType":"solid","textBorderColor":"#fff","textShadowBlur":0},"labelLine":{"show":true,"length":10,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"shadowBlur":0,"width":1,"type":"solid","opacity":1,"shadowColor":"#000"},"length2":14,"smooth":false}}},
			map: {"tooltip":{"formatter":"{b} : {c}","trigger":"item"},"backgroundColor":"transparent","title":{"borderType":"solid","padding":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"shadowBlur":0,"bottom":"auto","show":true,"right":"auto","top":"auto","borderRadius":0,"left":"left","borderWidth":0,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","fontSize":14,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"shadowColor":"transparent"},"legend":{"padding":5,"itemGap":10,"shadowOffsetX":0,"backgroundColor":"transparent","borderColor":"#ccc","shadowOffsetY":0,"orient":"horizontal","shadowBlur":0,"bottom":"auto","itemHeight":14,"show":false,"icon":"roundRect","itemStyle":{"borderType":"solid","shadowOffsetX":0,"borderColor":"inherit","shadowOffsetY":0,"color":"inherit","shadowBlur":0,"borderWidth":0,"opacity":1,"shadowColor":"transparent"},"right":"auto","top":"auto","borderRadius":0,"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"color":"inherit","shadowBlur":0,"width":"auto","type":"inherit","opacity":1,"shadowColor":"transparent"},"left":"right","borderWidth":0,"width":"auto","itemWidth":25,"textStyle":{"textBorderWidth":0,"color":"#fff","textShadowColor":"transparent","ellipsis":"...","overflow":"none","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"transparent","textShadowBlur":0},"shadowColor":"rgba(0,0,0,.3)","height":"auto"},"series":{"aspectScale":0.75,"itemStyle":{"emphasis":{"areaColor":"rgba(230,230,230,.9)"},"areaColor":"rgba(110,220,230,.8)"},"zoom":1.4,"label":{"emphasis":{"show":true,"textStyle":{"color":"inherit"}},"normal":{"color":"inherit","show":true}},"roam":true,"showLegendSymbol":false,"animation":false},"visualMap":{"min":0,"text":["High","Low"],"inRange":{"color":["#BCD4E6","#8090f0","#80f0ec","#B3EE3A","#FFF5EE"]},"max":100,"calculable":true,"seriesIndex":[0]}},
			boardBase: {"funnelNum":16,"lineNum":16,"gaugeNum":8,"barNum":16,"pieNum":16},
			gauge: {"tooltip":{"formatter":"{b} : {c}","trigger":"item"},"backgroundColor":"transparent","color":["#5470c6","#91cc75","#fac858","#ee6666","#73c0de","#3ba272","#fc8452","#9a60b4","#ea7ccc"],"title":{"show":false,"textStyle":{"textBorderWidth":0,"color":"#ccc","textShadowColor":"transparent","fontSize":12,"lineHeight":24,"textShadowOffsetX":0,"textShadowOffsetY":0,"textBorderType":"solid","fontWeight":500,"textBorderColor":"#ccc","textShadowBlur":0},"top":"top","left":"left"},"series":{"pointer":{"offsetCenter":[0,"10%"],"icon":"path://M2.9,0.7L2.9,0.7c1.4,0,2.6,1.2,2.6,2.6v115c0,1.4-1.2,2.6-2.6,2.6l0,0c-1.4,0-2.6-1.2-2.6-2.6V3.3C0.3,1.9,1.4,0.7,2.9,0.7z","width":5,"length":"80%"},"axisLine":{"lineStyle":{"shadowOffsetX":0,"shadowOffsetY":0,"opacity":0.5,"shadowBlur":1,"shadowColor":"#000"},"roundCap":true},"anchor":{"show":true,"itemStyle":{"color":"inherit"},"size":12,"showAbove":true},"emphasis":{"disabled":false},"progress":{"show":true,"roundCap":true,"overlap":true},"splitNumber":10,"detail":{"formatter":"{value}","backgroundColor":"inherit","color":"#fff","borderRadius":3,"width":20,"fontSize":12,"height":12},"title":{"color":"#666","fontSize":12},"animation":true}},
			myChart0: null,
			boardDataList: [],
            systemIntroductionDetail: null,
			dates: '',
            fangyuanxinxiCount: 0,
            fangjiayuceCount: 0,
		};
	},
  mounted(){
    this.init();
    this.getDataList();
  },
  created() {
    this.$nextTick(()=>{
		this.times()
		setTimeout(()=>{
			this.getfangyuanxinxiCount();
			this.fangyuanxinxiChat1();
			this.fangyuanxinxiChat2();
			this.fangyuanxinxiChat3();
			this.fangyuanxinxiChat4();
			this.fangyuanxinxiChat5();
			this.getfangjiayuceCount();
			this.fangjiayuceChat6();
		},500)
	})
  },
  methods:{
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
    getDataList() {
      let params = {
        page: 1,
        limit: 10,
      }
      this.$http({
        url: "fangyuanxinxi/page",
        method: "get",
        params: params
      }).then(({ data }) => {
        if (data && data.code === 0) {
          this.boardDataList = data.data.list;
        }
      });
    },
	wordclouds(wordcloudData,echartsId) {
		let wordcloud = {"maskImage":"data:image/png;base64,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","option":{"tooltip":{"show":false},"backgroundColor":"transparent","series":[{"sizeRange":[9,32],"layoutAnimation":true,"shape":"circle","data":[{"name":"花鸟市场","value":1446},{"name":"汽车","value":928},{"name":"视频","value":906},{"name":"电视","value":825},{"name":"Lover Boy 88","value":514},{"name":"动漫","value":486},{"name":"音乐","value":53},{"name":"直播","value":163},{"name":"广播电台","value":86},{"name":"戏曲曲艺","value":17},{"name":"演出票务","value":6},{"name":"给陌生的你听","value":1},{"name":"资讯","value":1437},{"name":"商业财经","value":422},{"name":"娱乐八卦","value":353},{"name":"军事","value":331},{"name":"科技资讯","value":313},{"name":"社会时政","value":307},{"name":"时尚","value":43},{"name":"网络奇闻","value":15},{"name":"旅游出行","value":438},{"name":"景点类型","value":957},{"name":"国内游","value":927},{"name":"远途出行方式","value":908},{"name":"酒店","value":693},{"name":"关注景点","value":611},{"name":"旅游网站偏好","value":512},{"name":"出国游","value":382},{"name":"交通票务","value":312},{"name":"旅游方式","value":187},{"name":"旅游主题","value":163},{"name":"港澳台","value":104},{"name":"本地周边游","value":3},{"name":"小卖家","value":1331},{"name":"全日制学校","value":941},{"name":"基础教育科目","value":585},{"name":"考试培训","value":473},{"name":"语言学习","value":358},{"name":"留学","value":246},{"name":"K12课程培训","value":207},{"name":"艺术培训","value":194},{"name":"技能培训","value":104},{"name":"IT培训","value":87},{"name":"高等教育专业","value":63},{"name":"家教","value":48},{"name":"体育培训","value":23},{"name":"职场培训","value":5},{"name":"金融财经","value":1328},{"name":"银行","value":765},{"name":"股票","value":452},{"name":"保险","value":415},{"name":"贷款","value":253},{"name":"基金","value":211},{"name":"信用卡","value":180},{"name":"外汇","value":138},{"name":"P2P","value":116},{"name":"贵金属","value":98},{"name":"债券","value":93},{"name":"网络理财","value":92},{"name":"信托","value":90},{"name":"征信","value":76},{"name":"期货","value":76},{"name":"公积金","value":40},{"name":"银行理财","value":36},{"name":"银行业务","value":30},{"name":"典当","value":7},{"name":"海外置业","value":1},{"name":"汽车","value":1309},{"name":"汽车档次","value":965},{"name":"汽车品牌","value":900},{"name":"汽车车型","value":727},{"name":"购车阶段","value":461},{"name":"二手车","value":309},{"name":"汽车美容","value":260},{"name":"新能源汽车","value":173},{"name":"汽车维修","value":155},{"name":"租车服务","value":136},{"name":"车展","value":121},{"name":"违章查询","value":76},{"name":"汽车改装","value":62},{"name":"汽车用品","value":37},{"name":"路况查询","value":32},{"name":"汽车保险","value":28},{"name":"陪驾代驾","value":4},{"name":"网络购物","value":1275},{"name":"做我的猫","value":1088},{"name":"只想要你知道","value":907},{"name":"团购","value":837},{"name":"比价","value":201},{"name":"海淘","value":195},{"name":"移动APP购物","value":179},{"name":"支付方式","value":119},{"name":"代购","value":43},{"name":"体育健身","value":1234},{"name":"体育赛事项目","value":802},{"name":"运动项目","value":405},{"name":"体育类赛事","value":337},{"name":"健身项目","value":199},{"name":"健身房健身","value":78},{"name":"运动健身","value":77},{"name":"家庭健身","value":36},{"name":"健身器械","value":29},{"name":"办公室健身","value":3},{"name":"商务服务","value":1201},{"name":"法律咨询","value":508},{"name":"化工材料","value":147},{"name":"广告服务","value":125},{"name":"会计审计","value":115},{"name":"人员招聘","value":101},{"name":"印刷打印","value":66},{"name":"知识产权","value":32},{"name":"翻译","value":22},{"name":"安全安保","value":9},{"name":"公关服务","value":8},{"name":"商旅服务","value":2},{"name":"展会服务","value":2},{"name":"特许经营","value":1},{"name":"休闲爱好","value":1169},{"name":"收藏","value":412},{"name":"摄影","value":393},{"name":"温泉","value":230},{"name":"博彩彩票","value":211},{"name":"美术","value":207},{"name":"书法","value":139},{"name":"DIY手工","value":75},{"name":"舞蹈","value":23},{"name":"钓鱼","value":21},{"name":"棋牌桌游","value":17},{"name":"KTV","value":6},{"name":"密室","value":5},{"name":"采摘","value":4},{"name":"电玩","value":1},{"name":"真人CS","value":1},{"name":"轰趴","value":1},{"name":"家电数码","value":1111},{"name":"手机","value":885},{"name":"电脑","value":543},{"name":"大家电","value":321},{"name":"家电关注品牌","value":253},{"name":"网络设备","value":162},{"name":"摄影器材","value":149},{"name":"影音设备","value":133},{"name":"办公数码设备","value":113},{"name":"生活电器","value":67},{"name":"厨房电器","value":54},{"name":"智能设备","value":45},{"name":"个人护理电器","value":22},{"name":"服饰鞋包","value":1047},{"name":"服装","value":566},{"name":"饰品","value":289},{"name":"鞋","value":184},{"name":"箱包","value":168},{"name":"奢侈品","value":137},{"name":"母婴亲子","value":1041},{"name":"孕婴保健","value":505},{"name":"母婴社区","value":299},{"name":"早教","value":103},{"name":"奶粉辅食","value":66},{"name":"童车童床","value":41},{"name":"关注品牌","value":271},{"name":"宝宝玩乐","value":30},{"name":"母婴护理服务","value":25},{"name":"纸尿裤湿巾","value":16},{"name":"妈妈用品","value":15},{"name":"宝宝起名","value":12},{"name":"童装童鞋","value":9},{"name":"胎教","value":8},{"name":"宝宝安全","value":1},{"name":"宝宝洗护用品","value":1},{"name":"软件应用","value":1018},{"name":"系统工具","value":896},{"name":"理财购物","value":440},{"name":"生活实用","value":365},{"name":"影音图像","value":256},{"name":"社交通讯","value":214},{"name":"手机美化","value":39},{"name":"办公学习","value":28},{"name":"应用市场","value":23},{"name":"母婴育儿","value":14},{"name":"游戏","value":946},{"name":"手机游戏","value":565},{"name":"PC游戏","value":353},{"name":"网页游戏","value":254},{"name":"游戏机","value":188},{"name":"模拟辅助","value":166},{"name":"个护美容","value":942},{"name":"护肤品","value":177},{"name":"彩妆","value":133},{"name":"美发","value":80},{"name":"香水","value":50},{"name":"个人护理","value":46},{"name":"美甲","value":26},{"name":"SPA美体","value":21},{"name":"花鸟萌宠","value":914},{"name":"绿植花卉","value":311},{"name":"狗","value":257},{"name":"其他宠物","value":131},{"name":"水族","value":125},{"name":"猫","value":122},{"name":"动物","value":81},{"name":"鸟","value":67},{"name":"宠物用品","value":41},{"name":"宠物服务","value":26},{"name":"书籍阅读","value":913},{"name":"网络小说","value":483},{"name":"关注书籍","value":128},{"name":"文学","value":105},{"name":"报刊杂志","value":77},{"name":"人文社科","value":22},{"name":"建材家居","value":907},{"name":"装修建材","value":644},{"name":"家具","value":273},{"name":"家居风格","value":187},{"name":"家居家装关注品牌","value":140},{"name":"家纺","value":107},{"name":"厨具","value":47},{"name":"灯具","value":43},{"name":"家居饰品","value":29},{"name":"家居日常用品","value":10},{"name":"生活服务","value":883},{"name":"物流配送","value":536},{"name":"家政服务","value":108},{"name":"摄影服务","value":49},{"name":"搬家服务","value":38},{"name":"物业维修","value":37},{"name":"婚庆服务","value":24},{"name":"二手回收","value":24},{"name":"鲜花配送","value":3},{"name":"维修服务","value":3},{"name":"殡葬服务","value":1},{"name":"求职创业","value":874},{"name":"创业","value":363},{"name":"目标职位","value":162},{"name":"目标行业","value":50},{"name":"兼职","value":21},{"name":"期望年薪","value":20},{"name":"实习","value":16},{"name":"雇主类型","value":10},{"name":"星座运势","value":789},{"name":"星座","value":316},{"name":"算命","value":303},{"name":"解梦","value":196},{"name":"风水","value":93},{"name":"面相分析","value":47},{"name":"手相","value":32},{"name":"公益","value":90}],"keepAspect":false,"type":"wordCloud","rotationRange":[-90,90],"gridSize":8,"shrinkToFit":false,"top":"center","left":"center","width":"80%","emphasis":{"focus":"self","textStyle":{"textShadowColor":"#333","textShadowBlur":4}},"drawOutOfBound":false,"rotationStep":45,"textStyle":{"color":"function(){return\"rgb(\"+[Math.round(Math.floor(Math.random() * (50 - 230)) + 230),Math.round(Math.floor(Math.random() * (50 - 230)) + 230),Math.round(Math.floor(Math.random() * (50 - 230)) + 230)].join(\",\")+\")\"}","fontWeight":500,"fontFamily":"sans-serif"},"height":"80%","maskImage":{}}]}}
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
    getTimeStrToDay(game_over_timestamp) {
        if (game_over_timestamp == 0)
            return "";
        var date = new Date(parseInt(game_over_timestamp));
        var now = new Date();
        var hours = date.getHours() >= 10 ? date.getHours().toString() : "0" + date.getHours();
        var minutes = date.getMinutes() >= 10 ? date.getMinutes().toString() : "0" + date.getMinutes();
        var seconds = date.getSeconds() >= 10 ? date.getSeconds().toString() : "0" + date.getSeconds();
        let arr = ["日", "一", "二", "三", "四", "五", "六"];
        let d = arr[date.getDay()]
        return date.getFullYear() + "年" + (date.getMonth() + 1) + "月" + date.getDate() + '日' + ' ' + ' ' + '星期' + d  + ' ' + "  " + hours + ":" + minutes + ":" + seconds
    },
	times() {
		setInterval(()=>{
            let date = new Date().getTime()
            this.dates = this.getTimeStrToDay(date)
		}, 1000)
	},
	filterTime(time) {
	  const date = new Date(time)
	  const Y = date.getFullYear()
	  const M = date.getMonth() + 1 < 10 ? '0'+(date.getMonth()+1) : date.getMonth()+1 
	  const D = date.getDate() < 10 ? '0' + date.getDate() : date.getDate()
	  
	  const H = date.getHours() < 10 ? '0' + date.getHours() : date.getHours() // 小时
	  const I = date.getMinutes() < 10 ? '0' + date.getMinutes() : date.getMinutes() // 分钟
	  const S = date.getSeconds() < 10 ? '0' + date.getSeconds() : date.getSeconds() // 秒
	  
	  return `${Y}-${M}-${D} ${H}:${I}:${S}`
	},
    getSystemIntroduction() {
        this.$http({
            url: `systemintro/detail/1`,
            method: "get"
        }).then(({
            data
        }) => {
            if (data && data.code == 0) {
                this.systemIntroductionDetail = data.data
            }
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
        this.getSystemIntroduction();
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

//统计接口1
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

//统计接口2
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
				let tooltipObj = {
					trigger: 'item',
					formatter: "{b} : {c}"
				}
				tooltipObj = Object.assign(tooltipObj , this.funnel.tooltip?this.funnel.tooltip:{})
				option = {
					backgroundColor: this.funnel.backgroundColor,
					color: this.funnel.color,
				    title: titleObj,
				    legend: legendObj,
				    tooltip: tooltipObj,
					grid: gridObj,
				    series: seriesObj,
				}
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart2.setOption(option);
				

                  //根据窗口的大小变动图表
				window.addEventListener('resize', () => {
                    fangyuanxinxiChart2.resize();
                },false);
            }
        });
      })
    },

//统计接口3
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
				
				let xAxisObj = this.line.xAxis
				xAxisObj.type = 'category'
				xAxisObj.boundaryGap = false
				xAxisObj.data = xAxis
                
				
				let yAxisObj = this.line.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
					data: yAxis,
					type: 'line',
				}
				seriesObj = Object.assign(seriesObj , this.line.series)
				const gridObj = this.line.grid
				let tooltipObj = {
					trigger: 'item',
					formatter: '{b} : {c}'
				}
				tooltipObj = Object.assign(tooltipObj , this.line.tooltip?this.line.tooltip:{})
                option = {
					backgroundColor: this.line.backgroundColor,
					color: this.line.color,
                    title: titleObj,
					legend: legendObj,
                    tooltip: tooltipObj,
					grid: gridObj,
                    xAxis: xAxisObj,
                    yAxis: yAxisObj,
                    series: [seriesObj]
                };
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart3.setOption(option);
				

                  //根据窗口的大小变动图表
				window.addEventListener('resize', () => {
                    fangyuanxinxiChart3.resize();
                },false);
            }
        });
      })
    },

//统计接口4
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
				let titleObj = this.line4?this.line4.title:this.line.title
				titleObj.text = '房屋单价'
				
				const legendObj = this.line4?this.line4.legend:this.line.legend
				
				let xAxisObj = this.line.xAxis
				xAxisObj.type = 'category'
				xAxisObj.boundaryGap = false
				xAxisObj.data = xAxis
                
				
				let yAxisObj = this.line4?this.line4.yAxis:this.line.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
					data: yAxis,
					type: 'line',
					areaStyle: {},
					smooth: true
				}
				seriesObj = Object.assign(seriesObj , this.line4?this.line4.series:this.line.series)
				const gridObj = this.line4.grid
				let tooltipObj = {
					trigger: 'item',
					formatter: '{b} : {c}'
				}
				tooltipObj = Object.assign(tooltipObj , this.line4.tooltip?this.line4.tooltip:{})
                option = {
					backgroundColor: this.line4?this.line4.backgroundColor:this.line.backgroundColor,
					color: this.line4?this.line4.color:this.line.color,
                    title: titleObj,
					legend: legendObj,
                    tooltip: tooltipObj,
					grid: gridObj,
                    xAxis: xAxisObj,
                    yAxis: yAxisObj,
                    series: [seriesObj]
                };
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart4.setOption(option);
				

                  //根据窗口的大小变动图表
				window.addEventListener('resize', () => {
                    fangyuanxinxiChart4.resize();
				},false);
            }
        });
      })
    },

//统计接口5
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
				
				let xAxisObj = this.bar.xAxis
				xAxisObj.type = 'category'
				xAxisObj.data = xAxis
                
				
				let yAxisObj = this.bar.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
                        data: yAxis,
                        type: 'bar'
                    }
				seriesObj = Object.assign(seriesObj , this.bar.series)
				const gridObj = this.bar.grid
				let tooltipObj = {
					trigger: 'item',
					formatter: '{b} : {c}'
				}
				tooltipObj = Object.assign(tooltipObj , this.bar.tooltip?this.bar.tooltip:{})
                option = {
					backgroundColor: this.bar.backgroundColor,
					color: this.bar.color,
                    title: titleObj,
					legend: legendObj,
                    tooltip: tooltipObj,
					grid: gridObj,
                    xAxis: xAxisObj,
                    yAxis: yAxisObj,
                    series: [seriesObj]
                };
                // 使用刚指定的配置项和数据显示图表。
                fangyuanxinxiChart5.setOption(option);


                  //根据窗口的大小变动图表
				window.addEventListener('resize', () => {
                    fangyuanxinxiChart5.resize();
					},false);
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

//统计接口1
    fangjiayuceChat6() {
      this.$nextTick(()=>{

        var fangjiayuceChart6 = echarts.init(document.getElementById("fangjiayuceChart6"),'macarons');
        this.$http({
            url: `fangjiayuce/value/yucenianfen/fangjiayuce`,
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
                    xAxis.push(res[i].yucenianfen);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].yucenianfen
                    })
                }
                var option = {};
				let titleObj = this.line.title
				titleObj.text = '房价预测'
				
				const legendObj = this.line.legend
				
				let xAxisObj = this.line.xAxis
				xAxisObj.type = 'category'
				xAxisObj.boundaryGap = false
				xAxisObj.data = xAxis
                
				
				let yAxisObj = this.line.yAxis
				yAxisObj.type = 'value'
				
				let seriesObj = {
					data: yAxis,
					type: 'line',
				}
				seriesObj = Object.assign(seriesObj , this.line.series)
				const gridObj = this.line.grid
				let tooltipObj = {
					trigger: 'item',
					formatter: '{b} : {c}'
				}
				tooltipObj = Object.assign(tooltipObj , this.line.tooltip?this.line.tooltip:{})
                option = {
					backgroundColor: this.line.backgroundColor,
					color: this.line.color,
                    title: titleObj,
					legend: legendObj,
					grid: gridObj,
                    tooltip: tooltipObj,
                    xAxis: xAxisObj,
                    yAxis: yAxisObj,
                    series: [seriesObj]
                };
                // 使用刚指定的配置项和数据显示图表。
                fangjiayuceChart6.setOption(option);
				

                  //根据窗口的大小变动图表
				window.addEventListener('resize', () => {
                    fangjiayuceChart6.resize();
                },false);
            }
        });
      })
    },









  }
};
</script>
<style lang="scss" scoped>
	.tables {
				padding: 0 10px;
				margin: 0 0 20px;
				background: none;
				width: 100%;
				overflow-y: scroll;
				transition: 0.3s;
				height: calc(32% - 20px);
				order: 2;
			}
	.tables:hover {
			}
	// table
	.el-table:before{
		display: none;
	}
	.el-table:after{
		display: none;
	}
	.el-table /deep/ .el-table__header-wrapper thead {
				color: #fff;
				font-weight: 500;
				width: 100%;
			}
	
	.el-table /deep/ .el-table__header-wrapper thead tr {
				background: rgba(0,205,215,.1);
			}
	
	.el-table /deep/ .el-table__header-wrapper thead tr th {
				padding: 6px 0;
				background: none;
				border-color: rgba(0,205,215,.3);
				border-width: 0 0px 1px 0;
				border-style: solid;
				text-align: left;
			}
	
	.el-table /deep/ .el-table__header-wrapper thead tr th .cell {
				padding: 0 10px;
				word-wrap: normal;
				color: #333;
				word-break: break-all;
				white-space: normal;
				font-weight: bold;
				display: inline-block;
				vertical-align: middle;
				width: 100%;
				line-height: 24px;
				position: relative;
				text-overflow: ellipsis;
			}
	
	.el-table /deep/ .el-table__body-wrapper tbody {
				width: 100%;
			}
	
	.el-table /deep/ .el-table__body-wrapper tbody tr {
				background: none;
			}
	
	.el-table /deep/ .el-table__body-wrapper tbody tr td {
				padding: 6px 0;
				color: #666;
				background: none;
				border-color: rgba(0,205,215,.3);
				border-width: 0 0px 1px 0;
				border-style: solid;
				text-align: left;
			}
	
	
	.el-table /deep/ .el-table__body-wrapper tbody tr:hover td {
				padding: 6px 0;
				color: #666;
				background: none;
				border-color: rgba(0,205,215,.3);
				border-width: 0 0px 1px 0;
				border-style: solid;
				text-align: left;
			}
	
	.el-table /deep/ .el-table__body-wrapper tbody tr td {
				padding: 6px 0;
				color: #666;
				background: none;
				border-color: rgba(0,205,215,.3);
				border-width: 0 0px 1px 0;
				border-style: solid;
				text-align: left;
			}
	
	.el-table /deep/ .el-table__body-wrapper tbody tr td .cell {
				padding: 0 10px;
				overflow: hidden;
				word-break: break-all;
				white-space: normal;
				line-height: 24px;
				text-overflow: ellipsis;
			}
	.echarts1 {
				padding: 0;
				margin: 0 0 20px 0;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(32% - 20px);
			}
	.echarts1:hover {
			}
	.echarts2 {
				padding: 0;
				margin: 0 0 20px 0;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(36% - 20px);
				order: 3;
			}
	.echarts2:hover {
			}
	.echarts3 {
				padding: 0;
				margin: 0 0 20px 0;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(50% - 20px);
				order: 3;
			}
	.echarts3:hover {
			}
	.echarts4 {
				padding: 0;
				margin: 0 0 20px;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(32% - 20px);
				order: 4;
			}
	.echarts4:hover {
			}
	.echarts5 {
				padding: 0;
				margin: 0 0 20px;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(32% - 20px);
				order: 5;
			}
	.echarts5:hover {
			}
	.echarts6 {
				padding: 0;
				margin: 0 0 20px;
				background: none;
				width: 100%;
				transition: 0.3s;
				height: calc(36% - 20px);
				order: 6;
			}
	.echarts6:hover {
			}
</style>


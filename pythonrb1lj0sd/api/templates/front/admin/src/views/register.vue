<template>
	<div>
		<div class="container" :style='{"minHeight":"100vh","alignItems":"center","background":"url(http://codegen.caihongy.cn/20231210/0fee9409214b41bfb94d032d3e1d9dbc.webp)","display":"flex","width":"100%","backgroundSize":"100% 100%","backgroundPosition":"center","backgroundRepeat":"no-repeat","justifyContent":"center"}'>
			<el-form v-if="pageFlag=='register'" :style='{"border":"10px solid #39b6e2","padding":"90px 60px 40px","boxShadow":"0px 1px 9px #21667f80","margin":"60px 0 20px","borderRadius":"30px","flexWrap":"wrap","background":"url(http://codegen.caihongy.cn/20231218/84c7bb0e65334a18b669a33041d75403.png) no-repeat center top / 100% auto,url(http://codegen.caihongy.cn/20231218/83bfa299c46a43d48aade06fcb1d9431.png) no-repeat center bottom / 100% auto,url(http://codegen.caihongy.cn/20231218/67cb8922dcdb4603ae570289e0708697.png) repeat-y center 10px / 100% auto,#fff","display":"flex","width":"900px","position":"relative","height":"auto"}' ref="rgsForm" class="rgs-form" :model="rgsForm" :rules="rules">
				<div v-if="true" :style='{"margin":"0 auto","color":"#000000","top":"-33px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20231218/a7b3487a278d4db6990f0207171527db.png) no-repeat center top / 100%  100% ","width":"800px","lineHeight":"90px","fontSize":"22px","position":"absolute","fontWeight":"600","height":"89px"}' class="title">大数据的郑州市房地产分析与趋势预测注册</div>
				<button :style='{"border":"0","cursor":"pointer","padding":"0 10px","margin":"20px auto 5px","outline":"none","color":"#fff","borderRadius":"8px","background":"#5CE5FB","display":"block","width":"60%","fontSize":"16px","height":"44px"}' type="button" class="r-btn" @click="login()">注册</button>
				<div :style='{"cursor":"pointer","padding":"0 10%","margin":"10px auto","color":"rgba(159, 159, 159, 1)","textAlign":"center","display":"inline-block","width":"60%","lineHeight":"1","fontSize":"12px"}' class="r-login" @click="close()">已有账号，直接登录</div>
			</el-form>
			
		</div>
	</div>
</template>

<script>
export default {
	data() {
		return {
			ruleForm: {
			},
			forgetForm: {},
            pageFlag : '',
			tableName:"",
			rules: {},
		};
	},
	mounted(){
		this.pageFlag = this.$route.query.pageFlag
		if(this.$route.query.pageFlag=='register'){
			
			let table = this.$storage.get("loginTable");
			this.tableName = table;
		}
	},
	created() {
	},
	destroyed() {
		  	},
	methods: {
		changeRules(name){
			if(this.rules[name]){
				return true
			}
			return false
		},
		// 获取uuid
		getUUID () {
			return new Date().getTime();
		},
		close(){
			this.$router.push({ path: "/login" });
		},

        // 多级联动参数


		// 注册
		login() {
			var url=this.tableName+"/register";
			this.$http({
				url: url,
				method: "post",
				data:this.ruleForm
			}).then(({ data }) => {
				if (data && data.code === 0) {
					this.$message({
						message: "注册成功",
						type: "success",
						duration: 1500,
						onClose: () => {
							this.$router.replace({ path: "/login" });
						}
					});
				} else {
					this.$message.error(data.msg);
				}
			});
		}
	}
};
</script>

<style lang="scss" scoped>
	.container {
	  position: relative;
	  background: url(http://codegen.caihongy.cn/20231210/0fee9409214b41bfb94d032d3e1d9dbc.webp);

		.el-date-editor.el-input {
		  width: 100%;
		}
		
		.rgs-form .el-input /deep/ .el-input__inner {
						border: 1px solid #D7D7D7;
						border-radius: 8px;
						padding: 0 10px;
						color: #818181;
						width: 250px;
						font-size: 14px;
						height: 44px;
					}
		
		.rgs-form .el-select /deep/ .el-input__inner {
						border: 1px solid #D7D7D7;
						border-radius: 8px;
						padding: 0 10px;
						color: #818181;
						width: 250px;
						font-size: 14px;
						height: 44px;
					}
		
		.rgs-form .el-date-editor /deep/ .el-input__inner {
						border: 1px solid #D7D7D7;
						border-radius: 8px;
						padding: 0 10px 0 30px;
						color: #818181;
						width: 250px;
						font-size: 14px;
						height: 44px;
					}
		
		.rgs-form .el-date-editor /deep/ .el-input__inner {
						border: 1px solid #D7D7D7;
						border-radius: 8px;
						padding: 0 10px 0 30px;
						color: #818181;
						width: 250px;
						font-size: 14px;
						height: 44px;
					}
		
		.rgs-form /deep/ .el-upload--picture-card {
			background: transparent;
			border: 0;
			border-radius: 0;
			width: auto;
			height: auto;
			line-height: initial;
			vertical-align: middle;
		}
		
		.rgs-form /deep/ .upload .upload-img {
		  		  border: 1px dashed #D7D7D7;
		  		  cursor: pointer;
		  		  border-radius: 8px;
		  		  color: #aaa;
		  		  width: 100px;
		  		  font-size: 32px;
		  		  line-height: 80px;
		  		  text-align: center;
		  		  height: 80px;
		  		}
		
		.rgs-form /deep/ .el-upload-list .el-upload-list__item {
		  		  border: 1px dashed #D7D7D7;
		  		  cursor: pointer;
		  		  border-radius: 8px;
		  		  color: #aaa;
		  		  width: 100px;
		  		  font-size: 32px;
		  		  line-height: 80px;
		  		  text-align: center;
		  		  height: 80px;
		  		}
		
		.rgs-form /deep/ .el-upload .el-icon-plus {
		  		  border: 1px dashed #D7D7D7;
		  		  cursor: pointer;
		  		  border-radius: 8px;
		  		  color: #aaa;
		  		  width: 100px;
		  		  font-size: 32px;
		  		  line-height: 80px;
		  		  text-align: center;
		  		  height: 80px;
		  		}
	}
	.required {
		position: relative;
	}
	.required::after{
				color: red;
				top: 4px;
				position: absolute;
				right: 5px;
				content: "*";
			}
	.editor>.avatar-uploader {
		line-height: 0;
		height: 0;
	}
</style>

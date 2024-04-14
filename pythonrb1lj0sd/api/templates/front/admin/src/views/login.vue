<template>
  <div>
    <div class="container" :style='{"minHeight":"100vh","alignItems":"center","backgroundImage":"url(http://codegen.caihongy.cn/20231210/0fee9409214b41bfb94d032d3e1d9dbc.webp)","display":"flex","width":"100%","backgroundSize":"100% 100%","backgroundPosition":"center","backgroundRepeat":"no-repeat","justifyContent":"center"}'>
      <el-form :style='{"border":"10px solid #39b6e2","padding":"90px 60px 40px","boxShadow":"0px 1px 9px #21667f80","margin":"60px 0 20px","borderRadius":"30px","background":"#fff","width":"900px","backgroundSize":"100% 100%","position":"relative","height":"auto"}'>
        <div v-if="true" :style='{"margin":"0 auto","color":"#000000","top":"-33px","textAlign":"center","background":"url(http://codegen.caihongy.cn/20231218/a7b3487a278d4db6990f0207171527db.png) no-repeat center top / 100%  100% ","width":"800px","lineHeight":"90px","fontSize":"22px","position":"absolute","fontWeight":"600","height":"89px"}' class="title-container">大数据的郑州市房地产分析与趋势预测登录</div>
        <div v-if="loginType==1" class="list-item" :style='{"width":"60%","margin":"0 auto 30px","alignItems":"center","display":"flex"}'>
          <div v-if="true" class="lable" :style='{"width":"84px","flex-shrink":"0","lineHeight":"44px","fontSize":"14px","color":"#818181","textAlign":"right"}'>用户名：</div>
          <input :style='{"border":"#818181","padding":"0 10px","outline":"1px solid #D7D7D7","color":"#818181","width":"100%","fontSize":"14px","height":"44px"}' placeholder="请输入用户名" name="username" type="text" v-model="rulesForm.username">
        </div>
        <div v-if="loginType==1" class="list-item" :style='{"width":"60%","margin":"0 auto 30px","alignItems":"center","display":"flex"}'>
          <div v-if="true" class="lable" :style='{"width":"84px","flex-shrink":"0","lineHeight":"44px","fontSize":"14px","color":"#818181","textAlign":"right"}'>密码：</div>
          <input :style='{"border":"#818181","padding":"0 10px","outline":"1px solid #D7D7D7","color":"#818181","width":"100%","fontSize":"14px","height":"44px"}' placeholder="请输入密码" name="password" type="password" v-model="rulesForm.password">
        </div>

        <div :style='{"width":"80%","textAlign":"center","margin":"20px auto"}' v-if="roles.length>1" prop="loginInRole" class="list-type">
          <el-radio v-if="loginType==1||(loginType==2&&item.roleName!='管理员')" v-for="item in roles" v-bind:key="item.roleName" v-model="rulesForm.role" :label="item.roleName">{{item.roleName}}</el-radio>
        </div>

		
        <div :style='{"padding":"0 0 0 74px","margin":"20px auto","alignItems":"center","flexWrap":"wrap","display":"flex","width":"60%","justifyContent":"center"}'>
          <el-button v-if="loginType==1" :style='{"border":"0","cursor":"pointer","padding":"0 24px","margin":"0 10px 10px","outline":"none","color":"#fff","borderRadius":"4px","background":"#5CE5FB","width":"100%","fontSize":"14px","height":"44px"}' type="primary" @click="login()" class="loginInBt">登录</el-button>
        </div>
      </el-form>

    </div>
  </div>
</template>
<script>
import menu from "@/utils/menu";
export default {
  data() {
    return {
		verifyCheck2: false,
		flag: false,
      baseUrl:this.$base.url,
      loginType: 1,
      rulesForm: {
        username: "",
        password: "",
        role: "",
      },
      menus: [],
      roles: [],
      tableName: "",
    };
  },
  mounted() {
    let menus = menu.list();
    this.menus = menus;

    for (let i = 0; i < this.menus.length; i++) {
      if (this.menus[i].hasBackLogin=='是') {
        this.roles.push(this.menus[i])
      }
    }

  },
  created() {

  },
  destroyed() {
	    },
  components: {
  },
  methods: {

    //注册
    register(tableName){
		this.$storage.set("loginTable", tableName);
		this.$router.push({path:'/register',query:{pageFlag:'register'}})
    },
    // 登陆
    login() {

		if (!this.rulesForm.username) {
			this.$message.error("请输入用户名");
			return;
		}
		if (!this.rulesForm.password) {
			this.$message.error("请输入密码");
			return;
		}
		if(this.roles.length>1) {
			if (!this.rulesForm.role) {
				this.$message.error("请选择角色");
				return;
			}

			let menus = this.menus;
			for (let i = 0; i < menus.length; i++) {
				if (menus[i].roleName == this.rulesForm.role) {
					this.tableName = menus[i].tableName;
				}
			}
		} else {
			this.tableName = this.roles[0].tableName;
			this.rulesForm.role = this.roles[0].roleName;
		}
		
		this.loginPost()
    },
	loginPost() {
		this.$http({
			url: `${this.tableName}/login?username=${this.rulesForm.username}&password=${this.rulesForm.password}`,
			method: "post"
		}).then(({ data }) => {
			if (data && data.code === 0) {
				this.$storage.set("Token", data.token);
				this.$storage.set("role", this.rulesForm.role);
				this.$storage.set("sessionTable", this.tableName);
				this.$storage.set("adminName", this.rulesForm.username);
				this.$router.replace({ path: "/" });
			} else {
				this.$message.error(data.msg);
			}
		});
	},
  }
}
</script>

<style lang="scss" scoped>
.container {
  min-height: 100vh;
  position: relative;
  background-repeat: no-repeat;
  background-position: center center;
  background-size: cover;
          
  .list-item /deep/ .el-input .el-input__inner {
		border: #818181;
		padding: 0 10px;
		outline: 1px solid #D7D7D7;
		color: #818181;
		width: 100%;
		font-size: 14px;
		height: 44px;
	  }
  
  .list-item.select /deep/ .el-select .el-input__inner {
		border: 1px solid #D7D7D7;
		padding: 0 10px;
		color: #818181;
		width: 288px;
		font-size: 14px;
		height: 44px;
	  }
  
  .list-code /deep/ .el-input .el-input__inner {
  	  	border: 1px solid #D7D7D7;
  	  	padding: 0 20px;
  	  	outline: none;
  	  	margin: 0;
  	  	color: #818181;
  	  	flex: 1;
  	  	font-size: 14px;
  	  	height: 44px;
  	  }

  .list-type /deep/ .el-radio__input .el-radio__inner {
		background: rgba(53, 53, 53, 0);
		border-color: #666666;
	  }
  .list-type /deep/ .el-radio__input.is-checked .el-radio__inner {
        background: #5CE5FB;
        border-color: #5CE5FB;
      }
  .list-type /deep/ .el-radio__label {
		color: #666666;
		font-size: 14px;
	  }
  .list-type /deep/ .el-radio__input.is-checked+.el-radio__label {
        color: #5CE5FB;
        font-size: 14px;
      }
}

</style>

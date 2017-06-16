# 用户标签

## 数据源

* 流量日志=hdfs:///logs/traffic/out-20170614??.log.gz
* 发号日志(giftID->gameID)=file:///mfs/home/qingchuanzheng/work/all_hao.csv
* 用户行为日志=hdfs:///logs/gift/behaviors/20170614.csv

## 程序部署

### 编译
	$ sbt "assembly"

### 部署
	$ sbt "deploy"

编译后的 Jar 包位置：/home/stat/hugsh/codeJar/UserBehavior.jar

### 每日任务执行任务命令
	/mfs/sparkProduct/celeryFalsk/userTagETL.sh YYYYmmdd


## 代码结构

	README.md	# 文档
	build.sbt	# 编译配置工具
	project	
	custom_lib	# 外部库导入，目前只有 oracle
	src
	├── main
	│   ├── resources
	│   │   ├── application.conf	# 常规配置（数据库连接，日志源定义）
	│   │   └── hadoop-conf			# Hadoop 配置信息
	│   └── scala
	│       ├── models				# 数据 ORM
	│       │   ├── CmsDB.scala		# CMS 相关的表
	│       │   ├── Game.scala		# SOA 游戏库相关的表
	│       │   └── UserTagDB.scala	# 用户标签存储的运行配置表
	│       ├── munge						# 数据ETL层（每天运行一次）
	│       │   ├── BBSBehavior.scala		# 论坛行为
	│       │   ├── GameInfo.scala			# SOA 游戏库导入
	│       │   ├── GiftBehavior.scala		# 发号行为
	│       │   ├── GiftInfo.scala			# 礼包信息导入
	│       │   └── Log2Par.scala			# 流量日志格式化（提取登录用户信息）
	│       └── UserTag.scala		# 用户标签关联分析（实时运行）
	└── test
	    ├── resources
	    │   ├── application.conf	# 测试配置
	    │   └── ssh-tunnel.sh		# 线上环境管道
	    └── scala					# 测试用例	
	        ├── BBSSpec.scala
	        ├── GameInfoSpec.scala
	        ├── GiftSpec.scala
	        ├── ModelSpec.scala
	        ├── org					# spark 测试套件封装
	        │   └── nextchen
	        └── SparkExampleSpec.scala
	

### 程序入口com.cyou.userBehavior.fahao.UserBehavior_Gift
需求
  * 记录登录用户在发号平台访问和动作（领号、预定、参与活动）；
  * （uid、gamecode、动作1、动作2、动作3、time）

步骤1
  * 用户访问--礼包行为日志
    * 输入= 流量日志 + 礼包字典数据（key=giftID,value=GameCode）
    * 流量日志中过滤出登入用户的（访问时间,日期，用户id,url）
    * 通过正则表达式过滤出符合发号相关的url，url中获取giftid
    * giftid通过礼包字典数据匹配到gamecode
    * 添加一个字段：行为，值=1，代表访问动作
    * 最后得到（访问时间,日期，用户id ，gameCode，behavior）
    * partitionBy("behavior", "日期")
步骤2
  * 用户预订，领取和活动---礼包行为日志
    * 输入=每天通过curl下载的用户礼包行为日志
    * 包含字段（访问时间,用户id ，gameCode，behavior中文）
    * 添加日期字段，behavior中文转换成数字="领号" -> 2, "预定" -> 3, "活动" -> 4
    * partitionBy("behavior", "日期")


### 用户（访问+行为）结果集入库

      $ PGPASSWORD=9pf7ewzlpCohptJScTWc psql -h 10.59.67.166 -p 5432 -U ubi --db bi \
    	-c "\COPY hao_source(game_code,gift_id,client_id,source,directory,source_url,visit_url,release,participation,reserve,pv,uv,d_date) from '{数据文件的位置}' CSV HEADER"

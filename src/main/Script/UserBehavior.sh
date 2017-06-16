./UserBehavior.sh fahao 20170613
====================================================================================================================
#!/bin/bash

#开启错误检查，报错即退出
set -e

LOGTYPE=$1
D_DATE=$2
# 2017040100


if [ "$LOGTYPE" == "fahao" ]; then
        #发号浏览+行为日志
        # 补数据: fahao 2017040100 [2017041023]
        time spark2-submit --master yarn  --class com.cyou.userBehavior.fahao.UserBehavior_Gift --executor-memory 2G  \
         --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/mfs/home/hugsh/conf/userBehavior_log4j.properties"  \
          /mfs/home/hugsh/codeJar/UserBehavior.jar    $D_DATE

for i in `ls /hdfs/logs/userBehavior/gift/date\=$D_DATE/*csv`;do
	echo $i;
	load_sql="LOAD DATA LOCAL  INFILE '$i'
				INTO TABLE UserBehavior_Gift
				CHARACTER SET utf8
				FIELDS TERMINATED BY ','
				ENCLOSED BY '\"'  "
	mysql  -h10.59.67.213 -P3306   -uuser_relation  -p123   user_relation -e "${load_sql}"
done;



elif [ "$LOGTYPE" == "CMS" ]; then
       echo "CMS hai mei zuo "
else
       echo "param is wrong"

fi


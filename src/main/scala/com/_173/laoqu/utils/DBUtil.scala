package com._173.laoqu.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.Map

object DBUtil {

  //--jars /home/stat/hugsh/ojdbc6.jar
//  --driver-class-path /home/stat/hugsh/ojdbc6.jar \
//  http://blog.csdn.net/qq_14950717/article/details/51323679

  val driver="oracle.jdbc.driver.OracleDriver"//oracle.jdbc.driver.OracleDriver  com.mysql.jdbc.Driver
  val url = "jdbc:oracle:thin:@10.59.67.79:1521:LOG"
  val userName = "web_stat"
  val password = "web20080522"
  val sql="select A_CODE, A_NAME from STAT_ADV "
  val tableName="STAT_ADV";//select fiels from sourceDBName  可以直接在这边过滤查询数据
  val jdbcMap:Map[String, String]=Map(("driver" , driver), ("url" , url), ("user" , userName), ("password" , password),("dbtable" , tableName))

  def  getAdsCode_Name(sqlContext: SQLContext):RDD[(String, String)]  ={
      val jdbcDF = sqlContext.read.format("jdbc").options(jdbcMap).load()
      //  jdbcDF.take(3)
      jdbcDF.createOrReplaceTempView("STAT_ADV")
      val adsCodeName: RDD[(String, String)] = sqlContext.sql(sql).rdd.map(r => (r(0).toString, r(1).toString))
      //adsCodeName.collectAsMap()
    adsCodeName
  }

  def getDelAdsCode4(sparkSession: SparkSession )={
    val delName = List("箭头", "广告", "媒体","对联","加减","通栏","按钮","劫持","终极")
    val adsCode_Name: RDD[(String, String)] = getAdsCode_Name(sparkSession.sqlContext)
    val adsCode= adsCode_Name.filter(f => {
      val adsName = f._2
      delName.exists(e => adsName.contains(e))
    }).map(m=>m._1)

     adsCode
  }

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("sparkSQL").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()

    val adsCodeName: RDD[String] = getAdsCode_Name(sparkSession.sqlContext).map(x => x._1 + "," + x._2)
    adsCodeName.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/adsCodeName")

    val adsCode2= getDelAdsCode4(sparkSession)
    adsCode2.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/delCode2")

  }
}

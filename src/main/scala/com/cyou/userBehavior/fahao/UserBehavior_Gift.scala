package com.cyou.userBehavior.fahao

import java.io.File
import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * Created by Administrator on 2017/6/14.
  * 记录登录用户在发号平台访问和动作（领号、预定、参与活动）；
  * （uid、gamecode、动作1、动作2、动作3、time）
  */

object UserBehavior_Gift {
  case class UserLog(behaviorTime: String,  user_id: String,  clean_url: String)
  val confPath = ConfigFactory.load().getString("subApp.userGameConf")
  val appConfig: Config = ConfigFactory.parseFile(new File(s"$confPath"))
  val GiftGamePattern = """^http://hao.17173.com/sche-info-(\d+).html""".r
  val GiftInfoPattern = """^http://hao.17173.com/gift-info-(\d+).html""".r

  def getCleanURL(url: String):String = {
    try {
      val urlObj = new URL(url)
      urlObj.getProtocol() + "://" +urlObj.getHost + urlObj.getPath
    } catch {
      case ex: java.net.MalformedURLException => url
    }
  }


  /**
    * 用户访问--礼包行为日志
    * 输入= 流量日志 + 礼包字典数据（key=giftID,value=GameCode）
    * 流量日志中过滤出登入用户的（访问时间,日期，用户id,url）
    * 通过正则表达式过滤出符合发号相关的url，url中获取giftid
    * giftid通过礼包字典数据匹配到gamecode
    * 添加一个字段：行为，值=1，代表访问动作
    * 最后得到（访问时间,日期，用户id ，gameCode，behavior）
    * partitionBy("behavior", "日期")
    * @param sparkSession
    * @param curDate
    */
  def giftTrafficLog( sparkSession: SparkSession,  curDate: Int): Unit = {

    import sparkSession.implicits._
    val inputFile = appConfig.getString("app.input.traffic") + s"/out-${curDate}??.log.gz" //  /hdfs/logs/traffic/out-${curDate}??.log.gz   hdfs:///logs/traffic/out-20170614??.log.gz   count=374759
    val input=sparkSession.read.textFile(inputFile).rdd                                     //  读取 流量日志
    val allHao= appConfig.getString("app.input.all_hao")                                      // file:///mfs/home/qingchuanzheng/work/all_hao.csv
    val outputFile = appConfig.getString("app.output.userBehavior_gift")                    //  hdfs:///logs/userBehavior/gift


    //抽取登入用户的(behaviorTime, user_id,  clean_url)
    val userDF = input.map(_.split("\t").toList).filter(_(33) != "0").map {//0=未登入，username|1=半登入，uid|2=登入   count=10113
      u => UserLog(
            u(28).substring(0,u(28).lastIndexOf("+")).split("T").mkString(" "), //2017-06-14T01:38:11+08:00 ==>2017-06-14 01:38:11
            if (u(33).contains("|")) { u(33).split("\\|")(0) } else u(33),
            getCleanURL(u(1))
        )
    }.toDF


    //读取 gift-game的字典表
    val giftGameMap: Map[String, String] = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(allHao).select("gift_id", "game_code").map(r => (r(0).toString, r(1).toString)).collect().toMap
    //url 匹配正则表达式后 过滤出访问发号平台的url，根据url中的gift得到gameCode
    val getGameCode: (String => String) = (arg: String) => {
      arg match {
        case GiftGamePattern(gameCode) => gameCode.toString
        case GiftInfoPattern(giftCode)=> giftGameMap.get(giftCode.toString).getOrElse("-1")
        case _ => "-2"
      }
    }
    val sqlfunc = udf(getGameCode)

//    (behaviorTime, user_id, game_code , behavior,date)  count=370
    val resDf: DataFrame = userDF
      .withColumn("game_code", sqlfunc($"clean_url"))
      .withColumn("behavior", lit(1))
      .withColumn("date", lit(curDate))
      .filter($"game_code" =!= "-2" && $"game_code" =!= "-1")
      .drop("clean_url")


    resDf.coalesce(1)
      .write.format("csv")
      .partitionBy("date")
      .option("header", "True")
      .mode(SaveMode.Append) //这里只能用Append能override，因为后面还有行为日志也是追加到这个时间分区的
      .save(outputFile)//    /logs/userBehavior/gift
  }


  /**
    * 用户预订，领取和活动---礼包行为日志
    * 输入=每天通过curl下载的用户礼包行为日志
    * 包含字段（访问时间,用户id ，gameCode，behavior中文）
    * 添加日期字段，behavior中文转换成数字="领号" -> 2, "预定" -> 3, "活动" -> 4
    * partitionBy("behavior", "日期")
    * @param sparkSession
    * @param curDate
    */
  def giftBehaviorLog( sparkSession: SparkSession,  curDate: Int): Unit = {

    //礼包行为日志
    val inputFile = appConfig.getString("app.input.giftBehavior") + s"/${curDate}.csv"  //   hdfs:///logs/gift/behaviors/20170614.csv
    val outputFile = appConfig.getString("app.output.userBehavior_gift")                //   hdfs:///logs/userBehavior/gift

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.DataTypes.StringType

    val giftBehav = sparkSession.read.format("csv")
      .option("header", "true").load(inputFile)


    if (curDate < 20170301) {
      giftBehav.toDF(
        "user_id", "game_code", "create_at", "log_type", "game_type",
        "gift_id", "gift_type")
      .createOrReplaceTempView("gift_behaviors")
//      用户ID,游戏ID,日志时间,日志类型,游戏类型,礼包ID,礼包类型
//      83509141,10613,2017-02-01 00:00:00,领号,网游,36771,新手卡
    } else {
      giftBehav.createOrReplaceTempView("gift_behaviors")
//      user_id,game_code,create_at,log_type,log_plat,gift_id,cookie,session_id,session_views,random_str
//      114946020,4051919,2017-06-14 00:00:07,领号,发号中心,38802,1496996302657723,149699630265772314973705936184181497368944483,31,1497371017107198
    }

    val typeMap = Map("用户访问" -> 1, "领号" -> 2, "预定" -> 3, "活动" -> 4)
    sparkSession.udf.register("get_log_type_id", (arg: String) => typeMap(arg))

    val resDf=sparkSession.sql("""
      SELECT
        CAST(user_id AS STRING) user_id,  game_code,
        get_log_type_id(log_type) behavior, create_at  behaviorTime
      FROM gift_behaviors
      """).withColumn("date", lit(curDate))

    resDf.coalesce(1)
      .write.format("csv").partitionBy("date")
      .option("header", "True")
      .mode(SaveMode.Append)
      .save(outputFile)



  }
  def main(args: Array[String]): Unit = {
    val curDate = args(0).toInt
    val sparkSession: SparkSession = SparkSession.builder.appName("UserBehavior-gift").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()

    giftTrafficLog(sparkSession,curDate)
    giftBehaviorLog(sparkSession,curDate)

  }
}

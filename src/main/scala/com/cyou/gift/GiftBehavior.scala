/*
 * GiftBehavior.scala
 * Copyright (C) 2017 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */

package com.cyou.gift

import org.apache.spark.sql._

/*
 *
 * root
 *  |-- user_id: string (nullable = true)
 *  |-- game_code: string (nullable = true)
 *  |-- log_plat: string (nullable = true)
 *  |-- gift_id: string (nullable = true)
 *  |-- cookie: string (nullable = true)
 *  |-- session_id: string (nullable = true)
 *  |-- random_str: string (nullable = true)
 *  |-- log_type_id: integer (nullable = true)
 *  |-- svn: integer (nullable = true)
 *  |-- client_id: integer (nullable = true)
 *  |-- source: integer (nullable = true)
 *  |-- directory: string (nullable = true)
 *  |-- source_url: string (nullable = true)
 *  |-- visit_url: string (nullable = true)
 *  |-- searchtype: integer (nullable = true)
 *  |-- cur_date: integer (nullable = true)
 */
object GiftBehavior extends SparkTrait {

  /*
   * 礼包行为日志
   *
   */
  def giftBehaviorLog(spark: SparkSession, curDate: Int): Unit = {
    // 礼包行为
    val giftLogPath = appConfig.getString("origin-log.gift-behavior") + s"/${curDate}.csv"// /hdfs/logs/gift/behaviors
    val trafficLogPath = appConfig.getString("origin-log.traffic") + s"/dt=${curDate}??/part-*.parquet"  // /hdfs/logs/logformat/traffic
    val outputFile = appConfig.getString("parquetpath")   //  /hdfs/logs/gift/giftAnalytics/

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.DataTypes.IntegerType
    import spark.implicits._

    val typeMap = Map("领号" -> 2, "预定" -> 3, "活动" -> 4)
    val getLogTypeId = udf((arg: String) => {
      val res=arg match {
        case null=>0
        case _=>typeMap(arg)
      }
      res
    })

    // 预订，领取和活动行为
    val giftBehav = spark.read
        .format("csv")
        .option("header", "true")
        .load(giftLogPath)
        .withColumn("log_type_id", getLogTypeId('log_type))
        .withColumn("svn", 'session_views.cast(IntegerType))
        .drop("log_type", "create_at", "session_views")
    giftBehav.createOrReplaceTempView("gift_behaviors")

    // 流量日志
    val traffic = spark.read.load(trafficLogPath)
      .repartition(20)
      .filter('domain.like("hao.17173.com%"))
    traffic.createOrReplaceTempView("traffic")

    spark.sql("""
      SELECT a.*, b.cliType client_id,
        b.referType source, b.referDomain directory, b.referUrl source_url,
        b.url visit_url, b.searchtype
      FROM gift_behaviors a LEFT JOIN traffic b
      ON a.session_id = b.ssid and a.svn = b.svn""")
      .coalesce(1)
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(outputFile+"/cur_date="+curDate+"/")
  }
	
  def main(args: Array[String]) {
    val curDate = args(0).toInt
    val sqlc = spark
    giftBehaviorLog(sqlc, curDate)
  }
}


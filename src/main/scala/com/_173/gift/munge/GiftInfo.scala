/*
 * GiftInfo.scala
 * Copyright (C) 2016 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */
package com._173.gift.munge

import com.typesafe.config.ConfigFactory

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

case class GiftInfo(id: Long, game_code: Int, cur_date: Int)

object GiftInfo {

  val appConfig = ConfigFactory.load().getConfig("app")

  def giftInfo(sc: SparkContext, sqlContext: SQLContext, curDate: Int): Unit = {
    // 20161030 之前是全量的礼包信息接口
    val curDateM = if (curDate > 20161030) curDate else 20161030
    val inputFile = appConfig.getString("origin-log.gift") + s"/${curDateM}.csv"
    val outputFile = appConfig.getString("user-tag.gift")

    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val giftInfo = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(inputFile).toDF("gift_id", "game_code", "created_at")

    giftInfo.registerTempTable("gift_info")

    sqlContext.sql("""
      SELECT CAST (gift_id AS long) id, game_code FROM gift_info
      """)
      .withColumn("cur_date", lit(curDate))
      .coalesce(1)
      .write.format("parquet").partitionBy("cur_date")
      .mode(SaveMode.Append)
      .save(outputFile)
  }

	def main(args: Array[String]) {
    val curDate = args(0).toInt
    val conf = new SparkConf().setAppName("user-tags")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    giftInfo(sc, sqlContext, curDate)
    sc.stop()
  }
}


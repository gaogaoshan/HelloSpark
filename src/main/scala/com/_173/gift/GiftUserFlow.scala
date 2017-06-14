/*
 * GiftUserFlow.scala
 * Copyright (C) 2017 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */

package com._173.gift

import com.typesafe.config.ConfigFactory

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object GiftUserFlow extends SparkTrait {

  /**   * 分析行为
   */
  def giftUserFlow(spark: SparkSession, curDate: Int): Unit = {
    val inputFile = appConfig.getString("parquetpath")+"/cur_date="+curDate+"/"
    val outputFile = appConfig.getString("csvpath")     //  /hdfs/logs/gp_result/giftAnalytics/

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val getHaoSource = udf((arg: Int, directory: String) => arg match {
      case 3 => directory match {
        case "hao.17173.com" => 0 // 从hao.17173.com流转过来的用户
        case _ => 3
      }
      case _ => arg
    })

    val userBehav = spark.read.load(inputFile)
      .repartition(20)
			.withColumn("hao_source",
        when(
          'source.isNotNull,
          getHaoSource('source, 'directory)
        ).otherwise(lit(100))  // 第三方接口（包括预订）
      )
			.drop("source")
      .withColumnRenamed("hao_source", "source")
			.withColumn("release", when('log_type_id===2, lit(1)).otherwise(lit(0)))
			.withColumn("participation", when('log_type_id===4, lit(1)).otherwise(lit(0)))
			.withColumn("reserve", when('log_type_id===3, lit(1)).otherwise(lit(0)))
      .na.fill("666",Seq("client_id", "directory", "source_url", "visit_url"))

    userBehav.createOrReplaceTempView("user_behaviors")
    spark.sql("""
      SELECT game_code, gift_id, client_id, source, directory, source_url, visit_url, 
        sum(release) release, sum(participation) participation, sum(reserve) reserve,
        count(1) pv, count(distinct(cookie)) uv
      FROM user_behaviors
      GROUP BY game_code, gift_id, client_id, source, directory, source_url, visit_url 
      GROUPING SETS(
        (game_code, gift_id, source),
        (game_code, gift_id, client_id, source),
        (game_code, gift_id, source, directory),
        (game_code, gift_id, client_id, source, directory),
        (game_code, gift_id, source, directory, source_url),
        (game_code, gift_id, client_id, source, directory, source_url),
        (game_code, gift_id, visit_url),
        (game_code, gift_id, client_id, visit_url)

      )
      ORDER BY gift_id, client_id
      """).na.fill("-1", Seq("game_code", "gift_id", "directory", "source_url", "visit_url")).na.fill(-1, Seq("client_id", "source"))
      .withColumn("d_date", lit(curDate))
      .coalesce(1)
      .write.format("csv")
      .option("header", "True")
      .mode(SaveMode.Overwrite)
      .save(outputFile+"cur_date="+curDate+"/")
  }

  def main(args: Array[String]) {
    val curDate = args(0).toInt
    val sqlc = spark
    giftUserFlow(sqlc, curDate)
  }
}


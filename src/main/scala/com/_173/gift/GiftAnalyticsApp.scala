/*
 * GiftAnalyticsApp.scala
 * Copyright (C) 2017 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */

package com._173.gift

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkTrait {
  val appConfig = ConfigFactory.load().getConfig("app")

  def spark: SparkSession = SparkSession
    .builder()
    .master(appConfig.getString("master"))
    .appName(this.getClass.getSimpleName)
    .getOrCreate()
}

object GiftAnalyticsApp extends SparkTrait {
  def main(args: Array[String]) {
//    val curDate = args(0).toInt
//    val sqlc = spark
//    GiftBehavior.giftBehaviorLog(sqlc, curDate)
//    GiftUserFlow.giftUserFlow(sqlc, curDate)

    val giftLogPath = appConfig.getString("origin-log.gift-behavior") ;
    print(giftLogPath)
  }
}


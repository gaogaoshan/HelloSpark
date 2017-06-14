package com._173.gift


object GiftAnalyticsApp extends SparkTrait {
  def main(args: Array[String]) {
        val curDate = args(0).toInt
        val sqlc = spark
        GiftBehavior.giftBehaviorLog(sqlc, curDate)
        GiftUserFlow.giftUserFlow(sqlc, curDate)

        val giftLogPath = appConfig.getString("origin-log.gift-behavior") ;
        print(giftLogPath)
  }
}


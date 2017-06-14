package com.cyou.gift
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

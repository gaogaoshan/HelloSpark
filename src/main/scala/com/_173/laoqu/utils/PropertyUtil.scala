package com._173.laoqu.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}


/**
  * Created by Administrator on 2017/5/24.
  */
object PropertyUtil {

  def getFromPro(): Unit ={
    val confPath = ConfigFactory.load().getString("fileConf.dir")
    println(confPath)
  }

  def main(args: Array[String]): Unit = {
    val confPath = ConfigFactory.load().getString("fileConf.dir")
    val dbPro: Config = ConfigFactory.parseFile(new File(s"$confPath/DB.conf"))
    val a=dbPro.getString("db.oracle_154.url")
    println(a)

  }
}

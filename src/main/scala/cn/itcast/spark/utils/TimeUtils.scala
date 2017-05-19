package cn.itcast.spark.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by root on 2016/5/23.
  */
object TimeUtil extends Serializable{

  def getWeekFromData(ddate:String): String ={
    val calendar = Calendar.getInstance()
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.setTime(simpleDateFormat.parse(ddate))

    val WEEK_OF_YEAR=calendar.get(Calendar.WEEK_OF_YEAR)
    val YEAR=ddate.take(4)

    YEAR+"_"+WEEK_OF_YEAR
  }

  def main(args: Array[String]): Unit = {
    val ddate1="20170305"
    println(getWeekFromData(ddate1))
    val ddate1_1="20170311"
    println(getWeekFromData(ddate1_1))

    val ddate2="20170429"
    println(getWeekFromData(ddate2))
    val ddate2_1="20170423"
    println(getWeekFromData(ddate2_1))
  }
}

package cn.itcast.spark.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by root on 2016/5/23.
  */
object TimeUtils {

//  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//  val calendar = Calendar.getInstance()
//  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

//  def filterByTime(fields: Array[String], startTime: Long, endTime: Long): Boolean = {
//    val time = fields(1)
//    val logTime = dateFormat.parse(time).getTime
//    logTime >= startTime && logTime < endTime
//
//    val d: Date = dateFormat.parse(time)
//    d.getDate
//
//  }


//  def getCertainDayTime(amount: Int): Long ={
//    calendar.add(Calendar.DATE, amount)
//    val time = calendar.getTimeInMillis
//    calendar.add(Calendar.DATE, -amount)
//    time
//  }

  def getWeekFromData(ddate:String): Int ={
    val calendar = Calendar.getInstance()
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.setTime(simpleDateFormat.parse(ddate))

    calendar.get(Calendar.WEEK_OF_YEAR)
  }


  def main(args: Array[String]): Unit = {
    val ddate="20170124"
    println(getWeekFromData(ddate))



  }


}

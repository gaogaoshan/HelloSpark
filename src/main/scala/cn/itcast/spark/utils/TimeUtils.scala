package cn.itcast.spark.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ListBuffer

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

  def getWeekList(formWeek:String,toWeek:String):ListBuffer[String] ={
    var weekSet=ListBuffer[String]()
    val calendar = Calendar.getInstance()
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.setTime(simpleDateFormat.parse(formWeek))
    val FROM_WEEK_OF_YEAR1=calendar.get(Calendar.WEEK_OF_YEAR)
    calendar.setTime(simpleDateFormat.parse(toWeek))
    val TO_WEEK_OF_YEAR=calendar.get(Calendar.WEEK_OF_YEAR)

    val From_YEAR=formWeek.take(4)
    println("from="+FROM_WEEK_OF_YEAR1+" to="+TO_WEEK_OF_YEAR)

    for(i<- FROM_WEEK_OF_YEAR1 to TO_WEEK_OF_YEAR){
      val year_week=From_YEAR+"_"+i
      weekSet+=year_week
    }
    weekSet
  }

  def getWeekBetweenYear(formWeek:String,toWeek:String):ListBuffer[String] ={
    val From_YEAR=formWeek.take(4)
    val To_YEAR=toWeek.take(4)

      val fromWeekList: ListBuffer[String] =getWeekList(formWeek,From_YEAR+"1231")
      val toWeekList: ListBuffer[String] =getWeekList(To_YEAR+"0101",toWeek)

      fromWeekList++=toWeekList
  }

  def main(args: Array[String]): Unit = {
    val ddate1="20170305"
    println(getWeekFromData(ddate1))
//    val weekList: ListBuffer[String] = getWeekList("20170507","20170520")
    val weekList: ListBuffer[String] =TimeUtil.getWeekList("20170101","20170114")
    println(weekList.toBuffer)
//    2016年7月4日-2017年4月30日
    }
}

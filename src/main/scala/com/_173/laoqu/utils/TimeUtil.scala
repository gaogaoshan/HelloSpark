package com._173.laoqu.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2016/5/23.
  */
object TimeUtil extends Serializable{

  def getWeekFromData(ddate:String,formatString:String): String ={
    val calendar = Calendar.getInstance()
    val simpleDateFormat = new SimpleDateFormat(formatString)////yyyyMMdd yyyyMMddHH
    calendar.setTime(simpleDateFormat.parse(ddate))

    val WEEK_OF_YEAR=calendar.get(Calendar.WEEK_OF_YEAR).toString
    val YEAR=ddate.take(4)

    YEAR+WEEK_OF_YEAR
  }

  def getWeekList(formWeek:String,toWeek:String,formatString:String):ListBuffer[Int] ={
    var weekSet=ListBuffer[Int]()
    val calendar = Calendar.getInstance()
    val simpleDateFormat = new SimpleDateFormat(formatString)//yyyyMMdd
    calendar.setTime(simpleDateFormat.parse(formWeek))
    val FROM_WEEK_OF_YEAR1=calendar.get(Calendar.WEEK_OF_YEAR)
    calendar.setTime(simpleDateFormat.parse(toWeek))
    val TO_WEEK_OF_YEAR=calendar.get(Calendar.WEEK_OF_YEAR)

    val From_YEAR=formWeek.take(4)
    println("from="+FROM_WEEK_OF_YEAR1+" to="+TO_WEEK_OF_YEAR)

    for(i<- FROM_WEEK_OF_YEAR1 to TO_WEEK_OF_YEAR){
      val year_week=(From_YEAR+i).toInt
      weekSet+=year_week
    }
    weekSet
  }

  def getWeekBetweenYear(formWeek:String,toWeek:String,formatString:String):ListBuffer[Int] ={
    val From_YEAR=formWeek.take(4)
    val To_YEAR=toWeek.take(4)

      val fromWeekList: ListBuffer[Int] =getWeekList(formWeek,From_YEAR+"1231",formatString)//yyyyMMdd
      val toWeekList: ListBuffer[Int] =getWeekList(To_YEAR+"0101",toWeek,formatString)

      fromWeekList++=toWeekList
  }

  def main(args: Array[String]): Unit = {
    val ddate1="2017030521"
    println(getWeekFromData(ddate1,"yyyyMMddHH"))
//    val weekList: ListBuffer[String] = getWeekList("20170507","20170520")
    val weekList: ListBuffer[Int] =TimeUtil.getWeekList("2017010100","2017011423","yyyyMMddHH")
    println(weekList.toBuffer)
//    2016年7月4日-2017年4月30日
    }
}

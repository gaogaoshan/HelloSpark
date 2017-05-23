package cn.itcast.spark.work3

import cn.itcast.spark.utils.{LogFormat, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.Map
import scala.collection.mutable.{ListBuffer, Set => SSet}

object RetentionRate_Traffic {


  def  getCountString(weekList:ListBuffer[String],fromWeek:String ,countList: List[Int] ): String ={
    var returnString:String =""
    val weekIndex=weekList.indexOf(fromWeek);

    for(i <- 1 to weekIndex){
      returnString+="0,"
    }
    println("weekList="+weekList.toBuffer+" weekIndex="+weekIndex+" returnString="+returnString)
    countList.foreach(returnString+=_+",")
    returnString.substring(0,returnString.lastIndexOf(","))
  }
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("sparkSQL").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()

    // =======================================================================================================================================
    //    统计每周的新用户在未来数周的留存情况，生成用户堆积数据表。
    //
    //    1、时间范围是2016年7月4日-2017年4月30日；
    //    2、统计维度是自然周和DAU；
    //    3、输出数据为周期时间和对应的活跃UV；（参见数据表范例1）
    //    4、分来源输出4个sheet：直接访问、搜索引擎、站内来源、外部链接。


    //1
    //    val weeks=List("20170507","20170508","20170509","20170510","20170511","20170512","20170513")
    //    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate from traffic where refer_host <> '3' and ddate>='20170507'  and  ddate<='20170513'  ").rdd
    val weekList: ListBuffer[String] = TimeUtil.getWeekList("20170101", "20170128","yyyyMMdd")
    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate,cnlid from traffic where refer_host <> '3' and ddate>='20170101'  and  ddate<='20170128'  ").rdd
    val _17173Rdd = selectRdd.filter(f => {
      val cnlIds = f(5).toString.split(",")
      cnlIds.contains("383")
    })

    //2添加来源字段| 格式化时间字段为周|第一周来的用户全部当中新用户   List[(suv,(refer_type,周，isnew))]
    val formatRdd: RDD[(String, (String, String, String))] = _17173Rdd.map(r => {
      val suv = r(0).toString
      var newuv = r(1).toString
      val refer_type = LogFormat.getReferType(r(2).toString, r(3).toString)
      val ddate = TimeUtil.getWeekFromData(r(4).toString,"yyyyMMdd")
      if (ddate == weekList(0)) newuv = "1"

      (suv, (refer_type, ddate, newuv))
    }).distinct() //persist(StorageLevel.DISK_ONLY)  1617493

    //3聚合用户的来访轨迹(suv,List[(refer_type,周，isnew)])
    //4 用户分组内 按照isnew排序 (suv,List[(refer_type,周，isnew)])
    val sortUvGroup: RDD[(String, List[(String, String, String)])] = formatRdd.groupByKey().map(g => {
      val suv = g._1
      val sortList = g._2.toList.sortBy(x => x._3).reverse
      (suv, sortList)
    }).cache()
    println("sortUvGroup has UV size=" + sortUvGroup.count()) //   2week=2674048

    //5 过滤出要对比的那一周的留存率  比如要算第2周新用户  在3,4,5周的留存
    // 排序后的List 取第一个，【如果周==第2周】&&【isnew==1】    是就说明这个用户是第二周来的  suv,List[(refer_type,周，isnew)

    var resSSet: SSet[(String, String, String, Int)] = SSet() //[ref,fromWeek,toWeek,,count)]

    for (x <- 0 until weekList.length) {

      val w = weekList(x)
      val newUvGroup: RDD[(String, List[(String, String, String)])] = sortUvGroup.filter(f => {
        val uvWeek = f._2.head._2
        val uvIsNew = f._2.head._3
        uvWeek == w && uvIsNew == "1"
      })
      println("filter week=" + w + " new UV size=" + newUvGroup.count())

      //(refer_type,周),1
      val refer_week_rdd: RDD[((String, String), Int)] = newUvGroup.flatMap(r => {
        val ref_week: List[((String, String), Int)] = r._2.map(x => {
          ((x._1, x._2), 1)
        }).distinct
        ref_week
      })
      val refer_week_count: RDD[((String, String), Int)] = refer_week_rdd.reduceByKey(_ + _)

      val asMap: Map[(String, String), Int] = refer_week_count.collectAsMap() //(直接来源,2017_19) -> 9565
      for (elem <- asMap) {
        val ref = elem._1._1.toString; val fromWeek = w; val toWeek = elem._1._2; val count = elem._2
        resSSet.add(ref, fromWeek, toWeek, count) //[ref,fromWeek ,toWeek,count]
      }
    }
    //======================================基本完成===有部分数据有问题比如 上一周还是老用户的这一周变成新用户了 要去除============================================================================================

    val delData = resSSet.flatMap(r => {
      val ref = r._1
      val fromWeek = r._2
      val toWeek = r._3
      val count = r._4
      if (fromWeek > toWeek) None
      else Some(ref, fromWeek, toWeek, count)
    })
    val res: List[String] = delData.toList.map(m => (m._1 + "," + m._2 + "," + m._3 + "," + m._4))
    sparkSession.sparkContext.parallelize(res, 1).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/RRate-res")
    //======================================完成==行转列=============================================================================================


    var resList: List[String] = List()
    delData.groupBy(f => f._1).map(r => {
      val fromWeek = r._1
      println("ref=" + fromWeek)
      val from_to_count_Set: SSet[(String, String, Int)] = r._2.map(m => (m._2, m._3, m._4)) //fromWeek,toWeek,count

      val fromGroup: Map[String, SSet[(String, String, Int)]] = from_to_count_Set.groupBy(f => f._1) //[fromWeek, (fromWeek,toWeek,count)]

      val from_countString: List[(String, String)] = fromGroup.map(f => {
        val fromWeek = f._1
        val countList: List[Int] = f._2.map(m => (m._2, m._3)).toList.sortBy(x => x._1).map(x => x._2) //toWeek,count
        val countString = getCountString(weekList, fromWeek, countList)
        (fromWeek, countString)
      }).toList.sortBy(x => x._1)

      val lis: List[String] = from_countString.map(m => fromWeek + "," + m._1 + "," + m._2)
      resList ++= lis
    })

    sparkSession.sparkContext.parallelize(resList, 1).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/RRate-2Week")

  }
}
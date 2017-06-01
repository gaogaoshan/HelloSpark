package com._173.laoqu.work3

import com._173.laoqu.utils.{DBUtil, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalog.Column
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{ListBuffer, Set => SSet}
import scala.collection.{Map, mutable}
import scala.util.control.Breaks._

object Module5 {

  def take5Code(arry: List[(String, String, String,String)]):(String, List[String]) ={
    var _take5AdsCode: List[String] = List.empty
    var ref:String=""

    breakable(
      for (i <-arry.zipWithIndex){

         val url=i._1._2   ; val ads_code=i._1._3 ;val refer=i._1._4  ; //(ts,url,ads_code,refer)
        if(url.equals("http://www.17173.com/") || url.equals("https://www.17173.com/")){

          val index:Int=i._2
          ref=refer
          val codes=for(step <- 0 to 4; if(index + step < arry.length) ) yield arry(index + step)._3

          _take5AdsCode=codes.toList
          break()
        }
      }
    )
    (ref,_take5AdsCode)
  }

  def getNameByCode(codeList:List[String],codeNameMap: Map[String, String] ): List[String]  ={
    var codeNameList: List[String] = List.empty

    codeList.foreach(code=>{
      val name=codeNameMap.getOrElse(code,"codeName")
      codeNameList=codeNameList.:+(name)
    })
    codeNameList
  }

  def getMonthDataString2(month_count: Map[String, Int] ,month:List[String]): String ={
    var monthData: List[String] = List.empty
    month.foreach(m=>{
      val g=month_count.getOrElse(m,"N").toString

      val count=
        g match {
          case "N"=> "0"
          case _  =>g
        }
      monthData=monthData:+count
    })
    monthData.mkString(",")
  }


  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("sparkSQL").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()
//    spark.catalog.listDatabases.show(false) spark.catalog.listTables.show(false)
//    val columns= sparkSession.catalog.listColumns("logapi_113").map(m=>(m.name,m.description)).take(100)

    // =======================================================================================================================================
//    "统计所有用户在首页上的主要访问路径。
//
//    1、区分新（初次访问）老（回访）用户；
//    2、时间范围是2016年7月4日-2017年4月30日；
//    3、统计维度是这些用户访问路径中前5步的模块名称（需排除名称中包含“箭头”“广告”“媒体”“对联”“加减”“通栏”“按钮” "劫持" "终极" 的模块）、顺序和月日均UV量（按日和路径去重）；
//    4、用户访问路径可以是5步以内，当路径种类过多时，取月日均UV有过1千以上的；
//    5、输出数据为：①模块名、②各月日均UV、③总日均UV（以此项由大到小排列）；（参见数据表范例2）
//    6、在新老用户两张表上都分来源输出4个sheet：直接访问、搜索引擎、站内来源、外部链接。"
//
//    范例2
//    模块1	……	模块5	201607日均UV	……	201704日均UV	总日均UV



//1481972675010484  1481878378854328  1487378065731597


//    1.获取访问173首页的用户    moduleRdd.rdd.getNumPartitions

    val month=List("201701","201702","201703","201704","201705")
    val trafficSql="select suv as uv,ddate from traffic where ddate in('20170110','20170210','20170310','20170410','20170510') and domain='www.17173.com'  group by suv, ddate";
    val moduleSql="select uv,ddate,isnew,ts,url,ads_code,ref_type FROM module WHERE ddate in ('20170110','20170210','20170310','20170410','20170510') "

    val _118parquetFile=sparkSession.read.parquet("hdfs:/logs/logapi/118/").createOrReplaceTempView("118_log")//click
    val _118Sql="""select suv as uv,ddate  from 118_log
        where ddate in('20170110','20170210','20170310','20170410','20170510')
        and  ( lower(u) like 'https://cvda.17173.com%'   or  lower(u) like 'http://cvda.17173.com%')
        and (cur_url="http://www.17173.com/" or cur_url="https://www.17173.com/")
        group by suv, ddate """.stripMargin.replaceAll("\n","");


//    val trafficeRdd = sparkSession.sql(trafficSql)
    val moduleRdd=sparkSession.sql(moduleSql)
    val guanggaoRdd=sparkSession.sql(_118Sql)

//    2.流量日志和模块日志关联 得到访问了首页的人 都访问了那些模块  ====（suv，day,isnew )(ts,url,ads_code,refer)  count=day5=9282033
    val joinRdd: RDD[((String, String, String), ( String, String,String, String))] =
          moduleRdd.join(guanggaoRdd, Seq[String]("uv", "ddate")).rdd.map(f => ((f(0).toString, f(1).toString,f(2).toString), ( f(3).toString, f(4).toString, f(5).toString, f(6).toString) )).cache()
//    val joinRdd: RDD[((String, String, String), ( String, String,String, String))] =
//      moduleRdd.join(trafficeRdd, Seq[String]("uv", "ddate")).rdd.map(f => ((f(0).toString, f(1).toString,f(2).toString), ( f(3).toString, f(4).toString, f(5).toString, f(6).toString) )).cache()

//    3 有些模块是不需要进行 统计的
//      val delCode: RDD[String] = DBUtil.getDelAdsCode4(sparkSession)
//     delCode.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/delCode")
    val dCode: Array[String] = sparkSession.read.text("/tmp/hugsh/laoqu/delCode2/").rdd.map(x => x(0).toString).collect()
    val filterRdd= joinRdd.filter(f=>{!dCode.contains(f._2._3)})//count=day5=5244003 filterRdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/tmpModule5-filterRdd_118")

//    4.根据 (suv，day,isnew)3个维度分组  得到====> 这个用户这一天内的访问轨迹 ===>（suv，day,isnew )[(ts,url,ads_code,refer)....]
//    Map==》按时间排序选取访问173首页后的5条记录， 第一个记录的来源就是用户的真实来源    count=day5=799549   oneUser_day_path_group.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/tmpModule5-oneUser_day_path_group")

    val oneUser_day_path_group: RDD[((String, String, String,List[String]), Int)] = filterRdd.groupByKey().map(m => {
      val suv = m._1._1
      val day = m._1._2
      val isnew = m._1._3

      val ref_UrlList: (String, List[String]) = take5Code(m._2.toList.sortBy(f => f._1))
      val ref = ref_UrlList._1
      val codeList = ref_UrlList._2

      ((isnew, ref, day,codeList), 1)
    }).cache()


    //    5.得到====》每天每个路径的统计量===>  去除量小于50的路径 ===》将天转换成月，方便下一步统计月的数据  count=day5=303563  Users_Day_Path_Count.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/tmpModule5-users_Day_Path_Count3")

    val Users_Day_Path_Count= oneUser_day_path_group.reduceByKey(_+_).map(m=>{
      val isnew = m._1._1
      val refer_type =  m._1._2.toString  match {
        case "1"   =>"直接来源"
        case "2"   =>"搜索引擎"
        case "3"   =>"站内来源"
        case "4"   =>"外部来源"
        case _      =>"N"
      }
      val day = m._1._3       ;val month=day.take(6)
      val codeList= m._1._4
      val count =m._2

      ((isnew,refer_type,codeList,month),count)
    }).cache()//.filter(x=>x._2<5).cache()
//    val t_countDat=Users_Day_Path_Count.sortBy(x=>x._2,false).cache()


//    6.得到每个月 路径的统计===>统计路径在每个月的数量按照月份排序  path_MonthCount.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/tmpModule5-path_MonthCount")
    val codeNameMap: Map[String, String] = sparkSession.read.text("/tmp/hugsh/laoqu/adsCodeName2").rdd.map(x => {
      val arr=x(0).toString().split(",")
      (arr(0),arr(1))
    }).collectAsMap()

    val path_MonthCount: RDD[((String, String, List[String]), Iterable[(String, Int)])] = Users_Day_Path_Count.reduceByKey(_ + _).map(m => {
      val isnew = m._1._1
      val refer_type = m._1._2
      val codeList = m._1._3  ;val nameList=getNameByCode(codeList,codeNameMap)
      val month = m._1._4
      val count = m._2

      ((isnew, refer_type, nameList), (month, count))
    }).groupByKey().cache()
//    val bigthan2Month=path_MonthCount.filter(x=>x._2.size>4).cache()


    val path_Count6 = path_MonthCount.map(m => {
      val isnew = m._1._1 match {
        case "1"   =>"newUV"
        case "0"   =>"oldUV"
        case _      =>"N"
      }
      val refer_type = m._1._2
      val nameList = m._1._3.mkString("|")
      val month_count: Map[String, Int] = m._2.toList.sortBy(x => x._1).toMap
      val countString = getMonthDataString2(month_count,month)
      val monthSum: Int = countString.split(",").map(x=>x.toInt).sum

      (isnew, refer_type, nameList, countString ,monthSum)

    }).sortBy(x=>x._5,false)
      //.filter(x=>x._5>100)
      .map(f=>{f._1+","+ f._2+","+ f._3+","+ f._4+","+f._5})


    path_Count6.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/tmpModule5-pathString_118")





  }
}
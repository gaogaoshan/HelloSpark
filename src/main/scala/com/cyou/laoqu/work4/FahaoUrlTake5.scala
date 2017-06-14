package com.cyou.laoqu.work4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by Administrator on 2017/4/24.
  */
object FahaoUrlTake5 {
  val urlTypes=Array[String](
    "http://hao.17173.com/gift-info-38405.html",
    "http://hao.17173.com/gift-info-38406.html",
    "http://hao.17173.com/gift-info-38552.html",
    "http://hao.17173.com/gift-info-38454.html",
    "http://hao.17173.com/gift-info-38392.html",
    "http://hao.17173.com/gift-info-38420.html")

  val url_name_Map=Map(
    "http://hao.17173.com/gift-info-38405.html"->"全平台通用初级礼包",
    "http://hao.17173.com/gift-info-38406.html"->"全平台通用高级礼包",
    "http://hao.17173.com/gift-info-38552.html"->"升级版特权礼包    ",
    "http://hao.17173.com/gift-info-38454.html"->"独家限量特权礼包  ",
    "http://hao.17173.com/gift-info-38392.html"->"全平台通用独家礼包",
    "http://hao.17173.com/gift-info-38420.html"->"独家高级宝石礼包  "
  )

  def take_5Urls(urls: List[String]):(String, List[String]) ={
    var _take5url: List[String] = List.empty
    var urlType:String=""

    breakable(
      for (i <-urls.zipWithIndex){
        val url: String = i._1
        if(urlTypes.exists(url.contains(_))){

          urlType=url
          val index:Int=i._2

          val list=for(step <- 1 to 5; if(index + step < urls.length) ) yield urls(index + step)
          _take5url=list.toList
          break()
        }
      }
    )
    (urlType,_take5url)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("sparkSQL").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()

// =======================================================================================================================================
    //读取数据文件 /hdfs/logs/logformat/traffic/dt=2017042400 ||  hdfs:/logs/logformat/traffic/dt=20170424* ||   C:/testData/traffic/2017042400/
    val parquetFile=sparkSession.read.parquet("hdfs:/logs/logformat/traffic").registerTempTable("traffic_log")
    val allDf=sparkSession.sql("select suv,dt,addtime,url from traffic_log where dt> '2017051700' and dt<'2017060100' ")//
    val uvDf = allDf.filter(allDf("url").isin(urlTypes:_*) ).select("suv").distinct().cache()//count=27124

    //访问过这些url的用户 的用户轨迹   723200
    val visitRdd: RDD[((String, String), (String, String))] = allDf.join(uvDf, allDf("suv") === uvDf("suv")).rdd.map(row => {
      ((row(0).toString, row(1).toString), (row(2).toString, row(3).toString))
    })
    //根据（suv,dt）分组， 得到用户点击轨迹(addtime,url,,)
    val uv_dt_urls_Rdd: RDD[((String, String), Iterable[(String, String)])] = visitRdd.groupByKey().cache()//（suv,dt）,[(addtime,url,)....]  83405

    //上面根据用户分组后，组内时间排序，只要url===取前5（urlType,urlList点击序号），==>RDD[每个用户的访问List]---List里面是(type,index,url),1     === 120791
    val type_index_url_1_Rdd: RDD[((String, Int, String), Int)]= uv_dt_urls_Rdd.flatMap(sidLine => {
      val urlList: List[String] = sidLine._2.toList.sortBy(x => x._1).map(x => x._2)
      val type_Urls: (String, List[String]) = take_5Urls(urlList)

      val urlType = type_Urls._1
      val type_index_url_1: List[((String, Int, String), Int)] = type_Urls._2.zipWithIndex.map(x => {
        val index = x._2
        val url = x._1
        ((urlType, index, url), 1)
      })

      type_index_url_1
    }).cache()



    //累加排序 访问小于3的去掉  ==5350
    val type_index_url_Count_Rdd: RDD[(String, Int, String, Int)] = type_index_url_1_Rdd.reduceByKey(_+_).map(x=>{
      val uslType=url_name_Map.getOrElse(x._1._1,"NN")
      val index=x._1._2
      val url=x._1._3
      val count=x._2

      (uslType,index,url,count)
    }).sortBy(x=>x._4,false).filter(x=>x._4>=2).cache()



    type_index_url_Count_Rdd.filter(x=>x._1!="NN").map(x=>{
      x._1+","+x._2+","+x._3+","+x._4
    }).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/urlTypes_Take5-all")//  /hdfs/tmp/hugsh/laoqu*/

    allDf.rdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/urlTypesAll_0430")
  }

}

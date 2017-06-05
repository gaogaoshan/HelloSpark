package com._173.laoqu.work4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.control.Breaks._

/**
  * Created by Administrator on 2017/4/24.
  */
object NeirongUrlTake5_2 {
  val urlTypes=Array[String](
    "http://news.17173.com/content/03102017/140330881_1.shtml",
    "http://news.17173.com/content/03152017/114541392_1.shtml",
    "http://news.17173.com/content/03172017/100045881_1.shtml",
    "http://news.17173.com/content/03232017/104059611.shtml",
    "http://news.17173.com/content/03282017/100324132.shtml",
    "http://news.17173.com/content/04102017/104945547.shtml",
    "http://news.17173.com/content/04142017/100632515.shtml",
    "http://news.17173.com/content/04262017/102310296.shtml",
    "http://news.17173.com/zt/201704271631/index.shtml",
    "http://news.17173.com/content/04282017/092038222.shtml",
    "http://news.17173.com/content/04292017/083836186_1.shtml",
    "http://news.17173.com/content/04302017/002327112_1.shtml"
   ")

  val url_name_Map=Map(
    "http://news.17173.com/content/03102017/140330881_1.shtml"->  "《天龙八部手游》限量测试已火爆开启",
    "http://news.17173.com/content/03152017/114541392_1.shtml"->"赵天师回归《天龙八部手游》3DMMO武侠手游",
    "http://news.17173.com/content/03172017/100045881_1.shtml"->"《天龙八部手游》不删档预约火爆开启",
    "http://news.17173.com/content/03232017/104059611.shtml"->"《天龙八部手游》心悦不删档预约惊喜来袭",
    "http://news.17173.com/content/03282017/100324132.shtml"->"《天龙八部手游》不删档预约重塑经典共战江湖",
    "http://news.17173.com/content/04102017/104945547.shtml"->"《天龙八部手游》“赵天师的信”活动引爆情怀",
    "http://news.17173.com/content/04142017/100632515.shtml"->"17173第139个手游专区《天龙八部手游》上线",
    "http://news.17173.com/content/04262017/102310296.shtml"->"《天龙八部手游》典当行上新品承载的师徒情",
    "http://news.17173.com/zt/201704271631/index.shtml"->"定制专题：定制专题：IP及游戏预约引导",
    "http://news.17173.com/content/04282017/092038222.shtml"->"再续情缘共战江湖天龙八部手游5.16不删档测试",
    "http://news.17173.com/content/04292017/083836186_1.shtml"->"有多少难念的经?腾讯天龙八部手游任务系统解读",
    "http://news.17173.com/content/04302017/002327112_1.shtml"->"女侠每月手游推荐"
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
    val allDf=sparkSession.sql("select suv,dt,addtime,url from traffic_log where dt> '2017031000' and dt<'2017060100' ")//
    val uvDf = allDf.filter(allDf("url").isin(urlTypes:_*) ).select("suv").distinct()

    //访问过这些url的用户 的用户轨迹   723200
    val visitRdd: RDD[((String, String), (String, String))] = allDf.join(uvDf, allDf("suv") === uvDf("suv")).rdd.map(row => {
      ((row(0).toString, row(1).toString), (row(2).toString, row(3).toString))
    })
    //根据（suv,dt）分组， 得到用户点击轨迹(addtime,url,,)
    val uv_dt_urls_Rdd: RDD[((String, String), Iterable[(String, String)])] = visitRdd.groupByKey()//（suv,dt）,[(addtime,url,)....]  83405

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
    }).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/typeUrl_neirong")
    //  /hdfs/tmp/hugsh/laoqu*/

  }

}

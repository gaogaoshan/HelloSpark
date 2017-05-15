package cn.itcast.spark.work2

import cn.itcast.spark.utils.DBUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.mutable.{Map => MMap, Set => SSet}
import scala.util.control.Breaks._


object Sid_Module_Urls {


  /**
    *
    * @param arr  List[(url,ts)]
    * @param ts_113
    * @return 如果时间相等 返回（current && next_4_url）
    */
  def takeUrls(arr: List[(Any, String)] ,ts_113:String):List[String] ={
    var _takeUrls: List[String] = List.empty

    breakable(
      for (i <-arr.zipWithIndex){
        val url=i._1._1  ;val ts_118: String = i._1._2

        if(ts_113==ts_118){

          val index:Int=i._2
          val tmp=for(step <- 0 to 4; if(index + step < arr.length) ) yield arr(index + step)._1.toString

          _takeUrls=tmp.toList
          break()
        }
      }
    )

    _takeUrls
  }


  def getStringFromList(l:List[String]): String ={
    var s:String=""
    l.foreach(x=>{
      s=s.concat(x).concat(",")
    })
    s
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQLDemo").setMaster("local[2]")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

// =======================================================================================================================================
    //读取数据文件 module=hdfs:/logs/logapi/113/ddate=20170424 ||   logapi_113= hdfs:/logs/logformat/113/dt=20170424* ||   /hdfs/logs/logapi/113/dt=20170424
    val _113parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170307*").createOrReplaceTempView("113_log")//module
    val _118parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/118/*20170307*").createOrReplaceTempView("118_log")//click

//    捞取2017年3月7日，newgame首页所有用户的会话情况
//    用户1	会话ID	模块名	URL1	URL2	URL3	URL4	URL5

// 1.拿到113模块，newGame首页的所有模块的点击事件 确定(会话ID+模块名+min时间)
    //module=   select uv ,ssid , ads_code  , min(ts)  from 113_log where url="http://newgame.17173.com/"  group by (uv,ssid,ads_code)
//2.拿到118日志，按ssid分组，得到用户当天的点击轨迹
    //click   select ssid,cur_url,u,ts from 118_log group by ssid order by ts
//1.2 join
//     (113.ssid=118.ssid) ===> ssid,(  (ads_code,click_url),     List(u,ts)  )
//      吧 click_url，List(u,ts)  关联click_url=u 然后去后3个 就得到  ssid,ads_code, [url1,url2,url3]







    //1.拿到113模块，newGame首页的所有模块的点击事件 确定(SSID+模块名+时间)  ----后面用ssid join 118 再用113的时间 找到113时间后的4个url
    val _113Sql=""" select uv ,ssid , ads_code  , min(ts)  from 113_log
             where url="http://newgame.17173.com/"
             group by uv,ssid,ads_code """.stripMargin.replaceAll("\n","");
    val sid_AdsCode_Set: RDD[(String, (Any, String, String))] = sqlContext.sql(_113Sql).rdd.map(r => {
      val uv = r(0)
      val ssid = r(1).toString
      val ads_code = r(2).toString
      val ts = r(3)
      (ssid, (uv, ads_code, ts.toString))
    })


   //2.拿到118日志，按ssid分组，得到用户当天的点击轨迹(ssid,[(u,ts),(u,ts),...])
    val _118_data=sqlContext.sql("select ssid,u,ts  from 118_log  ").rdd

    val _118_Sid_Urls: RDD[(String,List[(String, String)])] = _118_data.groupBy(r => r(0)).map(r => {
      val ssid: String = r._1.toString
      val clickUrl_ts: List[(String, String)] = r._2.map(x => (x(1).toString, x(2).toString)).toList.sortBy(x => x._2)

      (ssid, clickUrl_ts)
    })

    //1.2 join
      //sid_AdsCode_Set.partitions.size//200
      //_118_Sid_Urls.partitions.size//17
  //(ssid ,( (uv, ads_code, ts),List[(u,ts),(u,ts),..]  ))
    val joinRdd: RDD[(String, ((Any, String, String), List[(String, String)]))] = sid_AdsCode_Set.join(_118_Sid_Urls)


    //翻译模块名称
    val adsCodeName: Map[String, String] = DBUtil.getAdsCode_Name(sqlContext).collectAsMap()
    val adsCodeNameBC = sc.broadcast(adsCodeName).value

    val resultRdd: RDD[String] = joinRdd.map(x => {
      val ssid = x._1
      val uv = x._2._1._1
      val ads_code = x._2._1._2.toString
      val ts_113: String = x._2._1._3
      val url_ts_List: List[(Any, String)] = x._2._2

      val urls: List[String] = takeUrls(url_ts_List, ts_113)
      val url_String: String = getStringFromList(urls)
      val ads_name = adsCodeNameBC.getOrElse(ads_code, ads_code)

      uv + "," + ssid + "," + ads_name + "," + url_String
    })
//
    resultRdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/sid_mod_urls")



//    val select113=sqlContext.sql("select uv,ssid,ads_code,url,click_url,ts from 113_log   order by ts")
//    select113.rdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113_all_3_7")
//
//    val select118=sqlContext.sql("select ssid,cur_url,u,ts from 118_log  order by ts")
//    select118.rdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/118_all_3_7")
  }



}

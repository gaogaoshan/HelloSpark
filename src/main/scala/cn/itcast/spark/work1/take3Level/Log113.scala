package cn.itcast.spark.work1.take3Level

import cn.itcast.spark.utils.{ArrUtil, DBUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by Administrator on 2017/4/24.
  */
object Log113 {

  /**
    * 统计从17173到yeyou后的三次点击
    * 根据（URL维度）和（域名维度）分别统计出到yeyou后（第1次）（第2次）（第3次） 点击的页面访问次数
    * 模块表在/logs/logapi/113/ (简化) 或者hue上面的logapi_113(/logs/logformat/113)
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQLDemo").setMaster("local[2]")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // =======================================================================================================================================
    //读取数据文件 hdfs:/logs/logapi/113/ddate=20170424 ||  hdfs:/logs/logformat/113/dt=20170424* ||   /hdfs/logs/logapi/113/dt=20170424
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113").registerTempTable("113_log")
    val selectDf=sqlContext.sql("select ssid,uv,ads_code,url,ref_url,svn from 113_log where ddate> '20170101' and ddate<'20170321' ")

//        val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170303*")
//        val selectDf=parquetFile.select("ssid","uv","ads_code","url","ref_url","svn").cache()


    val aaa = selectDf.filter(selectDf("ref_url").contains("www.17173.com") && selectDf("url").contains("yeyou.com")).select("ssid").distinct()
    //找到173到yeyou之后，访问的哪些页面(用来源是yeyou的页面 join  173到yeyou的sid)
    val fromYeyou_SidRdd: RDD[((Any, Any), (String,String,String, String))] = selectDf.join(aaa, aaa("ssid") === selectDf("ssid")).rdd.map(row => {
      ((row(0), row(1)), (row(2).toString,row(3).toString, row(4).toString(), row(5).toString))//(ssid,uv),(ads_code,url,ref_url，svn)
    })

    //根据（ssid,suv）分组， 得到用户点击轨迹
    val fromYeyou_SidGroupRdd=fromYeyou_SidRdd.groupByKey()//（ssid,suv）


    val sid_3Url_Rdd: RDD[((String, String), List[(String, Int)])] = fromYeyou_SidGroupRdd.flatMap(sidLine => {
      val sid = sidLine._1._1
      val suv = sidLine._1._2
      val s_List = sidLine._2 //List[(ads_code,url,ref_url，svn),.....]

      val s_sort_List: List[(String, String,String, String)] = s_List.toList.sortBy(x => x._4)
      val _3take: List[(String, String,String, String)] = ArrUtil.take3_2(s_sort_List, "www.17173.com","yeyou.com")

        if (!_3take.isEmpty) {
          val sid_urls = _3take.map(x => x._1.toString) //只取ads_code
          Some((sid.toString, suv.toString), sid_urls.zipWithIndex)// Array[((String,String), List[(String, Int)])]=Array((ssid,suv）, List((ads1,1), (ads2,2), (ads3,3))))
        } else
          None

    })//.filter(_ != None).cache()
    //上面根据用户分组后，组内排序，取前三和点击（url和点击序号），   Array((ssid,suv）, List((ads1,1), (ads2,2), (ads3,3))))


    //    PV===================================================================================================================
    //flatMap= [((url1,第1次点击),1)),((url2,第2次点击),1)]=((http://game.yeyou.com/info-53898.html,0),1), ((http://game.yeyou.com/info-53670.html,1),1)
    // reduceByKey=((http://kc.yeyou.com/,0),16), ((http://top.yeyou.com/,0),7)
    // map= RDD[((String, Int), Int)]   [(url,第几次访问)],次数)  根据次数降序
    val flat_PVUrl: RDD[((String, Int), Int)] = sid_3Url_Rdd.flatMap(x => {
      x._2.map(x => (x, 1))
    })
    val reducePVUrl: RDD[((String, Int), Int)] = flat_PVUrl.reduceByKey(_ + _).sortBy(x => x._2, false).cache()//reducePVUrl_cache


    //   UV===================================================================================================================
    // Array((ssid,suv）, List((url1,1), (url2,2), (url3,3))))== Array(suv, List((url1,1), (url2,2), (url3,3))))
    val suv_3Url: RDD[(String, List[(String, Int)])] = sid_3Url_Rdd.map(line => {
      (line._1._2, line._2)
    })
    // reduceByKey=根据suv聚合List[(url,第几次访问)]= RDD[(String, List[(String, Int)])]
    val suv_views: RDD[(String, List[(String, Int)])] = suv_3Url.reduceByKey((x,y)=>x.++(y))
    // flatMap=只要 RDD[(String, Int)]  ，去重
    val suv_distinViews: RDD[(String, Int)] = suv_views.flatMap(x => {
      x._2.distinct
    })
    //map=RDD[((String, Int), Int)]
    //reduceByKey=RDD[((String, Int-第几次), Int-求和)]
    val reduceUvUrl:RDD[((String, Int), Int)]= suv_distinViews.map(x=>(x,1)).reduceByKey(_+_).sortBy(x => x._2, false)



    //    PV UV join  根据PV排序=====（adsCode,第几次）,（PV，UV）==============================================================================================================
    val joinRusult: RDD[((String, Int), (Int, Int))] = reducePVUrl.join(reduceUvUrl).sortBy(x=>x._2._1,false)

    //翻译模块名称
    val adsCodeName: Map[String, String] = DBUtil.getAdsCode_Name(sqlContext)
    val adsCodeNameBroadCast = sc.broadcast(adsCodeName).value

    val mapRusult: RDD[String] = joinRusult.map(x => {
      x._1._2.toString.toInt+1 + "," + adsCodeNameBroadCast.getOrElse(x._1._1, x._1._1) + "," +x._2._1 + "," + x._2._2
    })

    mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113_levelResult")//  /hdfs/tmp/hugsh/laoqu

  }


}

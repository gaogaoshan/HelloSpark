package com._173.laoqu.work4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.control.Breaks._

/**
  * Created by Administrator on 2017/4/24.
  */
object NeirongUrlTake5_1 {
  val urlTypes=Array[String](
    "http://news.17173.com/content/05032017/070048115_1.shtml",
    "http://news.17173.com/content/05052017/113931436_1.shtml",
    "http://news.17173.com/content/05092017/090836735.shtml",
    "http://news.17173.com/content/05102017/093633680_1.shtml",
    "http://newgame.17173.com/game-review-4044600.html",
    "http://news.17173.com/content/05112017/091532593.shtml",
    "http://news.17173.com/content/05122017/091545587_1.shtml",
    "http://news.17173.com/content/05122017/110001170.shtml",
    "http://news.17173.com/content/05182017/144135401.shtml",
    "http://v.17173.com/v_4_4044600/Mzg4MTIyMTU.html",
    "http://news.17173.com/content/05152017/092908625_1.shtml",
    "http://news.17173.com/content/05152017/090238091_1.shtml",
    "http://news.17173.com/content/05152017/112838205.shtml",
    "http://news.17173.com/z/tlbb/guide/index.shtml",
    "http://news.17173.com/content/05162017/094310578.shtml",
    "http://news.17173.com/content/05172017/123905190_1.shtml",
    "http://news.17173.com/content/05172017/105635197.shtml",
    "http://news.17173.com/zt/201704271644/index.shtml",
    "http://news.17173.com/content/05182017/094030163.shtml",
    "http://news.17173.com/content/05182017/090010732_1.shtml",
    "http://news.17173.com/content/05192017/182044772.shtml",
    "http://news.17173.com/content/05192017/135245278.shtml",
    "http://images.17173.com/tlbb/index.shtml",
    "http://news.17173.com/content/05202017/140503400.shtml",
    "http://news.17173.com/content/05222017/102917867.shtml",
    "http://news.17173.com/z/tlbb/")

  val url_name_Map=Map(
    "http://news.17173.com/content/05032017/070048115_1.shtml"->"让王语嫣做你的师傅腾讯天龙八部手游特色解析",
    "http://news.17173.com/content/05052017/113931436_1.shtml"->"降龙十八掌独步天下？腾讯天龙八部手游职业介绍",
    "http://news.17173.com/content/05092017/090836735.shtml"->"我们就是江湖！腾讯天龙八部手游518全平台开测",
    "http://news.17173.com/content/05102017/093633680_1.shtml"->"还原记忆中的经典！《天龙八部》端手游对比",
    "http://newgame.17173.com/game-review-4044600.html"->"手游麻辣烫",
    "http://news.17173.com/content/05112017/091532593.shtml"->"神器出鞘！腾讯天龙八部手游门派神器重现江湖",
    "http://news.17173.com/content/05122017/091545587_1.shtml"->"分分钟让你懂武侠！腾讯天龙八部手游实战解析",
    "http://news.17173.com/content/05122017/110001170.shtml"->"腾讯天龙八部手游主题曲将首发歌者身份成谜",
    "http://news.17173.com/content/05182017/144135401.shtml"->"《天龙八部手游》今日不限号开启江湖之路",
    "http://v.17173.com/v_4_4044600/Mzg4MTIyMTU.html"->"定制视频栏目：Game囧很大",
    "http://news.17173.com/content/05152017/092908625_1.shtml"->"定制专题：热点预告",
    "http://news.17173.com/content/05152017/090238091_1.shtml"->"相逢一笑泯恩仇腾讯《天龙八部手游》试玩",
    "http://news.17173.com/content/05152017/112838205.shtml"->"腾讯天龙八部手游安卓不删档重现两亿武侠梦",
    "http://news.17173.com/z/tlbb/guide/index.shtml"->"定制专题：版本攻略",
    "http://news.17173.com/content/05162017/094310578.shtml"->"腾讯天龙八部手游安卓不删档开测杨宗纬献声",
    "http://news.17173.com/content/05172017/123905190_1.shtml"->"整个人都龘了!玩家表情包玩坏天龙手游马赛克",
    "http://news.17173.com/content/05172017/105635197.shtml"->"腾讯天龙八部手游不删档人气爆棚LB直播引围观",
    "http://news.17173.com/zt/201704271644/index.shtml"->"定制专题：情怀感召",
    "http://news.17173.com/content/05182017/094030163.shtml"->"腾讯天龙八部手游今日全平台不限号我们就是江湖！",
    "http://news.17173.com/content/05182017/090010732_1.shtml"->"屮艸芔茻怎么读?游戏中那些生僻字你知道多少",
    "http://news.17173.com/content/05192017/182044772.shtml"->"《天龙八部手游》不限号火爆力夺畅销榜亚军",
    "http://news.17173.com/content/05192017/135245278.shtml"->"《天龙八部手游》全平台不限号主播带你游江湖",
    "http://images.17173.com/tlbb/index.shtml"->"定制双端H5页面：生僻字期末考试",
    "http://news.17173.com/content/05202017/140503400.shtml"->"定制专题：哔哔",
    "http://news.17173.com/content/05222017/102917867.shtml"->  "腾讯《天龙八部手游》牵手多直播平台发布主播集结令",
    "http://news.17173.com/z/tlbb/"->  "天龙八部"
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
    val allDf=sparkSession.sql("select suv,dt,addtime,url from traffic_log where dt> '2017050300' and dt<'2017060100' ")//
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
    }).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/urlTypes_Take5-all")//  /hdfs/tmp/hugsh/laoqu*/

    allDf.rdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/urlTypesAll_0430")
  }

}

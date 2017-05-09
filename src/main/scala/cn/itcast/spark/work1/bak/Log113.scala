package cn.itcast.spark.work1.bak

import cn.itcast.spark.utils.DBUtil
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
     // .registerKryoClasses(new Class[]{})

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val sparkconf = new SparkConf().setAppName("LSQS").setMaster("local[*]")
//    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    //读取Hive表数据 2017.1.1-2017.3.21.
//    val sqlResult: DataFrame = sqlContext.sql("select ssid,referurl,url,svn,suv from traffic_log where dt> '2017010100' and dt<'2017032123' ")
//    val selectDf=sqlResult.select("ssid","referurl","url","svn","suv")
//    sqlResult.printSchema()
//    sqlResult.show()
// =======================================================================================================================================
    //读取数据文件 hdfs:/logs/logapi/113/ddate=20170424 ||  hdfs:/logs/logformat/113/dt=20170424* ||   /hdfs/logs/logapi/113/dt=20170424
//    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113").registerTempTable("113_log")
//    val selectDf=sqlContext.sql("select ssid,uv,ads_code,url,ref_url,svn from 113_log where ddate> '20170101' and ddate<'20170321' ")

    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170303*")
    val selectDf=parquetFile.select("ssid","uv","ads_code","url","ref_url","svn").cache()


    val aaa = selectDf.filter(selectDf("ref_url").contains("www.17173.com") && selectDf("url").contains("yeyou.com")).select("ssid").distinct()
    val bbb = selectDf.filter(selectDf("ref_url").contains("yeyou.com"))
    //找到173到yeyou之后，访问的哪些页面(用来源是yeyou的页面 join  173到yeyou的sid)
    val fromYeyou_SidRdd: RDD[((Any, Any), (Any,Any, String))] = bbb.join(aaa, aaa("ssid") === bbb("ssid")).rdd.map(row => {
      ((row(0), row(1)), (row(2).toString, row(4).toString(), row(5).toString))//(ssid,uv),(ads_code,ref_url，svn)
    })

    //根据（ssid,suv）分组， 得到用户点击轨迹
    val fromYeyou_SidGroupRdd=fromYeyou_SidRdd.groupByKey()//（ssid,suv）


    //上面根据用户分组后，组内排序，取前三和点击（url和点击序号），   Array((ssid,suv）, List((ads1,1), (ads2,2), (ads3,3))))
    val sid_3Url_Rdd=fromYeyou_SidGroupRdd.map(sidLine=>{
      val sid=sidLine._1._1
      val suv=sidLine._1._2
      val s_List=sidLine._2
      val s_sort_List=s_List.toList.sortBy(x=>x._3).take(3)
      val sid_urls =s_sort_List.map(x=> x._1.toString )//只取ads_code
      // Array[((String,String), List[(String, Int)])]=Array((ssid,suv）, List((ads1,1), (ads2,2), (ads3,3))))
      ((sid.toString,suv.toString) ,sid_urls.zipWithIndex)
    }).cache()


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
      x._1._2 + "," + adsCodeNameBroadCast.getOrElse(x._1._1, x._1._1) + ","
      +x._2._1 + "," + x._2._2
    })

      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113-02")//  /hdfs/tmp/hugsh/laoqu



  }



//  //173到yeyou的ssid--做小表字典用  //正常是[ssid]数组，要(0)选出来变成ssid
//  val _173ToYeyou_Sid: Array[String] = selectDf.filter(selectDf("referurl").contains("17173.com") && selectDf("url").contains("yeyou.com"))
//    .select("ssid").distinct()
//    .rdd.map(x => x(0).toString).collect()
//  //广播规则
//  val sidBroadCast = sc.broadcast(_173ToYeyou_Sid)
//  //从yeyou主页点击的页面  //用Some外面要FlatMap
//  val fromYeyou_SidRdd=selectDf.filter(selectDf("referurl").contains("http://www.yeyou.com/")).rdd.
//    flatMap{line =>
//      val ssid = line(0)
//      if (sidBroadCast.value.contains(ssid)) Some((line(0),line(4)), (line(1), line(2), line(3).toString))
//      else  None
//    }
//  //.filter(_ != None)  Array((（ssid,suv）,(referurl,url,svn)),(),())


 // case class sidObject(ssid: String, referurl: String, url: String, addtime: String, svn: String) extends Serializable
//  Name		Type	  Comment
//    1		  ip			  string	Add a comment...
//    2		  iploc		  string	地域编码
//    3		  area		  struct<COUNTRY:string,PROVINCE:string,CITY:string>	{country:国家,province:省份,city:城市}
//    4		  tids		  struct<SITEIDS:array<int>,CNLIDS:array<int>,COLIDS:array<int>,GIDS:array<int>>	{siteIds:站点id列表,cnlIds:频道id列表,colIds:栏目id列表,gids:组id列表}
//    5		  url			  string	当前url
//    6		  clickurl	string	点击的模块url
//    7		  domain		string	当前域名
//    8		  randomstr	string	随机值
//    9		  suv			  string	uv
//    10		isnewer			是否新用户(0/1:旧/新)
//    11		referurl	string	来源url
//    12		referdomain	string	来源域名
//    13		refer		  struct<REFERTYPE:int,SEARCHTYPE:int>	{referType:来源类型(1:直接来源,2:搜索引擎,3:站内来源,4:外部来源),searchType:搜索引擎类型}
//    14		modules		array<struct<ADSCODE:string,POS:string>>	[{adsCode:模块code,pos:点击的模块索引}, ....]
//    15		kw			  string	搜索关键字
//    16		ua			  string	user-agent
//    17		ol			  int	时长
//    18		ssid		  string	会话id
//    19		svn			  int	点击的第几个模块
//    20		uid			  string	用户id
//    21		clitype		int	pc/mobile:1/2
//    22		addtime		string	点击时间
//    23		day			  string	yyyymmdd
//    24		hour		  int	HH
//    25		dt			  string	Add a comment...

  //  object OrderContext {
  //    implicit val sidTimeOrdering  = new Ordering[sidObject] {
  //      override def compare(x: sidObject, y: sidObject): Int = {
  //        if(x.svn > y.svn) 1
  //        else -1
  //      }
  //    }
  //  }


}

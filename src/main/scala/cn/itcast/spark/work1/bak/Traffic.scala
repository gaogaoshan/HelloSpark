package cn.itcast.spark.work1.bak

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/24.
  */
object Traffic {



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
    //读取数据文件 /hdfs/logs/logformat/traffic/dt=2017042400 ||  hdfs:/logs/logformat/traffic/dt=20170424* ||   C:/testData/traffic/2017042400/
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logformat/traffic").registerTempTable("traffic_log")
    val selectDf=sqlContext.sql("select ssid,suv,referdomain,domain,svn from traffic_log where dt> '2017010100' and dt<'2017032123' ")
//    val selectDf=sqlContext.sql("select ssid,suv,referdomain,url,svn from traffic_log where dt> '2017010100' and dt<'2017032123' ")

//    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logformat/traffic/dt=2017042400")
//    val selectDf=parquetFile.select("ssid","referurl","url","svn","suv").cache()


    val aaa = selectDf.filter(selectDf("referdomain").contains("17173.com") && selectDf("domain").contains("yeyou.com")).select("ssid").distinct()
    val bbb = selectDf.filter(selectDf("referdomain").contains("yeyou.com"))
    //找到173到yeyou之后，访问的哪些页面
    val fromYeyou_SidRdd: RDD[((Any, Any), (Any, String))] = bbb.join(aaa, aaa("ssid") === bbb("ssid")).rdd.map(row => {
      ((row(0), row(1)), (row(3), row(4).toString))
    })


    //根据（ssid,suv）分组， 得到用户点击轨迹
    val fromYeyou_SidGroupRdd=fromYeyou_SidRdd.groupByKey()//（ssid,suv）


    //上面根据用户分组后，组内排序，取前三和点击（url和点击序号），   Array((ssid,suv）, List((url1,1), (url2,2), (url3,3))))
    val sid_3Url_Rdd=fromYeyou_SidGroupRdd.map(sidLine=>{
      val sid=sidLine._1._1
      val suv=sidLine._1._2
      val s_List=sidLine._2

      val s_sort_List=s_List.toList.sortBy(x=>x._2).take(3)
      val sid_urls =s_sort_List.map(x=> x._1.toString )//只取url
      // Array[((String,String), List[(String, Int)])]=Array((ssid,suv）, List((url1,1), (url2,2), (url3,3))))
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



//    PV UV join  根据PV排序===================================================================================================================
      val joinRusult: RDD[((String, Int), (Int, Int))] = reducePVUrl.join(reduceUvUrl).sortBy(x=>x._2._1,false)
      //第几次访问，url,pv,uv
      val mapRusult: RDD[String] = joinRusult.map(x=>({
        x._1._2.toString.toInt+1+","   +x._1._1+","   +x._2._1+","    +x._2._2
      }))


      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/wc03")//  /hdfs/tmp/hugsh/laoqu


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
  case class UrlCount(url: String, count: String)
//  case class TrafficLine(
//                          ip: String,
//                          iploc: String,
//                          //               area: Area,
//                          country:String,        //ip解析
//                          province:String,
//                          city:String,
//                          siteIds:List[Int],
//                          cnlIds:List[Int],
//                          colIds:List[Int],
//                          gids:List[Int],
//                          //               tids: Tids,
//                          url: String,
//                          domain: String,
//                          randomStr: String,     //随机数
//                          suv: String,
//                          isNewer: Int,            //是否新用户(0/1:旧/新)
//                          referUrl: String,
//                          referDomain: String,
//                          referType: Int,        //来源类型（全部来源0，直接访问through 1，搜索引擎2，外部链接outlink 4, 站内来源500）
//                          searchType: Int,       //搜索引擎类型
//                          kw: String,            //搜索关键字
//                          ua: String,            //userAgent
//                          openJav: Int,          //是否支持并启用了Java 10
//                          flu: String,           //Flash版本(浏览器内置)11
//                          os: String,            //操作系统12
//                          scr: String,           //分辨率13
//                          clr: String,           //色彩(浏览器内置)14
//                          lng: String,           //语言(浏览器内置)15
//                          isOpenCk: Int,         //COOKIE是否开启(浏览器内置)16
//                          isDy: Int,             //是否订阅　17
//                          bro: String,           //浏览器18
//                          lastPageOl: Int,       //前一页在线时长（秒）19
//                          appName: String,       //app名称
//                          ssid: String,
//                          svn: Int,              //用户翻页数 22
//                          lastDiffTime: Int,     //上次到本次会话访问时间间隔 >0  ： 超过1小时 0  ： 首次访问 -1  ： 1小时内访问  -3/-4  ： 异常数据
//                          loginType: Int,        //未登录0 半登录username|1 登录uid|2 　25
//                          uid: String,           //用户id
//                          userName: String,      //用户名
//                          isOutSite: Int,        //是否站外引用(0/1)
//                          cliType: Int,          //pc/mobile:1/2
//                          addTime: String,       //访问时间
//                          dt: String
//                        )

  //  object OrderContext {
  //    implicit val sidTimeOrdering  = new Ordering[sidObject] {
  //      override def compare(x: sidObject, y: sidObject): Int = {
  //        if(x.svn > y.svn) 1
  //        else -1
  //      }
  //    }
  //  }
  //读取一个parquet文件     val dataDf = sqlContext.read.format("parquet").load("C:/testData/traffic/2017042400/*.parquet")   //C://testData//traffic//2017042400//*     hdfs://hadoop1:9000/input/users.parquet
  //写一个parquet文件      usersDF.write.mode(SaveMode.Overwrite).format("parquet").save("hdfs://hadoop1:9000/output/namesAndFavColors_scala")

}

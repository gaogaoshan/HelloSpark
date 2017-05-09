package cn.itcast.spark.work1.bak

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/24.
  */
object TrafficOld {




  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[1]")// local[2]
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val sparkconf = new SparkConf().setAppName("LSQS").setMaster("local[*]")
//    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    //读取Hive表数据 2017.1.1-2017.3.21.
//    val sqlResult: DataFrame = sqlContext.sql("select ssid,referurl,url,svn,suv from traffic_log where dt> '2017010100' and dt<'2017032123' ")
//    val selectDf=sqlResult.select("ssid","referurl","url","svn","suv")
//    sqlResult.printSchema()
//    sqlResult.show()

    //读取数据文件 /hdfs/logs/logformat/traffic/dt=2017042400 ||  hdfs:/logs/logformat/traffic/dt=20170424* ||   C:/testData/traffic/2017042400/
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logformat/traffic").registerTempTable("traffic_log")
    val selectDf=sqlContext.sql("select ssid,referurl,url,svn,suv from traffic_log where dt> '2017010100' and dt<'2017032123' ")

//    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logformat/traffic/dt=20170424*")
//    val selectDf=parquetFile.select("ssid","referurl","url","svn","suv").cache()


    //173到yeyou的ssid--做小表字典用  //正常是[ssid]数组，要(0)选出来变成ssid
    val _173ToYeyou_Sid: Array[String] = selectDf.filter(selectDf("referurl").contains("17173.com") && selectDf("url").contains("yeyou.com"))
      .select("ssid").distinct()
      .rdd.map(x => x(0).toString).collect()
    val sidBroadcast = sc.broadcast(_173ToYeyou_Sid).value


//    selectDf.rdd.filter(f=>_173ToYeyou_Sid.contains(_(0)))
    //从yeyou主页点击的页面  //用Some外面要FlatMap

    val fromYeyou_SidRdd: RDD[((Any, Any), (Any, Any, String))] = selectDf.filter(selectDf("referurl").contains("http://www.yeyou.com/")).rdd.
      flatMap { line =>
        val ssid = line(0)
        if (sidBroadcast.contains(ssid))
          Some((line(0), line(4)), (line(1), line(2), line(3).toString))
        else
          None
      }
    //.filter(_ != None)  Array((（ssid,suv）,(referurl,url,svn)),(),())

    //根据（ssid,suv）分组
    val fromYeyou_SidGroupRdd=fromYeyou_SidRdd.groupByKey()//（ssid,suv）


    //分组排序后取前三 只要url   sidLine=（ssid,suv）,[("referurl","url","svn")，(""referurl","url","svn")]
    val sid_3Url_Rdd=fromYeyou_SidGroupRdd.map(sidLine=>{
      val sid=sidLine._1._1
      val suv=sidLine._1._2
      val s_List=sidLine._2
      val s_sort_List=s_List.toList.sortBy(x=>x._3).take(3)
      val sid_urls =s_sort_List.map(x=> x._2.toString )//只取url

      ((sid.toString,suv.toString) ,sid_urls.zipWithIndex)// Array[((String,String), List[(String, Int)])]=Array((ssid,suv）, List((url1,1), (url2,2), (url3,3))))
    }).cache()



    //    PV
        //flatMap= [((url1,第1次点击),1)),((url2,第2次点击),1)]=((http://game.yeyou.com/info-53898.html,0),1), ((http://game.yeyou.com/info-53670.html,1),1)
        // reduceByKey=((http://kc.yeyou.com/,0),16), ((http://top.yeyou.com/,0),7)
        // map= RDD[((String, Int), Int)]   [(url,第几次访问)],次数)  根据次数降序
        val flat_PVUrl: RDD[((String, Int), Int)] = sid_3Url_Rdd.flatMap(x => {
          x._2.map(x => (x, 1))
        })
        val reducePVUrl: RDD[((String, Int), Int)] = flat_PVUrl.reduceByKey(_ + _).sortBy(x => x._2, false)


 //   UV
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



//    PV UV join  根据PV排序
      val joinRusult: RDD[((String, Int), (Int, Int))] = reducePVUrl.join(reduceUvUrl).sortBy(x=>x._2._1,false)
      //第几次访问，url,pv,uv
      val mapRusult: RDD[(Int, String, Int, Int)] = joinRusult.map(x=>(x._1._2,x._1._1,x._2._1,x._2._2))


      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/wc01")//  /hdfs/tmp/hugsh/laoqu









    //每个页面的PV&UV
//    val _3Url=sid_3Url_Rdd.map(x=>x._2).cache()
//    val flat_PVUrl=_3Url.flatMap(_.map(x=>(x,1)))//吧List(list,list) 压平成List--->后加上1 --> List((url,1),(url,1))
//    val reducePVUrl=flat_PVUrl.reduceByKey(_+_)//统计url次数----》List((url,n1),(url,n2))
//    val flat_UVUrl=_3Url.map(_.distinct).flatMap(_.map(x=>(x,1))) //List(list,list) 里面每个list去重后 再做上面PV操作
//    val reduceUvUrl=flat_UVUrl.reduceByKey(_+_)
//    //val reducePvUv_Url=reducePVUrl.join(reduceUvUrl).sortBy(_._2._1,false).collectAsMap()  // Array((url1,(19,17)), (url2,(8,8)))
//    val reducePvUv=reducePVUrl.join(reduceUvUrl).sortBy(_._2._1,false).map(t=>t._1+","+t._2)
//    reducePvUv.saveAsTextFile("/tmp/hugsh/laoqu/wc01")//821 --/hdfs/tmp/hugsh/laoqu 挂载



//    val resultRdd=sid_3Url_Rdd.map(x=>{
//      var sid=x._1
//      var sid_urls=x._2
//      var r=sid_urls.map(url =>{
//        (url,reducePvUv_Url.getOrElse(url,(-1,-1)))
//      })
//      (sid,r(0),r(1),r(2))
//    })
//    resultRdd.saveAsTextFile("/root/hugsh/data/out/wc12")//821
    //==================================================================================================================================================================================================================
//    val ssidGroup=_173ToYeyou_SidRdd.groupBy(x=>x(0))//("ssid",[("referurl","url","ssid","addtime")],[("referurl","url","ssid","addtime")])
//    val ssid3=ssidGroup.filter(x=>x._2.size ==3)
//    val filter2Df=parquetFile
//            .filter(parquetFile("referurl").contains("yeyou.com"))
//            .select("ssid","referurl","url","svn")
//    val groupDf2= filter2Df.groupBy("ssid").agg(Map("svn" -> "max")) //[ssid,max(svn)]
//    val orderDf=groupDf2.sort(groupDf2("max(svn)").desc)
//    orderDf.take(3)//Array([ssid,max(svn)], [ssid,max(svn)], [ssid,max(svn)])

    //==================================================================================================================================================================================================================
    //初始化数据
//    val filter3Df=parquetFile.filter(parquetFile("referurl").contains("yeyou.com")).select("ssid","referurl","url","addtime","svn").rdd.cache()
//    val group3Rdd= filter3Df.groupBy(x=>x(0))// Array((ssid,CompactBuffer([ssid,referurl,url,addtime,svn])))
//
//    val sortRdd=group3Rdd.map(sidLine=>{
//      val sid=sidLine._1
//      val s_List=sidLine._2
//      val s_sort_List=s_List.toList.sortBy( v =>
//        v(4).toString
//      ).take(3)//根据addtime排序 取前3
//
//      val sid_urls =s_sort_List.map(x=>x(2))//只取url
//      sid_urls//ssid的urls   [url1,url2,url3]
//    }).cache()
//
//    val flat_PVUrl=sortRdd.flatMap(_.map(x=>(x,1)))//吧List(list,list) 压平成List--->后加上1 --> List((url,1),(url,1))
//    val reducePVUrl=flat_PVUrl.reduceByKey(_+_)//统计url次数----》List((url,n1),(url,n2))
//    val flat_UVUrl=sortRdd.map(_.distinct).flatMap(_.map(x=>(x,1))) //List(list,list) 里面每个list去重后 再做上面PV操作
//    val reduceUVUrl=flat_UVUrl.reduceByKey(_+_)
//    val reduceUrl=reducePVUrl.join(reduceUVUrl).sortBy(_._2._1,false)
//
//    reduceUrl.saveAsTextFile("/root/hugsh/data/out/wc11")//821


    //    val ssidGroup2=filter2Rdd.groupBy(x=>x(0))//("ssid",[("referurl","url","ssid","addtime")],[("referurl","url","ssid","addtime")])
//    ssidGroup2.agg
//    ssidGroup2.take(1)

  }




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

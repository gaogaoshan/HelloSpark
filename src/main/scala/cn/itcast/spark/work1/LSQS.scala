package cn.itcast.spark.work1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by Administrator on 2017/4/24.
  */
object LSQS {

//  object OrderContext {
//    implicit val sidTimeOrdering  = new Ordering[sidObject] {
//      override def compare(x: sidObject, y: sidObject): Int = {
//        if(x.svn > y.svn) 1
//        else -1
//      }
//    }
//  }

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[1]")// local[2]
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
    println("begin-----------------------")
      //读取一个parquet文件     val dataDf = sqlContext.read.format("parquet").load("C:/testData/traffic/2017042400/*.parquet")   //C://testData//traffic//2017042400//*     hdfs://hadoop1:9000/input/users.parquet
      //写一个parquet文件      usersDF.write.mode(SaveMode.Overwrite).format("parquet").save("hdfs://hadoop1:9000/output/namesAndFavColors_scala")

      val parquetFile=sqlContext.read.parquet("/root/hugsh/data/traffic/2017042400")// /root/hugsh/data/traffic/2017042400    C:/testData/traffic/2017042400/
    // sessionid, suv(计算uv数), 访问时间
    // 返回数组0开始 Array([http://www.17173.com/,http://kf.yeyou.com/redirect-1659633.html?ref=indexkfb,149296704052229014929665472505861492965078707,2017-04-24T00:31:18+08:00])
    //==================================================================================================================================================================================================================
    val filterRdd= parquetFile
            .filter( parquetFile("referurl").contains("17173.com") && parquetFile("url").contains("yeyou.com"))
            .select("ssid","referurl","url","addtime","svn").rdd
    val ssidGroup=filterRdd.groupBy(x=>x(2))//("ssid",[("referurl","url","ssid","addtime")],[("referurl","url","ssid","addtime")])
    val ssid3=ssidGroup.filter(x=>x._2.size ==3)
    //==================================================================================================================================================================================================================
    val filter2Df=parquetFile.filter(parquetFile("referurl").contains("yeyou.com")).select("ssid","referurl","url","addtime","svn")
    val groupDf2= filter2Df.groupBy("ssid").agg(Map("svn" -> "max")) //[149296666856998514929670915514331492965157863,3]
    val orderDf=groupDf2.sort(groupDf2("max(svn)").desc)
    orderDf.take(3)//Array([149296178365032514929638755752061492963409085,96], [148717533999071814929632478299751492963117349,73], [149296386793434214929638244258991492962609174,68])
    //==================================================================================================================================================================================================================
    val filter3Df=parquetFile.filter(parquetFile("referurl").contains("yeyou.com")).select("ssid","referurl","url","addtime","svn").rdd
    val group3Rdd= filter3Df.groupBy(x=>x(0))// Array((1492966222115280627614929667024706021492966465093,CompactBuffer([1492966222115280627614929667024706021492966465093,http://www.yeyou.com/search.shtml?q=\xE8\x88\xB0\xE5\xA8\x98,http://v.17173.com/list/index/bc/2?rel=yeyou,2017-04-24T00:53:04+08:00,1])))

    val sortRdd=group3Rdd.map(sidLine=>{
      val sid=sidLine._1
      val s_List=sidLine._2
      val s_sort_List=s_List.toList.sortBy( v =>
        v(4).toString
      ).take(3)//根据addtime排序 取前3

      val sid_urls =s_sort_List.map(x=>x(2))//只取url
      sid_urls//ssid的urls   [url1,url2,url3]
    })

    val flat_sidUrl=sortRdd.flatMap(_.map(x=>(x,1)))//吧List(list,list)==》List()

    val reduceUrl=flat_sidUrl.reduceByKey(_+_).sortBy(_._2,false)//统计url次数
    reduceUrl.saveAsTextFile("/root/hugsh/data/out/wc8")


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

}

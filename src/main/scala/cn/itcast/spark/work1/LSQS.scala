package cn.itcast.spark.work1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by Administrator on 2017/4/24.
  */
object LSQS {


  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[1]")// local[2]
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
    println("begin-----------------------")
      //读取一个parquet文件
      //      val dataDf = sqlContext.read.format("parquet").load("C:/testData/traffic/2017042400/*.parquet")   //C://testData//traffic//2017042400//*     hdfs://hadoop1:9000/input/users.parquet
      //写一个parquet文件
      //usersDF.write.mode(SaveMode.Overwrite).format("parquet").save("hdfs://hadoop1:9000/output/namesAndFavColors_scala")

      val parquetFile=sqlContext.read.parquet("/root/hugsh/data/traffic/2017042400")// /root/hugsh/data/traffic/2017042400    C:/testData/traffic/2017042400/
    // sessionid, suv(计算uv数), 访问时间
     val filterData= parquetFile.filter(
        parquetFile("referurl").contains("17173.com") &&
          parquetFile("url").contains("yeyou.com"))

    val resultData=filterData.select("referurl","url","ssid","addtime")
    resultData.take(3)

//      parquetFile.registerTempTable("pTable");
//      val data = sqlContext.sql("SELECT referurl,url,ssid,addtime FROM pTable where limit 3")     // referurl url  ssid addtime dt  yeyou.com  17173.com
//      data.collect().take(2)


//    val rdd1 = sc.textFile("C:\\testData\\itcast.log").map(line => {//20160321102225	http://java.itcast.cn/java/course/javaee.shtml
//      val f = line.split("\t")
//      UrlCount(f(1), f(0))
//    })
//    import sqlContext.implicits._
//    val urlDf=rdd1.toDF()
//    urlDf.registerTempTable("Url")
//    val resultDf= sqlContext.sql("select * from Url  limit 5")
//    resultDf.show()
//    resultDf.write.mode(SaveMode.Overwrite).save("C:\\testData\\Url")



  }

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

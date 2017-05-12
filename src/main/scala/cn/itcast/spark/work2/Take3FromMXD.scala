package cn.itcast.spark.work2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Map, mutable}
import scala.collection.mutable.{Map => MMap, Set => SSet}
import scala.util.control.Breaks._


object Take3FromMXD {

  /**
    *当前索引后取后4个
    * @param arry List[(url,ads_code,click_url,ts),.....]  url like %www.17173.com%   click_url=http://newgame.17173.com/game-info-1000219.html
    * @return  (String, List[String]) --- (ads_code,List(click_url1,click_url2,click_url3))
    */
  def take3(arry: List[(String, String, String,String)]):(String, List[String]) ={
    var _take4Url: List[String] = List.empty
    var ads_code:String=""
    val adsList=List("3a9b3f036a2f571a23e1c499d1477791","6273154c808759048e3200c51cbb44b1","9c9f1366edbae758a3bb56eca4388b64")

    breakable(
      for (i <-arry.zipWithIndex){
        val url=i._1._1  ; val code=i._1._2   ; val click_url=i._1._3
        if(url.contains("www.17173.com")  && adsList.contains(code) &&  click_url=="http://newgame.17173.com/game-info-1000219.html"){

          val index:Int=i._2
          ads_code=code
          val tmp=for(step <- 1 to 4; if(index + step < arry.length) ) yield arry(index + step)._3

          _take4Url=tmp.toList
          break()
        }
      }
    )
    (ads_code,_take4Url)
  }


  def getCodeName(code:String): String = {
    val codeName=
      code match {
        case "3a9b3f036a2f571a23e1c499d1477791" =>"测试时间表"
        case "6273154c808759048e3200c51cbb44b1"=>"新游期待榜"
        case "9c9f1366edbae758a3bb56eca4388b64"=>"游戏图片导航"
      }
    codeName
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
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170307*").createOrReplaceTempView("113_log")
//    捞取2017年3月7日，www.17173.com地址【新游测试表】【新游期待榜】【图片导航】三个模块中点击http://newgame.17173.com/game-info-1000219.html用户的单次会话
//    结果格式			用户		入口模块名	指定页面	URL1	URL2	URL3	URL4

//    1.获取满足要求的用户UV集合
    // url like %www.17173.com%
    // ads_code in【新游测试表】【新游期待榜】【图片导航】
    // click_url=http://newgame.17173.com/game-info-1000219.html
//    2.用全部数据和1的结果关联得到 满足条件用户的的点击轨迹
//    3.根据UV分组 并按SVN排序  每个分组内取前3次点击


    //1.获取满足要求的用户UV集合 uvSet.count()=14217
    val ssidSql="""select distinct ssid from 113_log
             where url like '%www.17173.com%'
             and  ads_code in('3a9b3f036a2f571a23e1c499d1477791','6273154c808759048e3200c51cbb44b1','9c9f1366edbae758a3bb56eca4388b64')
             and  click_url='http://newgame.17173.com/game-info-1000219.html'  """.stripMargin.replaceAll("\n","");
    val uvSet= sqlContext.sql(ssidSql)


   //2.用全部数据和1的结果关联得到 满足条件用户的的点击轨迹  allData.count()=3917661
    val allData=sqlContext.sql("select ssid,url,ads_code,click_url,ts from 113_log  ")
    val joinRdd: RDD[(Any, (String, String, String, String))] = allData.join(uvSet, "ssid").rdd.map(r => (r(0), (r(1).toString, r(2).toString, r(3).toString, r(4).toString))).cache()



    //3.根据UV分组 并按SVN排序  取冒险岛后的4次url (uv,adsCode,urls)
    val take4Rdd: RDD[(Any, String, String)] = joinRdd.groupByKey().flatMap(x => {
      val ssid = x._1
      val clickList = x._2 //url,ads_code,click_url,svn
      val sortedClickList = clickList.toList.sortBy(x => x._4)

      val code_4urls: (String, List[String]) = take3(sortedClickList) //(ads_code,List(url1,url2,url3))

      if (!code_4urls._2.isEmpty) {
        val codeName = getCodeName(code_4urls._1)
        Some(ssid, codeName, getStringFromList(code_4urls._2))
      }
      else None
    })

    val resultRdd: RDD[String] = take4Rdd.map(x => x._1 + "," + x._2 + "," + x._3)

    resultRdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/mxd4")



//    val selectData11=sqlContext.sql("select ssid,url,ads_code,click_url,svn,ts  from 113_log  order by ts")
//    selectData11.rdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/t3")

  }



}

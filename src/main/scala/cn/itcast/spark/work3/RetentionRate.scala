package cn.itcast.spark.work3

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.itcast.spark.utils.{LogFormat, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.mutable.{Map => MMap, Set => SSet}

object RetentionRate {


  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("sparkSQL").
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      enableHiveSupport().
      getOrCreate()

// =======================================================================================================================================
//    统计每周的新用户在未来数周的留存情况，生成用户堆积数据表。
//
//    1、时间范围是2016年7月4日-2017年4月30日；
//    2、统计维度是自然周和DAU；
//    3、输出数据为周期时间和对应的活跃UV；（参见数据表范例1）
//    4、分来源输出4个sheet：直接访问、搜索引擎、站内来源、外部链接。


    // 1. select ssid | newuv（是否新用户 0=老，1=新） | refer_domain（来源的域名） | search（整数说明来源于搜索引擎） | ddate
    // 2. 格式化=ddate格式化成（年+周），添加refer_type 字段  ====》【(refer_type+周),ssid，isnew】
    // 3. 过滤出 listAll=List【(refer_type+周),ssid】.distinct    和 listNew=List【(refer_type+周),ssid_new】.distinct
    // 4. gAll=listAll group by (refer_type+周)= List【(refer_type+周),ssidList】,    gNew=listNew group by (refer_type+周)=List【(refer_type+周),ssidNewList】
    // 5. for gNew ，每个 数据都和 gAll  做join  ==>List【(refer_type，周new1，周all2),(ssidNewList 求交集 ssidList)】  ===》size===> yield(refer_type ,周new1交周all2，ssidList。size)
    // 6. group by refer_type  ===map===(refer_type ,【（周1交集周2，size），（周1交集周3，size），（周1交集周4，size），，，，】)


    //1
    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate from traffic where ddate>='20170506'  and  ddate<='20170507'  ").rdd
    selectRdd.count()//24806815

    //2格式化  List[(refer_type,周),(suv，isnew)]
    val formatRdd: RDD[((String, String), (String, String))] = selectRdd.map(r => {
      val suv = r(0).toString
      val newuv = r(1).toString
      val refer_type = LogFormat.getReferType(r(2).toString, r(3).toString)
      val ddate = TimeUtil.getWeekFromData(r(4).toString,"yyyyMMdd")

      ((refer_type, ddate), (suv, newuv))
    }).distinct().cache()//persist(StorageLevel.DISK_ONLY)  7513431

    //3过滤List[(refer_type,周),suv]   每天所有的suv 和 新增的suv
    val allRdd: RDD[((String, String), String)] = formatRdd.map(r => {(r._1, r._2._1)}).cache()
    val isNewRdd: RDD[((String, String),String)] = formatRdd.filter(r=> r._2._2=="1").map(r => {(r._1, r._2._1)}).cache()

    allRdd.count()//7513431
    isNewRdd.count()//4466829
    formatRdd.unpersist()

    //4 统计  week1New intersection (week1All,week2All，week3All)
    val refers=List("直接来源","搜索引擎"  , "站内来源"  , "外部来源"  )
    val weeks=List("2017_18","2017_19")



    for(i<-  0 until  refers.length){
      val ref_index=refers(i)
      println("begin ref="+ref_index)

      for(x<-  0 until  weeks.length){//第x周newUv 和下面一个for的 (x+1 until all) allUv求交集
        val week_index_x=weeks(x)

        val newUv: RDD[String] = isNewRdd.filter(r => {
          val ref = r._1._1
          val week = r._1._2
          ref == ref_index && week == week_index_x
        }).map(m => m._2)
        println("  begin week="+week_index_x +" newSuv size="+newUv.count())

        for(y<- x until weeks.length){
          val week_index_y=weeks(y)

          val allUv: RDD[String] = allRdd.filter(r => {
            val ref = r._1._1
            val week = r._1._2
            ref == ref_index && week == week_index_y
          }).map(m => m._2)

          val count= newUv.intersection(allUv).count()
          println("   new "+week_index_x+" intersection "+week_index_y+" size="+count)
        }
      }
    }

    allRdd.unpersist()
    isNewRdd.unpersist()
//      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113-02")//  /hdfs/tmp/hugsh/laoqu


  }



}


//begin ref=直接来源
//begin week=2017_18 newSuv size=380563
//new 2017_18 intersection 2017_18 size=380563
//new 2017_18 intersection 2017_19 size=6448
//begin week=2017_19newSuv size=409764
//new 2017_19 intersection 2017_19 size=409764
//
//begin ref=搜索引擎
//begin week=2017_18 newSuv size=785091
//new 2017_18 intersection 2017_18 size=785091
//new 2017_18 intersection 2017_19 size=23948
//begin week=2017_19newSuv size=727868
//new 2017_19 intersection 2017_19 size=727868
//
//
//begin ref=站内来源
//begin week=2017_18 newSuv size=376497
//new 2017_18 intersection 2017_18 size=376497
//new 2017_18 intersection 2017_19 size=10349
//begin week=2017_19newSuv size=424211
//new 2017_19 intersection 2017_19 size=424211
//
//
//begin ref=外部来源
//begin week=2017_18 newSuv size=626869
//new 2017_18 intersection 2017_18 size=626869
//new 2017_18 intersection 2017_19 size=4100
//begin week=2017_19newSuv size=735966
//new 2017_19 intersection 2017_19 size=735966
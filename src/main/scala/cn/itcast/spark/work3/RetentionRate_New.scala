package cn.itcast.spark.work3

import cn.itcast.spark.utils.{LogFormat, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.Map
import scala.collection.mutable.{ListMap => LListMap, Map => MMap, Set => SSet}

object RetentionRate_New {


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



    //1
    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate from traffic where ddate>='20170506'  and  ddate<='20170507'  ").rdd
    selectRdd.count()//24806815

    //2添加来源字段| 格式化时间字段为周   List[(suv,(refer_type,周，isnew))]
    val formatRdd: RDD[(String, (String , String, String))] = selectRdd.map(r => {
      val suv = r(0).toString
      val newuv = r(1).toString
      val refer_type = LogFormat.getReferType(r(2).toString, r(3).toString)
      val ddate = TimeUtil.getWeekFromData(r(4).toString)

      (suv,(refer_type, ddate , newuv))
    }).distinct()//persist(StorageLevel.DISK_ONLY)  7513431


    //3聚合用户的来访轨迹(suv,List[(refer_type,周，isnew)])
    val uvGroup: RDD[(String, Iterable[(String, String, String)])] = formatRdd.groupByKey()

    //4 用户分组内 按照isnew排序 (suv,List[(refer_type,周，isnew)])
    val sortUvGroup: RDD[(String, List[(String, String, String)])] = uvGroup.map(g => {
      val suv = g._1
      val sortList = g._2.toList.sortBy(x => x._3).reverse
      (suv, sortList)
    }).cache()
    println("sortUvGroup has UV size="+sortUvGroup.count())//5698576


    //5 过滤出要对比的那一周的留存率  比如要算第2周新用户  在3,4,5周的留存
    // 排序后的List 取第一个，【如果周==第2周】&&【isnew==1】    是就说明这个用户是第二周来的
//    val refers=List("直接来源","搜索引擎"  , "站内来源"  , "外部来源"  )
    //    var resMap:MMap[(String, String), Int] = MMap()
    val weeks=List("2017_18","2017_19")

    for(x<-  0 until  weeks.length){

//      val w=weeks(x)
      val w="2017_19"
      val newUvGroup: RDD[(String, List[(String, String, String)])] = sortUvGroup.filter(f => {
        val uvWeek = f._2.head._2
        val uvIsNew = f._2.head._3
        uvWeek == w && uvIsNew == "1"
      })
      println("filter week="+w+" new UV size="+newUvGroup.count())

      val refer_week_rdd: RDD[((String, String),Int)] = newUvGroup.flatMap(r => {
        val dis: List[((String, String),Int)] = r._2.map(x => { ((x._1, x._2),1)  }).distinct
        dis
      })
      val refer_week_count: RDD[((String, String), Int)] = refer_week_rdd.reduceByKey(_+_)

      val asMap: Map[(String, String), Int] = refer_week_count.collectAsMap()
      println(asMap)
    }







//    filter week=2017_18 new UV size=1809071
//    Map((直接来源,2017_19) -> 9565, (外部来源,2017_18) -> 627732, (搜索引擎,2017_18) -> 787561, (站内来源,2017_19) -> 14312, (外部来源,2017_19) -> 5240, (搜索引擎,2017_19) -> 25982, (站内来源,2017_18) -> 377853, (直接来源,2017_18) -> 383175)
//    filter week=2017_19 new UV size=1902387
//    Map((直接来源,2017_19) -> 412318, (外部来源,2017_18) -> 672, (搜索引擎,2017_18) -> 1976, (站内来源,2017_19) -> 425290, (外部来源,2017_19) -> 736811, (搜索引擎,2017_19) -> 730107, (站内来源,2017_18) -> 1572, (直接来源,2017_18) -> 1036)
//

//    selectRdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/RetiontoinRateAll")
//      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113-02")//  /hdfs/tmp/hugsh/laoqu
  }



}

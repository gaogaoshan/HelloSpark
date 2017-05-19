package cn.itcast.spark.work3

import cn.itcast.spark.utils.{LogFormat, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.Map
import scala.collection.mutable.{ Map => MMap, Set => SSet}

object RetentionRate_New {


  class SecondarySortKey(val first: String, val second: String) extends Ordered[SecondarySortKey] with Serializable {
    override def compare(other: SecondarySortKey): Int = {
      if (this.first > other.first || (this.first == other.first && this.second > other.second)) {
        return 1;
      }
      else if (this.first < other.first || (this.first == other.first && this.second < other.second)) {
        return -1;
      }
      return 0;
    }
  }

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
//    val weeks=List("2017_18","2017_19")
//    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate from traffic where refer_host <> '3' and ddate>='20170506'  and  ddate<='20170507'  ").rdd
    val weeks=List("2017_10","2017_11","2017_12","2017_13","2017_14","2017_15","2017_16","2017_17")
    val selectRdd: RDD[Row] = sparkSession.sql("select suv,newuv,refer_domain,search,ddate from traffic where refer_host <> '3' and  ddate>='20170305'  and  ddate<='20170429'  ").rdd
    selectRdd.count()// 4月=156875889   3-4月=251568780

    //2添加来源字段| 格式化时间字段为周|第一周来的用户全部当中新用户   List[(suv,(refer_type,周，isnew))]
    val formatRdd: RDD[(String, (String , String, String))] = selectRdd.map(r => {
      val suv = r(0).toString
      var newuv = r(1).toString
      val refer_type = LogFormat.getReferType(r(2).toString, r(3).toString)
      val ddate = TimeUtil.getWeekFromData(r(4).toString)
      if(ddate==weeks(0)) newuv="1"

      (suv,(refer_type, ddate , newuv))
    }).distinct()//persist(StorageLevel.DISK_ONLY)  7513431

    //3聚合用户的来访轨迹(suv,List[(refer_type,周，isnew)])
    //4 用户分组内 按照isnew排序 (suv,List[(refer_type,周，isnew)])
    val sortUvGroup: RDD[(String, List[(String, String, String)])] = formatRdd.groupByKey().map(g => {
      val suv = g._1
      val sortList = g._2.toList.sortBy(x => x._3).reverse
      (suv, sortList)
    }).cache()
    println("sortUvGroup has UV size="+sortUvGroup.count())//   35831015 55624346



    //5 过滤出要对比的那一周的留存率  比如要算第2周新用户  在3,4,5周的留存
    // 排序后的List 取第一个，【如果周==第2周】&&【isnew==1】    是就说明这个用户是第二周来的  suv,List[(refer_type,周，isnew)
     val resSset:SSet[(String,String,String, Int)] = SSet()//[fromWeek ,toWeek,ref,count)]

    for(x<-  0 until  weeks.length){

      val w=weeks(x)
      val newUvGroup: RDD[(String, List[(String, String, String)])] = sortUvGroup.filter(f => {
        val uvWeek = f._2.head._2
        val uvIsNew = f._2.head._3
        uvWeek == w && uvIsNew == "1"
      })
      println("filter week="+w+" new UV size="+newUvGroup.count())

      //(refer_type,周),1
      val refer_week_rdd: RDD[((String, String),Int)] = newUvGroup.flatMap(r => {
        val ref_week: List[((String, String),Int)] = r._2.map(x => { ((x._1, x._2),1)  }).distinct
        ref_week
      })
      val refer_week_count: RDD[((String, String), Int)] = refer_week_rdd.reduceByKey(_+_)

      val asMap: Map[(String, String), Int] = refer_week_count.collectAsMap()//(直接来源,2017_19) -> 9565
      for (elem <- asMap) {
        val ref=elem._1._1  ;val fromWeek=w  ;val toWeek=elem._1._2  ;val count=elem._2
        resSset.add((fromWeek,toWeek,ref,count))//[fromWeek ,toWeek,ref,count)]
      }
    }




    println("report=====SSet[(String,String,String, Int)] [fromWeek ,toWeek,ref,count)]===二次排序= ==有部分数据有问题比如 上一周还是老用户的这一周变成新用户了 要去除===================")
    val sortRes: SSet[(SecondarySortKey, String)] = resSset.flatMap(r => {
      val fromWeek = r._1
      val toWeek = r._2
      val ref = r._3
      val count = r._4
      val key = new SecondarySortKey(fromWeek, toWeek)

      if (fromWeek > toWeek) None
      else Some(key, (ref + "," + fromWeek + "," + toWeek + "," + count))
    })
    val resOk: List[String] = sortRes.toList.sortBy(x=>x._1).map(i=>i._2)
    sparkSession.sparkContext.parallelize(resOk,1).saveAsTextFile("hdfs:/tmp/hugsh/laoqu/RRate-8week")

    //    selectRdd.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/RetiontoinRateAll")
  }

}
//filter week=2017_10 new UV size=7827261
//filter week=2017_11 new UV size=1473185
//filter week=2017_12 new UV size=1674683
//filter week=2017_13 new UV size=7530168
// filter week=2017_14 new UV size=8984217
// filter week=2017_15 new UV size=7510512
// filter week=2017_16 new UV size=2697776
// filter week=2017_17 new UV size=6808325

//[ 14:36:15-root@10-59-66-102:RRate-4week ]#cat part-00000 |grep 直接来源
//直接来源,2017_14,2017_14,3432645
//直接来源,2017_14,2017_15,424057
//直接来源,2017_14,2017_16,216469
//直接来源,2017_14,2017_17,308733
//直接来源,2017_15,2017_15,2003475
//直接来源,2017_15,2017_16,33996
//直接来源,2017_15,2017_17,40574
//直接来源,2017_16,2017_16,789721
//直接来源,2017_16,2017_17,19034
//直接来源,2017_17,2017_17,1992928
//[ 14:36:55-root@10-59-66-102:RRate-4week ]#cat part-00000 |grep 外部来源
//外部来源,2017_14,2017_14,2643128
//外部来源,2017_14,2017_15,140363
//外部来源,2017_14,2017_16,61636
//外部来源,2017_14,2017_17,87719
//外部来源,2017_15,2017_15,1692263
//外部来源,2017_15,2017_16,14351
//外部来源,2017_15,2017_17,16825
//外部来源,2017_16,2017_16,523451
//外部来源,2017_16,2017_17,7812
//外部来源,2017_17,2017_17,1176658
//[ 14:37:56-root@10-59-66-102:RRate-4week ]#cat part-00000 |grep 搜索引擎
//搜索引擎,2017_14,2017_14,7062222
//搜索引擎,2017_14,2017_15,818646
//搜索引擎,2017_14,2017_16,337144
//搜索引擎,2017_14,2017_17,550885
//搜索引擎,2017_15,2017_15,3606715
//搜索引擎,2017_15,2017_16,83399
//搜索引擎,2017_15,2017_17,111257
//搜索引擎,2017_16,2017_16,1310540
//搜索引擎,2017_16,2017_17,51033
//搜索引擎,2017_17,2017_17,3463519
//[ 14:37:59-root@10-59-66-102:RRate-4week ]#cat part-00000 |grep 站内来源
//站内来源,2017_14,2017_14,4207260
//站内来源,2017_14,2017_15,663666
//站内来源,2017_14,2017_16,329870
//站内来源,2017_14,2017_17,482946
//站内来源,2017_15,2017_15,2186495
//站内来源,2017_15,2017_16,52889
//站内来源,2017_15,2017_17,64579
//站内来源,2017_16,2017_16,793919
//站内来源,2017_16,2017_17,31743
//站内来源,2017_17,2017_17,2093236
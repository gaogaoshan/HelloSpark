package cn.itcast.spark.work2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Map, mutable}
import scala.collection.mutable.{ Map => MMap, Set => SSet}

object ModuleIntersection {

  val groupList: List[String] = AdsCodes.adsClassMap.values.flatMap(x=>x).toList


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQLDemo").setMaster("local[2]")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

// =======================================================================================================================================
    //读取数据文件 hdfs:/logs/logapi/113/ddate=20170424 ||  hdfs:/logs/logformat/113/dt=20170424* ||   /hdfs/logs/logapi/113/dt=20170424
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170307*").createOrReplaceTempView("113_log")
    val fromNewGameRdd=sqlContext.sql("select uv,ads_code from 113_log where ref_url ='http://newgame.17173.com/' ").rdd


//  2017年3月7日 捞取newgame.17173.com首页模块用户交
    //1.过滤= 筛选出来自newgame.17173.com的点击数据    ref_url ==newgame.17173.com
    //2.格式化数据  （ads_code,uv）变成（ads_code,Set(uv)） 变成单个元素的Set方便后面 根据ads_code做聚合
    //3.分组= 按照ads_code分组reduce ，Set(uv)做聚合
    //4.过滤= 筛选出需要统计的模块
    //5.聚合= 将模块聚合成大模块
            //比如adsGroup1包含【2016_新游首页_端游入库、2016_新游首页_手游入库 、2016_新游首页_VR入库】
            //adsGroup1,List[uv1,uv2,uv3....]
    //6.每个分组两两求交集


    //2.格式化数据  记录格式（ads_code,uv）  这里用set是为了下一步reduce 时候两个Set做合并，（如果只是String，没办法做累加）
    val gameRdd: RDD[(String, Set[String])] = fromNewGameRdd.map(m => (m(1).toString, Set(m(0).toString)))
    println("from newgame.17173  count="+gameRdd.count())//count()=9421

    //3.分组  记录格式=ads,Set(uv1,uv2,uv3)
    val adsGroup: RDD[(String, Set[String])] = gameRdd.reduceByKey((x,y)=>x++y)
    println("has group(adsCode) count=")//count()=171

    //4.过滤 过滤掉不需要统计的模块
    val filterAdsGroup: RDD[(String, Set[String])] = adsGroup.filter(f => {
      val code: String = f._1
      groupList.contains(code)
    })


    //5 聚合   将18个模块 聚合成 8个分类
    //聚合后的8个分类 放在这个ads8ClassMap里面
    val ads8ClassMap:MMap[String,SSet[String]]=MMap[String,SSet[String]]()

    val ads18Map: Map[String, Set[String]] = filterAdsGroup.collectAsMap()
    println("group count after filter="+ads18Map.size)//count()=14

    ads18Map.foreach(m=>{
      val code:String=m._1
      val uvSet:Set[String]=m._2
      println("code="+code+" |count="+uvSet.size)

      var className:Option[String]=AdsCodes.getClassNameByCode(code)

      className match {
        case  Some(name)=>{
          ads8ClassMap.getOrElseUpdate(className.toString, SSet()).++=(uvSet)
          println("after "+code+" add into "+className+", size="+ads8ClassMap.get(className.toString).size)
        }
        case  None=>println(code+" no in dic adsClassMap")
      }

//      if(className.is)

    })


//      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113-02")//  /hdfs/tmp/hugsh/laoqu


  }



}

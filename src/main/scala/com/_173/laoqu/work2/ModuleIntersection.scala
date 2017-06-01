package com._173.laoqu.work2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Map}
import scala.collection.mutable.{Map => MMap, Set => SSet}

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
    //读取数据文件 module=hdfs:/logs/logapi/113/ddate=20170424 ||   logapi_113= hdfs:/logs/logformat/113/dt=20170424* ||   /hdfs/logs/logapi/113/dt=20170424
    val parquetFile=sqlContext.read.parquet("hdfs:/logs/logapi/113/*20170307*").createOrReplaceTempView("113_log")
    val fromNewGameRdd=sqlContext.sql("select uv,ads_code from 113_log where url ='http://newgame.17173.com/' ").rdd


//  2017年3月7日 捞取newgame.17173.com首页模块用户交集
    //1.过滤= 筛选出在newgame.17173.com的点击数据    url ==newgame.17173.com
    //2.格式化数据  （ads_code,uv）变成（ads_code,Set(uv)） 变成单个元素的Set方便后面 根据ads_code做聚合
    //3.分组= 按照ads_code分组reduce ，Set(uv)做聚合
    //4.过滤= 筛选出需要统计的模块
    //5.聚合= 将模块聚合成大模块
            //比如adsGroup1包含【2016_新游首页_端游入库、2016_新游首页_手游入库 、2016_新游首页_VR入库】
            //adsGroup1,List[uv1,uv2,uv3....]
    //6.每个分组两两求交集





    //2.格式化数据  记录格式（ads_code,uv）  这里用set是为了下一步reduce 时候两个Set做合并，（如果只是String，没办法做累加）
    val gameRdd: RDD[(String, Set[String])] = fromNewGameRdd.map(m => (m(1).toString, Set(m(0).toString)))
    println("from newgame.17173  count="+gameRdd.count())//count()=23319


    //3.分组  记录格式=ads,Set(uv1,uv2,uv3)
    val adsGroup: RDD[(String, Set[String])] = gameRdd.reduceByKey((x,y)=>x++y)
    println("has group(adsCode) count="+adsGroup.count())//count()=55

    //4.过滤 过滤掉不需要统计的模块
    val filterAdsGroup: RDD[(String, Set[String])] = adsGroup.filter(f => {
      val code: String = f._1
      groupList.contains(code)
    })
    val ads17Map: Map[String, Set[String]] = filterAdsGroup.collectAsMap()
    filterAdsGroup.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/ads17Map")
      println("group count after filter="+ads17Map.size)//count()=17
      ads17Map.foreach(_17m=>{ println("code="+_17m._1+" codeName="+AdsCodes.adsCodeMap.get(_17m._1)+" |count="+_17m._2.size) })
      println("17Codes has uvs="+ads17Map.flatMap(x => x._2).toList.length)

    //5 聚合  17个模块 聚合后的8个分类 放在这个ads8ClassMap里面
    val ads8ClassMap:MMap[String,SSet[String]]=MMap[String,SSet[String]]()
    ads17Map.foreach(m=>{
      val code:String=m._1
      val uvSet:Set[String]=m._2
      val className:Option[String]=AdsCodes.getClassNameByCode(code)

      className match {
        case  Some(name)=>{
          ads8ClassMap.getOrElseUpdate(className.get, SSet()).++=(uvSet)
          println("after "+code+"-"+AdsCodes.adsCodeMap.get(code)+" add into "+className+", size="+ads8ClassMap.get(className.get).get.size)
        }
        case  None=>println(code+" no in dic adsClassMap")
      }
    })
    ads8ClassMap.foreach(_8c=>{ println("className="+_8c._1+" |count="+_8c._2.size) })



    //6.每个分组两两求交集
    val ads8ClassList: List[(String, SSet[String])] = ads8ClassMap.toList
    println(ads8ClassList.length)

    for(i<-  0 until  ads8ClassList.length){

      val className=ads8ClassList(i)._1
      val uvSet=ads8ClassList(i)._2
      println(className +"  开始求交集")

      for(x<- (i+1) until ads8ClassList.length){
        var className_tmp=ads8ClassList(x)._1
        var uvSet_tmp=ads8ClassList(x)._2
        val interSet=uvSet.intersect(uvSet_tmp)
        println("   "+className +" intersect " +className_tmp +" result="+interSet.toBuffer.size)
      }
    }

//      mapRusult.saveAsTextFile("hdfs:/tmp/hugsh/laoqu/113-02")//  /hdfs/tmp/hugsh/laoqu


  }



}

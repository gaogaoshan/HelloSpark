package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取出学科点击前三的
  * Created by root on 2016/5/16.
  */
object UrlCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("D:\\itcast.log").map(line => {//20160321102225	http://java.itcast.cn/java/course/javaee.shtml
      val f = line.split("\t")
      (f(1), 1)//  (http://java.itcast.cn/java/course/javaee.shtml,1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)//  (http://java.itcast.cn/java/course/javaee.shtml,3)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2) //(java.itcast.cn,http://java.itcast.cn/java/course/javaee.shtml , 3)
    })

    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3)
    })

    println(rdd4.collect().toBuffer)
    sc.stop()

  }
}

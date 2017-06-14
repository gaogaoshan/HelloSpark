package cn.itcast.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import Array._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by Administrator on 2017/4/27.
  */
object Test {

  /**
    * //    val arr = Array("tom","rain","jack","mike")  take3(arr)
    * @param arry
    * @return
    */
  def take3(arry:Array[String]) ={
    var _take3: List[String] = List.empty

    for (i <-arry.zipWithIndex){
      if(i._1=="rain"){

        var index:Int=i._2//当前索引后取后3个
        var tmp=for(step <- 1 to 3; if(index + step < arry.length) )
            yield arry(index + step)

        _take3=tmp.toList
      }
    }
    _take3
  }

  def main(args: Array[String]): Unit = {
    val arr = Array("dd","rain","ww","jack")
    var a=take3(arr)
    println(a.toBuffer)
  }
}

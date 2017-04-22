package cn.itcast.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/16.
  */
object UserLocation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoblieLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("c://bs.log").map(x => {//18611132889,20160327180000,16030401EAFB68F1E3CDF819735E1C66,0
      val arr = x.split(",")
      val mb = (arr(0),arr(2))
      val flag = arr(3)
      var time = arr(1).toLong
      if (flag == "1") time = -time
      (mb, time)                    // (18611132889,16030401EAFB68F1E3CDF819735E1C66) ,20160327180000  -------   （ 手机号+基站），时间
    })
    val rdd2 = rdd1.reduceByKey(_+_)// (18611132889,16030401EAFB68F1E3CDF819735E1C66) ,60 -------   （ 手机号+基站），手机基站总时间

    val rdd3 = sc.textFile("c://loc_info.txt").map(x => {     //CC0710CC94ECC657A8561DE549D940E0,116.303955,40.041935,6
      val arr = x.split(",")
      val bs = arr(0)
      (bs, (arr(1), arr(2)))                                   //(CC0710CC94ECC657A8561DE549D940E0 ,(116.303955,40.041935,6))  -------   基站,经纬度
    })

    //val rdd4 = rdd2.map(t => (t._1._2, (t._1._1, t._2)))      //16030401EAFB68F1E3CDF819735E1C66,(18611132889,60)    -------   基站，(手机,基站总时间)
    //val rdd5 = rdd4.join(rdd3)                                //16030401EAFB68F1E3CDF819735E1C66,((18611132889,60),(116.303955,40.041935,6))

  //                            [手机,基站,时间]      以手机分组
    val rdd6 = rdd2.map(t => (t._1._1, t._1._2, t._2)).groupBy(_._1).values.map(it => {
      it.toList.sortBy(_._3).reverse
    })
    println(rdd6.collect.toBuffer)
  }
}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by GaozhiSoft on 2017/4/22 0022.
  */
object Test {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val mbt = sc.textFile("c://bs.log").map( line => {//18611132889,20160327180000,16030401EAFB68F1E3CDF819735E1C66,0
      val fields = line.split(",")
      val eventType = fields(3)
      val time = fields(1)
      val timeLong = if(eventType == "1")  -time.toLong else time.toLong
      (fields(0) + "_"  + fields(2), timeLong)      // 18611132889_16030401EAFB68F1E3CDF819735E1C66,20160327180000
    })

    val rdd1 = mbt.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2))//(18611132889,16030401EAFB68F1E3CDF819735E1C66) ,60 ----- （ 手机号+基站），手机基站总时间
    val rdd1_1 = mbt.reduceByKey(_+_)// (18611132889,16030401EAFB68F1E3CDF819735E1C66) ,60 -------   （ 手机号+基站），手机基站总时间



    println(rdd1.collect().toBuffer)
    println(rdd1_1.collect().toBuffer)
    sc.stop()

  }
}

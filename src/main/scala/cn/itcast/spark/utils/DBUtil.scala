package cn.itcast.spark.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.Map

/**
  * Created by Administrator on 2017/5/5.
  */
object DBUtil {

//  Oracle 示例：
//  Class.forName("oracle.jdbc.driver.OracleDriver")
//  var theConf = new SparkConf().setAppName("testRDDMethod").setMaster("local")
//  var theSC = new SparkContext(theConf)
//  var theSC2 = new SQLContext(theSC)
//  var theJdbcDF = theSC2.load("jdbc",Map("url"->"jdbc:oracle:thin:用户/密码@//ip地址:端口/实例名",
//    "dbtable" -> "(select * from tab) a","driver"->"oracle.jdbc.driver.OracleDriver"))
//  theJdbcDF.registerTempTable("myuser")
//  var theDT = theSC2.sql("select * from myuser")
//  theDT.registerTempTable("tempsum")
//  2）MySQL示例：
//  Class.forName("com.mysql.jdbc.Driver")
//  var theConf = new SparkConf().setAppName("testRDDMethod").setMaster("local")
//  var theSC = new SparkContext(theConf)
//  var theSC2 = new SQLContext(theSC)
//  var theJdbcDF = theSC2.load("jdbc",Map("url"->"jdbc:mysql://ip地址:端口/mysql?user=XXXX&password=XXXX","dbtable" -> "要操作的表"))
//  theJdbcDF.registerTempTable("myuser")
//  var theDT = theSC2.sql("select * from myuser where b>2")
//  theDT.registerTempTable("tempsum")

  //export SPARK_CLASSPATH= /tmp/hugsh/xxxconnectot.jar
//  http://blog.csdn.net/qq_14950717/article/details/51323679


  val driver="oracle.jdbc.driver.OracleDriver"//oracle.jdbc.driver.OracleDriver  com.mysql.jdbc.Driver
  val url = "jdbc:oracle:thin:@10.59.67.79:1521:LOG"
  val userName = "web_stat"
  val password = "web20080522"
  val sql="select A_CODE, A_NAME from STAT_ADV "
  val tableName="STAT_ADV";//select fiels from sourceDBName  可以直接在这边过滤查询数据


def  getAdsCode_Name(sqlContext: SQLContext): Map[String, String] ={

  val jdbcDF = sqlContext.read.format("jdbc").options(Map(
    "driver" -> driver, "url" -> url, "user" -> userName, "password" -> password,
    "dbtable" -> tableName)).load()//“numPartitions” ->”5”,”partitionColumn”->”OBJECTID”,”lowerBound”->”0”,”upperBound”->”80000000”

//  jdbcDF.take(3)

  jdbcDF.createOrReplaceTempView("STAT_ADV")
  val adsCodeName: RDD[(String, String)] = sqlContext.sql(sql).rdd.map(r => (r(0).toString, r(1).toString))

  adsCodeName.collectAsMap()
}



//  def getAdsName(sc:SparkContext): Unit ={
//
//    val connection = () => {
//      Class.forName(driver).newInstance()
//      DriverManager.getConnection(url, userName, password)
//    }
//    val jdbcRDD = new JdbcRDD(
//      sc,
//      connection,
//      sql,
//      lowerBound =Long.MinValue, upperBound =Long.MaxValue, numPartitions =2,
//      r => {
//        val id = r.getString(1)
//        val code = r.getString(2)
//        (id, code)
//      }
//    )
//    val jrdd = jdbcRDD.collect()
//    jrdd.take(4)
//  }






  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQLDemo").setMaster("local[2]")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val adsCodeName: Map[String, String] = getAdsCode_Name(sqlContext)
  }

}
package cn.itcast.spark.utils

/**
  * Created by Administrator on 2017/5/19.
  */
object SecondarySortUtil  extends Serializable{

  class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
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
    var l:List[String]=List("20,30","20,10","30,40","30,50")

    val pairRDD: List[(SecondarySortKey, String)] = l.map(line => {
      val splited = line.split(",")
      val key = new SecondarySortKey(splited(0).toInt, splited(1).toInt)
      (key, line)
    })

    val sorted = pairRDD.sortBy(x=>x._1)
    val result = sorted.map(item => item._2)
    result.foreach(println)

  }
}

package cn.itcast.spark.utils
import scala.util.control.Breaks._
/**
  * Created by Administrator on 2017/5/8.
  */
object ArrUtil {

  /**
    *当前索引后取后3个
    * @param arry List[(url,referdomain,svn),.....]
    * @return
    */
  def take3(arry: List[(String, String, String)] ,refDomain:String, domain:String):List[(String, String, String)] ={
    var _take3: List[(String, String, String)] = List.empty

    breakable(
      for (i <-arry.zipWithIndex){
        if(i._1._1.contains(domain)  && i._1._2.contains(refDomain) ){

          val index:Int=i._2
          val tmp=for(step <- 1 to 3; if(index + step < arry.length) ) yield arry(index + step)

          _take3=tmp.toList
          break()
        }
      }
    )
    _take3
  }


  /**
    *
    * @param arry (ads_code,url,ref_url，svn)
    * @param refDomain
    * @param domain
    * @return
    */
  def take3_2(arry: List[(String, String,String, String)] ,refDomain:String, domain:String):List[(String, String, String,String)] ={
    var _take3: List[(String, String,String, String)] = List.empty

    breakable(
      for (i <-arry.zipWithIndex){
        if( i._1._2.contains(domain)  &&  i._1._3.contains(refDomain) ){

          val index:Int=i._2
          val tmp=for(step <- 1 to 3; if(index + step < arry.length) ) yield arry(index + step)

          _take3=tmp.toList
          break()
        }
      }
    )
    _take3
  }

  def main(args: Array[String]): Unit = {

    val arr1: List[(String, String, String)] = List(("url1", "baidu", "1"), ("url2", "17173", "2"), ("url3", "17173", "3"),
      ("url4", "17173", "4"),("url5", "17173", "5"),("url6", "17173", "6"))
    val arr2: List[(String, String, String)] = List(("url11", "baidu", "1"), ("url12", "cc", "2"), ("url13", "baidu", "3"))
    val arrs = List(arr1,arr2)
    val r=arrs.flatMap(x=>{
      val _3take: List[(String, String, String)] = take3(x,"17173","yeyou")

      if (!_3take.isEmpty) {
        val sid_urls = _3take.map(x => x._1.toString) //只取ads_code
        Some(("sid","suv"), sid_urls.zipWithIndex)// Array[((String,String), List[(String, Int)])]=Array((ssid,suv）, List((ads1,1), (ads2,2), (ads3,3))))
      } else
        None
    })
    print("vvvv="+r.toBuffer)
  }
}

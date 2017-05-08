package cn.itcast.spark.utils

/**
  * Created by Administrator on 2017/5/8.
  */
object ArrUtil {

  /**
    *当前索引后取后3个
    * @param arry List[(url,referdomain,svn),.....]
    * @return
    */
  def take3(arry: List[(String, String, String)] ,refDomain:String) ={
    var _take3: List[(String, String, String)] = List.empty

    for (i <-arry.zipWithIndex){
      if(i._1._2.contains(refDomain)){

        var index:Int=i._2
        var tmp=for(step <- 1 to 3; if(index + step < arry.length) ) yield arry(index + step)

        _take3=tmp.toList
      }
    }
    _take3
  }


  def take3FromArrayString(arry:Array[String]) ={
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

//    val arr1=Array("tom","rain","jack","mike")
//    val arr2=Array("tom2","rain2","jack2","mike2")
//    val arrs = Array(arr1,arr2)
//    var r=arrs.flatMap(x=>{//flat能去掉空值
//      take3FromArrayString(x)
//    })
//    print(r.toBuffer)
  }
}

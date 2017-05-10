package cn.itcast.spark.work2

import scala.collection.mutable.{Map => MMap, Set => SSet}
/**
  * Created by Administrator on 2017/5/10.
  */
object AdsCodes {

  val adsClassMap=Map(
    "g1测试"->List("ZvQBbm","Nj2IJr","yQZBvq"),
    "g2入库"->List("63YJZr","fIry6v","jM77nu"),
    "g3找游戏"->List("UnU3Ij"),
    "g4游戏试玩"->List("IrmyQj"),
    "g5图集"->List("buuqye"),
    "g6视频"->List("BjQRF3"),
    "g7热门"->List("vuui6j","vYVZ3u","qYJnma"),
    "g8榜单"->List("If6zAf","7FjUji","AnUVRn","AFvuQz","Q3qmIz")
  )

  var adsCodeMap=Map(
    "ZvQBbm"->"2016_新游首页_近期测试"  ,  "Nj2IJr"->"2016_新游首页_端游测试表" ,  "yQZBvq"->"2016_新游首页_手游测试表"  ,
    "63YJZr"->"2016_新游首页_端游入库"  ,  "fIry6v"->"2016_新游首页_手游入库" ,    "jM77nu"->"2016_新游首页_VR入库"  ,

    "UnU3Ij"->"2016_新游首页_找游戏"  ,
    "IrmyQj"->"2016_新游首页_游戏试玩"  ,
    "buuqye"->"2016_新游首页_图集"  ,
    "BjQRF3"->"2016_新游首页_视频"  ,

    "vuui6j"->"2016_新游首页_热门端游"  ,"vYVZ3u"->"2016_新游首页_热门手游" ,   "qYJnma"->"2016_新游首页_热门VR"  ,
    "If6zAf"->"2016_新游首页_手游期待榜"  ,"7FjUji"->"2016_新游首页_端游期待榜" ,"AnUVRn"->"2017_手游网首页_手游热门榜"  ,"AFvuQz"->"2016_新游首页_VR热门榜"  ,"Q3qmIz"->"2016_新游首页_VR期待榜"
  )

  /**
    * 根据adsCode 找到 他的分组
    * @param code
    * @return
    */
  def getClassNameByCode(code:String): Option[String] ={
    var continue:Boolean=true
    var className:Option[String]=None

    for (elem <- adsClassMap if continue) {
      if (elem._2.contains(code)){
        className=Some(elem._1)
        continue=false
      }
    }

    className
  }

  def main(args: Array[String]): Unit = {
    //val groupList: List[String] = adsClassMap.values.flatMap(x=>x).toList
    val className:Option[String]=None

    val ss=Set(4).toSet

    val a=MMap("c"->SSet(1,2),"zzz"->SSet(1,2, 1))

    val b = a.getOrElseUpdate("zzz", ss).++=(Set(999,7))
    println(a)
  }


}
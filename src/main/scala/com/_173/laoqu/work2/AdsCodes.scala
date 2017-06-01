package com._173.laoqu.work2

import scala.collection.mutable.{Map => MMap, Set => SSet}
/**
  * Created by Administrator on 2017/5/10.
  */
object AdsCodes extends Serializable{

  val adsClassMap=Map(
    "g1测试"->List("ZvQBbm","Nj2IJr","yQZBvq"),
    "g2入库"->List("63YJZr","fIry6v","jM77nu"),
    "g3找游戏"->List("UnU3Ij"),
    "g4游戏试玩"->List("IrmyQj"),
    "g5图集"->List("buuqye"),
    "g6视频"->List("BjQRF3"),
    "g7热门"->List("vuui6j","vYVZ3u","qYJnma"),
    "g8榜单"->List("If6zAf","7FjUji","AFvuQz","Q3qmIz")
  )

  var adsCodeMap=Map(
    "ZvQBbm"->"2016_新游首页_近期测试"  ,  "Nj2IJr"->"2016_新游首页_端游测试表" ,  "yQZBvq"->"2016_新游首页_手游测试表"  ,
    "63YJZr"->"2016_新游首页_端游入库"  ,  "fIry6v"->"2016_新游首页_手游入库" ,    "jM77nu"->"2016_新游首页_VR入库"  ,

    "UnU3Ij"->"2016_新游首页_找游戏"  ,
    "IrmyQj"->"2016_新游首页_游戏试玩"  ,
    "buuqye"->"2016_新游首页_图集"  ,
    "BjQRF3"->"2016_新游首页_视频"  ,

    "vuui6j"->"2016_新游首页_热门端游"  ,"vYVZ3u"->"2016_新游首页_热门手游" ,   "qYJnma"->"2016_新游首页_热门VR"  ,
    "If6zAf"->"2016_新游首页_手游期待榜"  ,"7FjUji"->"2016_新游首页_端游期待榜"  ,"AFvuQz"->"2016_新游首页_VR热门榜"  ,"Q3qmIz"->"2016_新游首页_VR期待榜"
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

    val ads8Class=Map(
      "g1测试"->Set("g1_1","g1_2","g1_3"),
      "g2入库"->Set("g2_1","g1_3","g1_1"),
      "g3找游戏"->Set("g3_1"),
      "g4游戏试玩"->Set("g4_1"),
      "g5图集"->Set("g5_1"),
      "g6视频"->Set("g6_1"),
      "g7热门"->Set("g7_1","g1_22","g1_2"),
      "g8榜单"->Set("g8_1","g1_22","g1_3","g1_1")
    )
    val s: Option[Set[String]] = ads8Class.get("g7热门")
    println(s)
    println(s.get.size)

  }


}
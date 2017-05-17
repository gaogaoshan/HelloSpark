package cn.itcast.spark.utils

import scala.util.{Failure, Success, Try}

/**
  * Created by Administrator on 2017/5/17.
  */
object LogFormat {

  object ReferType {
    val through   = 1     //直接来源
    val search    = 2     //搜索引擎
    val innerLink = 3     //站内来源
    val outLink   = 4     //外部来源
  }

  val internalDomain_My=List("17173.com","sohu.com","37wanwan.com","shouyou.com","yeyou.com",
    "kfb126.com","cy.com","17173ie.com","1y.com","17173v.com",
    "kfb163.com","2kfb.com","173tuku.com","2p.com","doyo.cn",
    "bless-source.com","black-desert.com","ywshouyou.com","168kaifu.com","test.com",
    "kaifu1.com","doyo.com")

  //获取来源类型
  case class Refer(referType: Int, searchType:Int)
  def getReferType(referDomain: String, search: String, internalDomain: List[String]=internalDomain_My): Refer = {
    if (List("_", "0", "").contains(referDomain)) Refer(ReferType.through, -1)
    else {
      // 搜索引擎ID大于0即判断为搜索引擎
      val searchNum = Try(search.toInt) match {
        case Success(n) => n
        case Failure(_) => 0
      }

      if (searchNum > 0) Refer(ReferType.search, searchNum)
      else {
        //站内来源 外部来源

        referDomain.split("\\.").takeRight(2).mkString(".") match {
          case netRef @ _ if internalDomain.contains(netRef) => Refer(ReferType.innerLink, -1)
          case _ => Refer(ReferType.outLink, -1)
        }
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //dnf.17173.com=- lol.qq.com=-  www.baidu.com=1
    val referType: Refer = getReferType("bbs.17173.com","-")
    println(referType.referType)

    val referType2: Refer = getReferType("lol.qq.com","-")
    println(referType2.referType)

    val referType3: Refer = getReferType("www.baidu.com","1")
    println(referType3.referType)
  }

}

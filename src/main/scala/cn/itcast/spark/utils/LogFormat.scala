package cn.itcast.spark.utils

import scala.util.{Failure, Success, Try}

/**
  * Created by Administrator on 2017/5/17.
  */
object LogFormat extends Serializable{

  object ReferType {
    val through   = "直接来源"     //直接来源  1
    val search    = "搜索引擎"     //搜索引擎  2
    val innerLink = "站内来源"     //站内来源  3
    val outLink   = "外部来源"     //外部来源  4
  }

  val internalDomain_My=List("17173.com","sohu.com","37wanwan.com","shouyou.com","yeyou.com",
    "kfb126.com","cy.com","17173ie.com","1y.com","17173v.com",
    "kfb163.com","2kfb.com","173tuku.com","2p.com","doyo.cn",
    "bless-source.com","black-desert.com","ywshouyou.com","168kaifu.com","test.com",
    "kaifu1.com","doyo.com")

  //获取来源类型
  def getReferType(referDomain: String, search: String, internalDomain: List[String]=internalDomain_My): String = {
    if (List("_", "0", "").contains(referDomain)) ReferType.through
    else {
      // 搜索引擎ID大于0即判断为搜索引擎
      val searchNum = Try(search.toInt) match {
        case Success(n) => n
        case Failure(_) => 0
      }

      if (searchNum > 0) ReferType.search
      else {
        //站内来源 外部来源

        referDomain.split("\\.").takeRight(2).mkString(".") match {
          case netRef @ _ if internalDomain.contains(netRef) => ReferType.innerLink
          case _ => ReferType.outLink
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //dnf.17173.com=- lol.qq.com=-  www.baidu.com=1
    val referType = getReferType("bbs.17173.com","-")
    println(referType)
  }

}

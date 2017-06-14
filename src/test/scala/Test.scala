import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Map, mutable}
import scala.collection.mutable.{ListBuffer, Map => MMap, Set => SSet}
/**
  * Created by GaozhiSoft on 2017/4/22 0022.
  */
object Test {

def getStringFromList(l:List[String]): String ={
  var s:String=""
  l.foreach(x=>{
    s=s.concat(x).concat(",")
  })
  s
}



  def main(args: Array[String]): Unit = {

    val GiftGamePattern = """^http://hao.17173.com/sche-info-(\d+).html""".r
    val GiftInfoPattern = """^http://hao.17173.com/gift-info-(\d+).html""".r

    val web="182.140.184.85  http://dnf.17173.com/content/03282016/155607063_all.shtml       dnf.17173.com   /content/03282016/155607063_all.shtml   2977    352     1       10000000,10000144,10000183      CN5101  1490117241119830        0       m.baidu.com m.baidu.com     https://m.baidu.com/from=1000953f/bd_page_type=1/ssid=0/uid=0/pu=sz%40224_220%2Cta%40iphone_1_9.0_2_7.2%2Cusm%400/baiduid=E5E5F10727FF219A9C6964EF14F56EA9/w=0_10_/t=iphone/l=1/tc?ref=www_iphone&lid=10992782777676887214&order=8&fm=alop&waplogo=1&tj=www_normal_8_0_10_title&vit=osres&waput=3&cltj=normal_title&asres=1&title=%E5%B9%B3%E6%B0%91%E9%80%9F%E6%88%90%E6%88%98%E5%90%BC%E6%9E%81%E9%99%90%E5%A2%9E%E5%8A%A0%E6%94%BB%E5%87%BB%E5%8A%9B%E9%85%8D%E8%A3%85..._17173%E5%9C%B0%E4%B8%8B%E5%9F%8E%E4%B8%8E%E5%8B%87%E5%A3%AB&dict=-1&w_qd=IlPT2AEptyoA_ykzx4gcqvyxGlBPcYUn9jG&sec=19767&di=3343702b4e313a83&bdenc=1&tch=124.350.57.1465.0.0&nsrc=IlPT2AEptyoA_yixCFOxXnANedT62v3IIhiX_85DQ78h95qshbWxBdNpXCPqLmvTUS3ddzDWsx5ExmGdWWUj8BR0qaxisERulyH8tqSwrcm&eqid=988e35b1ac7678001000000258d3e9c9&wd=&clk_info=%7B%22srcid%22%3A%221599%22%2C%22tplname%22%3A%22www_normal%22%2C%22t%22%3A1490321619161%2C%22xpath%22%3A%22div-a-h3%22%7D \"Mozilla/5.0 (iPhone 5SGLOBAL; CPU iPhone OS 9_0 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 MQQBrowser/7.2 Mobile/13A344 Safari/8536.25 MttCustomUA/2 QBWebViewType/1\"  0       -1      iPhone%209.0    640x1136   32-bit   zh-cn   1       0       mq      7630    -       2017-03-24T10:13:33+08:00       149011724111983014903225726701211490321620581   1       3       2       0\n117.136.79.20   http://lol.17173.com/zhangmeng/lpl/20170324/qianzhan.shtml?r=0.7850355463789089 lol.17173.com   /zhangmeng/lpl/20170324/qianzhan.shtml?r=0.7850355463789089     146641  146640  1       10000000,10000144       CN4400  1490322207393120841 0       3       lol.qq.com      http://lol.qq.com/m/act/a20141225match/detail.shtml?id=1566&comment=1830679048&jl_articleid=29686&APP_BROWSER_VERSION_CODE=1&android_version=1&imgmode=auto     -       0       \"Mozilla/5.0 (Linux; Android 5.1; MX5 Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.49 Mobile MQQBrowser/6.2 TBS/043024 Safari/537.36lolapp/5.0.5.2395 lolappcpu/armeabi-v7a\"       0       -1      Android%205.1       360x640 32-bit  zh-cn   0       0       lolapp  0       -       2017-03-24T10:23:27+08:00       148507428733238214903234580871431490322206198   1       115     2       0"

    val listWeb: List[String] = web.split("\t").toList


    val split: Array[String] = web.split("\t")
    println(split.length)
  }
}

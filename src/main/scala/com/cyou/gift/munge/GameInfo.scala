/*
 * GameInfo.scala
 * Copyright (C) 2017 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */

package com.cyou.gift.munge

import scala.io._
import scala.util.Try

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.HttpResponse

//import main.models.Game

object GameInfo {

  implicit val formats = DefaultFormats

  def getRestContent(httpResponse: HttpResponse): String = {
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    content
  }

/*  def request(st: Long, et: Long, page: Long=1L, pageSize: Long = 100L): JValue = {
    // create an HttpPost object
    val post = new HttpPost("http://usap80.soa.internal.17173.com/gamelib/soa/Game")

    // set header
    post.setHeader("Content-type", "application/json")
    post.setHeader("soa-appId", "user_tag")
    post.setHeader("soa-appSign", "c45ecc2bb4b8d9bf2dfa62130b418cc7")

    // add the JSON as a StringEntity
    val playload = ("jsonrpc" -> "2.0") ~ ("id" -> 0) ~
      ("method" -> "searchInfoList") ~
      ("params" -> JArray(List(
        ("createTime" -> (("begin" -> st) ~ ("end" -> et))) ~
          ("page" -> page) ~ ("pageSize" -> pageSize)
      )))
    println(compact(render(playload)))
    post.setEntity(new StringEntity(compact(render(playload))))

    // send the post request
    val httpClient = new DefaultHttpClient
    val res = getRestContent(httpClient.execute(post))
    httpClient.getConnectionManager().shutdown()
    parse(res)
  }*/

  /*def proc(st: Long, et: Long, pageSize: Int = 100) {
    var offset = 1L
    var total = 0L

    do {
      val page = offset / pageSize + 1
      val data = request(st, et, page)
      val games: Seq[(String, String, String, String)] = for (
        x <- (data \ "result" \ "data").extract[Seq[JValue]]
      ) yield (
        (x \ "gameBase" \ "name").extract[String],
        (x \ "oldGameCode").extract[String],
        (x \ "kind").extract[String],
        (x \ "platform").extract[String]
      )
      Game.upsert(games)
      total = (data \ "result" \ "total").extract[Long]
      println("offset", offset, "page:", page, "page size:", pageSize, "total:", total)
      offset = offset + pageSize
    } while (offset < total)
  }*/

  def main(args: Array[String]) {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val st: Long = Try(format.parse(args(0)).getTime/1000).toOption.getOrElse(0L)
    val et: Long = Try(format.parse(args(1)).getTime/1000).toOption.getOrElse(0L)

    if (st.toInt == 0 || et.toInt == 0) {
      println("时间格式有误")
    } else {
//      proc(st, et)
    }
  }
}


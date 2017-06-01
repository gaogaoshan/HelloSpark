/*
 * CmsDB.scala
 * Copyright (C) 2016 n3xtchen <echenwen@gmail.com>
 *
 * Distributed under terms of the GPL-2.0 license.
 */

package com._173.gift.models

import com.typesafe.config.ConfigFactory
//import slick.driver.MySQLDriver.api._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

final case class GameItem(name: String, code: String, plat: String, kind: String, 
                          id: Long=0)

object Game {
//  class Games(tag:Tag) extends Table[GameItem](tag, "hao_game") {
//    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
//    def name = column[String]("game_name")
//    def code = column[String]("game_code")
//    def plat = column[String]("plat")
//    def kind = column[String]("kind")
//    def * = (name, code, plat, kind, id) <> (GameItem.tupled, GameItem.unapply)
//  }

  val dbConf = ConfigFactory.load().getConfig("db.usertag")

//  val db = Database.forURL(
//    dbConf.getString("conn"), dbConf.getString("user"),
//    dbConf.getString("pass"), driver="com.mysql.jdbc.Driver"
//  )
//  val games = TableQuery[Games]
//
//  def upsert(newGames: Seq[(String, String, String, String)]) {
//    db withSession {
//      implicit session => {
//        val actions = newGames.map(
//          g =>
//            games.insertOrUpdate(GameItem(g._1, g._2, g._3, g._4))
//          )
//        val create = DBIO.seq(actions: _*)
//        val run = db.run(create)
//        Await.result(run, Duration.Inf)
//      }
//    }
//  }
}


package com.project.canalTest

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.logging.log4j.scala.Logging

object runApp extends App with Logging {


  implicit val system = ActorSystem("MovieServiceSpec")
  implicit val materializer = ActorMaterializer

  // Example usage of principalsForMovieName method
  val principals = MovieServiceImpl.principalsForMovieName("Le clown et ses chiens")
  //val principals = MovieServiceImpl.principalsForMovieName("The Girl and the Press Photographer")

  principals.runForeach(principal => println(s"Principal: $principal"))

  // Example usage of tvSeriesWithGreatestNumberOfEpisodes method
  val tvSeries = MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes()
  tvSeries.runForeach(tvSerie => println(s"TV Series: $tvSerie"))

}

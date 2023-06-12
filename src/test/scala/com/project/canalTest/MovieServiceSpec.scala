package com.project.canalTest

import org.scalatest.flatspec.AnyFlatSpec
import akka.actor.ActorSystem
import akka.actor.Status.{Failure, Success}
import akka.event.jul.Logger
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.project.canalTest.Schema.{Principal, TvSeries}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Await
import scala.concurrent.duration._

class MovieServiceSpec extends AnyFlatSpec with Logging  {

  implicit val system = ActorSystem("MovieServiceSpec")
  implicit val materializer = Materializer(system)

  //val logger = LoggerFactory.getLogger(getClass)

  "principalsForMovieName" should "return a Source of Principal objects" in {
    logger.info("Testing principalsForMovieName method")
    val result = MovieServiceImpl.principalsForMovieName("Carmencita")
    assert(result.isInstanceOf[Source[Schema.Principal, _]])
  }

  it should "return the correct Principal object" in {
   logger.info("Testing principalsForMovieName method with specific input")
    val result = MovieServiceImpl.principalsForMovieName("Carmencita").runWith(Sink.collection)
    val expected = Seq(Principal("nm0000001", "Fred Astaire", Some(1899), Some(1987), List("soundtrack", "actor", "miscellaneous")),
                      Principal("nm0000002", "Lauren Bacall", Some(1924), Some(2014), List("actress","soundtrack")),
                      Principal("nm0000003", "Brigitte Bardot", Some(1934), None, List("actress","soundtrack","music_department"))
    )
    assert(Await.result(result, 3.seconds) == expected)
  }

  "tvSeriesWithGreatestNumberOfEpisodes" should "return a Source of TvSeries objects" in {
    logger.info("Testing tvSeriesWithGreatestNumberOfEpisodes method")
    val result = MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes()
    assert(result.isInstanceOf[Source[TvSeries, _]])
  }

  it should "return the correct TvSeries object" in {
    logger.info("Testing tvSeriesWithGreatestNumberOfEpisodes method with specific input")
    val result = MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes().runWith(Sink.seq)
    val expected = Seq(TvSeries("tt0000001", "Carmencita", Some(1894), None, List("Documentary", "Short")),
      TvSeries("tt0000002", "Le clown et ses chiens", Some(1892), None, List("Animation", "Short")),
      //TvSeries("tt0000003","Pauvre Pierrot",Some(1892),None,List("Animation", "Comedy", "Romance"))

    )
    assert(Await.result(result, 3.seconds) == expected)
  }
}


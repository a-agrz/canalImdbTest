package com.project.canalTest

import org.scalatest.flatspec.AnyFlatSpec
import akka.actor.ActorSystem
import akka.actor.Status.{Failure, Success}
import akka.event.jul.Logger
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Await
import scala.concurrent.duration._

class MovieServiceSpec extends AnyFlatSpec with Logging  {

  implicit val system = ActorSystem("MovieServiceSpec")
  implicit val materializer = Materializer(system)

  //val logger = LoggerFactory.getLogger(getClass)

  "principalsForMovieName" should "return a Source of Principal objects" in {
    logger.info("Testing principalsForMovieName method")
    val result = MovieServiceImpl.principalsForMovieName("Le clown et ses chiens")
    assert(result.isInstanceOf[Source[Schema.Principal, _]])
  }

  it should "return the correct Principal object" in {
   logger.info("Testing principalsForMovieName method with specific input")
    val result = MovieServiceImpl.principalsForMovieName("Le clown et ses chiens").runWith(Sink.seq)
    val expected = Seq(Schema.Principal("nm0000001", "Fred Astaire", Some(1899), Some(1987), List("soundtrack", "actor", "miscellaneous")),
                      Schema.Principal("nm0000002", "Lauren Bacall", Some(1899), Some(2014), List("actress","soundtrack")),
                      Schema.Principal("nm0000003", "Brigitte Bardot", Some(1934), None, List("actress","soundtrack","music_department")),
    )
    assert(Await.result(result, 3.seconds) == expected)
  }
}


package com.project.canalTest

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Source}
import com.project.canalTest.Schema._
import org.apache.logging.log4j.scala.Logging

import java.nio.file.Paths
import scala.util.Try

trait MovieService {
  def principalsForMovieName(name: String): Source[Schema.Principal, _]

  def tvSeriesWithGreatestNumberOfEpisodes(): Source[Schema.TvSeries, _]
}

object MovieServiceImpl extends MovieService with Logging {

  implicit val system = ActorSystem("MovieServiceSpec")
  implicit val materializer = ActorMaterializer

  val nameBasicsFile = "src/main/ressources/name.basics.test.tsv"
  val titleBasicsFile = "src/main/ressources/title.basics.test.tsv"
  val titlePrincipalsFile = "src/main/ressources/title.principals.test.tsv"
  val titleEpisodesFile = "src/main/ressources/title.episode.test.tsv"
  val size = 2

  override def principalsForMovieName(name: String): Source[Schema.Principal, _] = {
    logger.info(s"Getting principals for movie name: $name")

    // Create a source of TitleActor objects from the titlePrincipalsFile
    lazy val titleActor = tsvSource(titlePrincipalsFile)
      .map(x =>
        TitleActor(
          x.getOrElse("tconst", ""),
          x.getOrElse("nconst", "")
        ))

    // Create a source of Principal objects from the nameBasicsFile
    lazy val principals: Source[Principal, _] =
      tsvSource(nameBasicsFile)
        .map(x =>
          Principal(
            x.getOrElse("nconst", ""),
            x.getOrElse("primaryName", ""),
            x.get("birthYear").flatMap(x => Try(x.toInt).toOption),
            x.get("deathYear").flatMap(x => Try(x.toInt).toOption),
            x.getOrElse("primaryProfession", "").split(",").toList
          ))

    // Create a source of Title objects from the titleBasicsFile filtered by the provided name
    lazy val title: Source[Title, _] = tsvSource(titleBasicsFile, Some(TsvFilter("primaryTitle", name)))
      .map(x =>
        Title(
          x.getOrElse("tconst", ""),
          x.getOrElse("primaryTitle", "")
        ))

    // Combine the sources to get the principals for the provided title
    val principalsFortitle = title.flatMapConcat(title =>
      titleActor.collect {
        case titleActor if titleActor.tConst.toUpperCase() == title.tConst.toUpperCase() => titleActor
      })
      .flatMapConcat(actorTitle =>
        principals.collect {
          case principal if principal.nconst.toUpperCase() == actorTitle.nConst.toUpperCase() => principal
        })
    principalsFortitle
  }

  override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSeries, _] = {
    logger.info("Getting TV series with greatest number of episodes")

    // Create a source of TvSeries objects from the titleBasicsFile filtered by type "tvseries"
    lazy val tvSeries: Source[TvSeries, _] = tsvSource(titleBasicsFile)
      .filter(x => x.getOrElse("titleType", "") == "tvseries")
      .map(x =>
        TvSeries(
          x.getOrElse("tconst", ""),
          x.getOrElse("originalTitle", ""),
          x.get("startYear").flatMap(x => Try(x.toInt).toOption),
          x.get("endYear").flatMap(x => Try(x.toInt).toOption),
          x.getOrElse("genres", "").split(",").toList
        ))

    // Count the number of episodes for each TV series and return the top `size` results
    tsvSource(titleEpisodesFile)
      .map(x => x.getOrElse("parentTconst", "") -> 1)
      .groupBy(1000000, x => x._1)
      .reduce((x, y) => (x._1, x._2 + y._2))
      .mergeSubstreams
      .fold(Seq.empty[(String, Int)]) {
        case (acc, e) if acc.length < size => acc ++ Seq(e)
        case (acc, e) if !acc.filter(_._2 < e._2).isEmpty => acc.sortBy(_._2).tail ++ Seq(e)
        case (acc, e) => acc
      }
      .flatMapConcat(episode =>
        tvSeries.collect {
          case tvSerie if (!episode.filter(_._1.toUpperCase() == tvSerie.tconst.toUpperCase()).isEmpty) => tvSerie
        })
  }

  // Helper method to create a source from a TSV file with an optional filter
  def tsvSource(filePath: String, filter: Option[TsvFilter] = None) = {
    logger.info(s"Creating TSV source from file: $filePath with filter: $filter")
    val source = FileIO.fromPath(Paths.get(filePath))
    source
      .via(CsvParsing.lineScanner('\t'))
      .via(CsvToMap.toMapAsStrings())
      .filter(row =>
        if (filter.isDefined) row.getOrElse(filter.head.column, "").toUpperCase() == filter.head.value.toUpperCase()
        else true
      )
  }
}

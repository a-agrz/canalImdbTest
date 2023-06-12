package com.project.canalTest

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Source}
import com.project.canalTest.Schema.{Principal, Title, TitleActor, TvSeries}

import java.nio.file.Paths
import scala.concurrent.Future
import scala.util.Try

trait MovieService {
  def principalsForMovieName(name: String): Source[Schema.Principal, _]

  def tvSeriesWithGreatestNumberOfEpisodes(): Source[Schema.TvSeries, _]
}

object MovieServiceImpl extends MovieService {

  implicit val system = ActorSystem("MovieServiceSpec")
  implicit val materializer = ActorMaterializer
  val nameBasicsFile = "src/main/ressources/name.basics.test.tsv" //"src/main/ressources/name.basics.tsv"
  val titleBasicsFile = "src/main/ressources/title.basics.test.tsv" //"src/main/ressources/title.basics.tsv"
  val titlePrincipalsFile = "src/main/ressources/title.principals.test.tsv" //"src/main/ressources/title.principals.tsv"
  val titleEpisodesFile = "src/main/ressources/title.principals.test.tsv" //"src/main/ressources/title.principals.tsv"

  override def principalsForMovieName(name: String): Source[Schema.Principal, _] = {

    lazy val titleActor = tsvSource(titlePrincipalsFile)
      .map(x =>
        TitleActor(
          x.getOrElse("tconst", ""),
          x.getOrElse("nconst", "")
        ))

    lazy val principals: Source[Principal, _] =
    //tsvSource(nameBasicsFile,Some(TsvFilter("nconst", nconst)))
      tsvSource(nameBasicsFile)
        .map(x =>
          Principal(
            x.getOrElse("nconst", ""),
            x.getOrElse("primaryName", ""),
            x.get("birthYear").flatMap(x => Try(x.toInt).toOption),
            x.get("deathYear").flatMap(x => Try(x.toInt).toOption),
            x.getOrElse("primaryProfession", "").split(",").toList
          ))

    lazy val title: Source[Title, _] = tsvSource(titleBasicsFile, Some(TsvFilter("primaryTitle", name)))
      .map(x =>
        Title(
          x.getOrElse("tconst", ""),
          x.getOrElse("primaryTitle", "")
        ))

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

  def tsvSource(filePath: String, filter: Option[TsvFilter] = None) = {
    val source = FileIO.fromPath(Paths.get(filePath))
    source
      .via(CsvParsing.lineScanner('\t'))
      .via(CsvToMap.toMapAsStrings())
      .filter(row =>
        if (filter.isDefined) row.getOrElse(filter.head.column, "").toUpperCase() == filter.head.value.toUpperCase()
        else true
      )
  }

  override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSeries, _] = {
    lazy val tvSeries: Source[TvSeries, _] = tsvSource(titleBasicsFile)
      .filter(x => x.getOrElse("titleType","") == "tvseries")
      .map(x =>
        TvSeries(
          x.getOrElse("tconst", ""),
          x.getOrElse("original", ""),
          x.get("startYear").flatMap(x => Try(x.toInt).toOption),
          x.get("endYear").flatMap(x => Try(x.toInt).toOption),
          x.getOrElse("genre", "").split(",").toList
        ))

    tsvSource(titleEpisodesFile)
      .map(x => x.getOrElse("parentTcount", "") -> 1)
      .groupBy(8, x => x._1)
      .reduce((x, y) => (x._1, x._2 + y._2))
      .mergeSubstreams
      .fold(Seq.empty[(String, Int)]) {
        case (acc, e) if acc.length < 3 => acc ++ Seq(e)
        case (acc, e) if !acc.filter(_._2 < e._2).isEmpty => acc.sortBy(_._2).tail ++ Seq(e)
        case (acc, e) => acc
      }
      .flatMapConcat(episode =>
        tvSeries.collect {
          case tvSerie if (!episode.filter(_._1.toUpperCase() == tvSerie.tconst.toUpperCase()).isEmpty) => tvSerie
        })

  }

  case class TsvFilter(column: String, value: String)
}

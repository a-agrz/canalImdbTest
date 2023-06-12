package com.project.canalTest

import akka.stream.scaladsl.Source

object Schema {
  final case class Principal(nconst: String,
                             name: String,
                             birthYear: Option[Int],
                             deathYear: Option[Int],
                             profession: List[String]
                            )

  final case class TvSeries(tconst: String,
                            original: String,
                            startYear: Option[Int],
                            endYear: Option[Int],
                            genres: List[String]
                           )

  case class Title(tConst: String, primaryTitle: String)

  case class Episode(tConst: String, parentTConst: String, seasonNumber: Option[Int], episodeNumber: Option[Int])

  case class Ratings(tConst: String, averageRating: Double, numVotes: Int)

  case class TitleActor(tConst: String, nConst: String)

  case class TsvFilter(column: String, value: String)

}


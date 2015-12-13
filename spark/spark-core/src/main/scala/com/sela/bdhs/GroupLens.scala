package com.sela.bdhs

import org.apache.spark.{SparkContext, SparkConf}

object GroupLens {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
    val sc = new SparkContext(conf)

    val ratings = sc.textFile("/user/eyalb/ratings-fixed/*").map(ParseRowToRating).cache
    val movies = sc.textFile("/user/eyalb/movies-fixed/*").map(ParseRowToMovie).filter(x => x != null).cache

    val movie_ratings = ratings.map(r => (r.movieId, r.rating))
    val sum_count_result = movie_ratings.combineByKey(myCreateCombiner, myMergeValues, myMergeCombiners)

    //    val avg_result = sum_count_result.map(x => (x._1, (x._2._1 / x._2._2, x._2._2)))
    val avg_result = sum_count_result.map({
      case (movieId, (rating_sum, rating_count)) =>
        (movieId, (rating_sum / rating_count, rating_count))
    })

    //    val joined = movies.join(avg_result).map(x => (x._1, x._2._1.title, x._2._1.year, x._2._2._1, x._2._2._2))
    val joined = movies.join(avg_result).map({
      case (movieId, (movie, (avg_rating, cnt_rating))) =>
        (movie.movieId, movie.title, movie.year, avg_rating, cnt_rating)
    })

    //    val ready_for_print = joined.sortBy(x => x._4, false)
    val ready_for_print = joined.sortBy({
      case (movieId, title, year, avg_rating, cnt_rating) =>
        avg_rating
    }, false)

    joined.take(20).foreach(println)

    sc.stop
  }

  def myCreateCombiner(rating: Double) = {
    (rating, 1)
  }

  def myMergeValues(r: (Double, Int), value: Double) = {
    (r._1 + value, r._2 + 1)
  }

  def myMergeCombiners(r1: (Double, Int), r2: (Double, Int)) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def ParseRowToMovie(row: String): (Int, Movie) = {
    val split = row.split("\t")
    if (split(2) == null || split(2).isEmpty) null else (split(0).toInt, new Movie(split(0).toInt, split(1), split(2).toInt, split(3).split("\\|")))
  }

  def ParseRowToRating(row: String): Rating = {
    val split = row.split("\t")
    new Rating(split(0).toInt, split(1).toInt, split(2).toDouble, split(3).toLong)
  }
}

case class Movie(movieId: Int, title: String, year: Int, genres: Array[String])

case class Rating(userId: Int, movieId: Int, rating: Double, ts: Long)

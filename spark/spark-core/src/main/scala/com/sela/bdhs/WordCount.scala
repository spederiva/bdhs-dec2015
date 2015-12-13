package com.sela.bdhs

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/user/eyalb/book/input/*")
    val words = rdd.flatMap(line => line.split(" "))
    val filtered_words = words.filter(word => word != null && !word.isEmpty)
    val words_one = filtered_words.map(word => (word, 1))

    val word_count = words_one.reduceByKey((x, y) => x + y).sortBy({ case (word, count) => count}, false)

    //    word_count.collect.foreach({case (word, count) => println(s"$word has appeared $count times")})
    word_count.saveAsTextFile("/user/eyalb/book/spark-output")
  }
}

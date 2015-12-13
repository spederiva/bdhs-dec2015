package com.sela.bdhs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterRunner {
  def main(args: Array[String]) {
    // get these parameters from twitter developer console
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("CONSUMER_KEY", "CONSUMER_SECRET", "ACCESS_TOKEN", "ACCESS_TOKEN_SECRET")
    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf(true).setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, filters)

    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60: DStream[(Int, String)] = hashTags.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(ascending = false))

    val topCounts10: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(ascending = false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

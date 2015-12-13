package com.sela.bdhs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

object MarvelGraph {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).setAppName("Marvel Wikipedia PageRank")
    val sc = new SparkContext(conf)

    val wiki = sc.textFile("/user/eyalb/spark-data/marvel/marvel.csv")
    val tuples = wiki.map(_.split(',')).map(line => (line(0).toLong, line(1), line.slice(2, line.length).map(_.toLong))).cache

    val vertices = VertexRDD(tuples.map(t => (t._1, t._2)))
    val edges = tuples.flatMap(t => t._3.map(targetId => (t._1, targetId))).map(e => Edge(e._1, e._2, 0))

    val graph = Graph(vertices, edges)

    val ranks = graph.ops.pageRank(0.0001).vertices.cache

    val rankedVertices = vertices.join(ranks)

    rankedVertices.collect.sortBy(_._2._2).reverse.take(20).foreach(println)
  }
}
package com.sela.bdhs

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object SQLDemos {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).setAppName("SQL Demos")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    val df = sqlContext.read.json("/user/eyalb/spark-data/people/people.json")
    df.show
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()



    val people = sc.textFile("/user/eyalb/spark-data/people/people.txt")
    val schemaString = "name age"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.registerTempTable("people")
    val results = sqlContext.sql("SELECT name FROM people")
    results.map(t => "Name: " + t(0)).collect().foreach(println)


    val people2 = sc.textFile("/user/eyalb/spark-data/people/people.txt")
    import sqlContext.implicits._
    val people_df = people2.map(_.split(",")).map(x => new Person(x(0), x(1).trim.toInt)).toDF


    hiveContext.sql("SELECT * FROM grouplens.movies").collect.foreach(println)
  }
}

case class Person(name:String, age:Int)

package com.yinker.data.neo4j


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/12/20.
  */
object sortByTest {


    val LOG: Logger = Logger.getLogger(sortByTest.getClass)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("aa")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext


    var n = 0

    val a = sc.parallelize(Array(
      "1,1",
      "3,1",
      "6,1",
      "2,1",
      "8,1",
      "12,1",
      "51,1",
      "5,1",
      "7,1",
      "9,1"
    )).map(x => {
      val s = x.split(",")

      (s(0), s(1))
    })


      .sortByKey()
       .foreach(println)


  }
}

package com.yinker.data.neo4j

import org.apache.spark.sql.SparkSession


/**
  * Created by think on 2017/12/18.
  */
object JoinInfo {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .appName("aaa")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    val overDueInfo = sc.textFile(args(0))
      //      .parallelize(Array(
      //      "436242,weibo,15273428885,2,0,2017-11-1611:35:02.0,2017-11-23",
      //      "1896780,weibo,15861317233,29,0,2017-11-1414:30:03.0,2017-12-14",
      //      "1949457,weibo,18276108603,2,0,2017-10-3113:00:02.0,2017-11-30",
      //      "1949457,weibo,18276108603,2,0,2017-10-3113:00:02.0,2017-11-30",
      //      "1949457,weibo,18276108603,2,0,2017-10-3113:00:02.0,2017-11-31",
      //      "1949457,weibo,18276108603,2,0,2017-10-3113:00:02.0,2017-11-32",
      //      "1949457,weibo,18276108603,2,0,2017-10-3113:00:02.0,2017-11-33",
      //      "1949484,weibo,18702394580,0,4,2017-10-3110:05:02.0,2017-11-34",
      //      "1988796,weibo,18283815811,0,15,2017-10-3100:05:07.0,2017-11-30",
      //      "1988805,weibo,14780038877,4,0,2017-10-3100:05:09.0,2017-11-07",
      //      "1988814,weibo,15190408857,14,0,2017-10-3100:05:11.0,2017-11-30"
      //    ))
      .map(x => {
      val split = x.split(",")

      val userId = split(0)
      val channel = split(1)
      val mobile = split(2)
      val earlyDay = split(3).toInt
      val overDue = split(4).toInt
      (mobile, (userId, channel, earlyDay, overDue))
    }).distinct()


    val rankInfo = sc.textFile(args(1))
      //      .parallelize(Array(
      //      "15273428885,1.2",
      //      "15861317233,2.2",
      //      "18276108603,9.25",
      //      "18702394580,10.51",
      //      "18283815811,1.5",
      //      "14780038877,-8.1",
      //      "15190408857,-9.1"
      //    ))
      .map(x => {
      val split = x.split(",")
      val mobile = split(0)
      val score = split(1).toDouble

      (mobile, score)
    })
    val resRDD = overDueInfo.join(rankInfo).map(x => {
      val mobile = x._1
      val score = x._2._2
      val userId = x._2._1._1
      val channel = x._2._1._2
      val earlyDay = x._2._1._3
      val overDue = x._2._1._4

      (score, overDue)
    }).sortByKey(false).map(x=>{
      s"${x._1},${x._2}"
    })

    resRDD.saveAsTextFile(args(2))


  }


}

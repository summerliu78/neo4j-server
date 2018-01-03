package com.yinker.data.neo4j

import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

/**
  * Created by Reynold on 17-9-18.
  *
  * target：
  * 求增量用户
  */
object Neo4jIncreService {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport().getOrCreate()

    // 参数形式“path=/home/reynold/Documents/complexnet/temp”
    val savePath = args(0).split("=")(1)

    // val current_day = LocalDateTime.now().toString().substring(0, 10)
    val pass_day = LocalDateTime.now().minusDays(1).toString().substring(0, 10)
    import spark.implicits._

    val applyCreditIncreSql =
      s"""
         |select
         |distinct(b.MOBILE) as MOBILE
         |from
         |(select USER_ID,ds from
         |edw_tinyv.E_CREDIT_APPLY_DETAIL_D
         |where dt='$pass_day' and apply_time > '$pass_day') a
         |join
         |(select USER_ID,ds,MOBILE from ods_tinyv.o_fuse_user_info_contrast_d
         |where dt='$pass_day') b
         |on a.USER_ID=b.USER_ID and a.ds=b.ds
      """.stripMargin
    spark.sql(applyCreditIncreSql).createOrReplaceTempView("applyCreditIncre")

    val callDetailSql =
      s"""
         |select
         |c.mobile as mobile,
         |c.other_mobile as other_mobile,
         |c.call_channel as call_channel,
         |c.call_time as call_time
         |from
         |(select
         |b.mobile as mobile,
         |b.other_mobile as other_mobile,
         |b.call_channel as call_channel,
         |b.call_time as call_time
         |from applyCreditIncre
         |join
         |(select mobile,other_mobile,call_channel,call_time from bdw_tinyv_outer.b_fuse_call_detail_i
         |where dt = '$pass_day') b
         |on applyCredit.mobile=b.mobile) c
         |join applyCredit on applyCredit.mobile=c.other_mobile
      """.stripMargin

    val callDetailRDD = spark.sql(callDetailSql).map(row => {
      val mobile = row.getAs[String](0)
      val other_mobile = row.getAs[String](1)
      val callPattern = "^[0-9]+".r
      val call_time_raw = row.getAs[String](3)
      val call_time = call_time_raw match {
        case callPattern(_*) => call_time_raw.toDouble
        case _ => 0.0
      }
      row.getAs[String](2) match {
        case "012001001" => (mobile + "," + other_mobile + ",CALL_OUT", call_time)
        case "012001002" => (other_mobile + "," + mobile + ",CALL_OUT", call_time)
      }
    }).filter(x => {
      val phonePattern = "^1[0-9]{10},1[0-9]{10},CALL_OUT".r
      x._1 match {
        case phonePattern(_*) => true
        case _ => false
      }
    }).rdd
      .combineByKey(
        call_time => (1, call_time),
        (c1: (Int, Double), newCallTime) => (c1._1 + 1, c1._2 + newCallTime),
        (c1: (Int, Double), c2: (Int, Double)) => (c1._1 + c2._1, c1._2 + c2._2))
      .map { case (name, (num, socre)) => (name, num, socre / num) }
      .filter(_._2 > 1) //过滤出通话次数大于１的
      .map(x => x._1 + "," + x._2 + "," + x._3.formatted("%.2f")).cache()

    callDetailRDD.saveAsTextFile(savePath + s"/$pass_day/relationship")

    callDetailRDD.map(x => {
      val sp = x.split(",")
      sp(0) + "," + sp(1)
    }).flatMap(_.split(",")).distinct()
      .saveAsTextFile(savePath + s"/$pass_day/person")
  }
}

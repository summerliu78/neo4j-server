import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

/**
  * Created by Reynold on 17-9-18.
  */
object Neo4jCallDetailBak {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport().getOrCreate()

    // 参数形式“path=/home/reynold/Documents/complexnet/temp”
    val savePath = args(0).split("=")(1)

    val current_day = LocalDateTime.now().toString().substring(0, 10)
    import spark.implicits._

    val callDetailSql =
      s"""
         |select mobile,other_mobile,call_channel,call_time from bdw_tinyv_outer.b_fuse_call_detail_i
         |where dt < '$current_day'
      """.stripMargin

    val callDetailRDD = spark.sql(callDetailSql).map(row => {
      val mobile = "p" + row.getAs[String](0)
      val other_mobile = "p" + row.getAs[String](1)
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
      val phonePattern = "^p1[0-9]{5,10},p1[0-9]{5,10},CALL_OUT".r
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
      .filter(_._2 > 1)
      .map(x => x._1 + "," + x._2 + "," + x._3.formatted("%.2f")).cache()

    callDetailRDD.saveAsTextFile(savePath + s"/$current_day/relationship")

    callDetailRDD.map(x => {
      val sp = x.split(",")
      sp(0) + "," + sp(1)
    }).flatMap(_.split(",")).distinct()
      .saveAsTextFile(savePath + s"/$current_day/person")
  }
}

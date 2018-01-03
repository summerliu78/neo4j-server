import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

/**
  * Created by Reynold on 17-11-8.
  *
  * 目标:
  *    1. 取出在我司授过信的电话号码;
  *    2. 要考虑黑白名单的csv,以便后期neo4j标签作颜色区分;
  *    3. 要考虑neo4j中需要的字段,比如逾期信息等.
  * 实现:
  *    1. 在bdw_tinyv_outer.b_fuse_call_detail_i通话详单中取出关系 relationship.csv 放在
  *    2. 在edw_tinyv.e_user_integ_info_d表中过滤出(下面是向下包含的关系)
  *       2.1 在我司授信未借款用户 applycreditNB.csv
  *       2.2 在我司借款未逾期的用户 borrow.csv
  *       2.3 在我司逾期天数小于等于7天的白名单 whitelist.csv
  *       2.4 在我司逾期天数大于14天的黑名单 blacklist.csv
  */
object Neo4jCallDetailCreditBak01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport().getOrCreate()

    // 参数形式“path=/home/reynold/Documents/complexnet/temp”
    val savePath = args(0).split("=")(1)

    val pass = LocalDateTime.now().minusDays(1).toString().substring(0, 10)
    import spark.implicits._

    val callDetailSql =
      s"""
         |select
         |b.MOBILE as MOBILE,FIRST_APPLY_LOAN_SUCC_TIME,MAX_OVERDUE_DAY_CNT
         |from
         |(select USER_ID,ds,FIRST_APPLY_CREDIT_SUCC_TIME,FIRST_APPLY_LOAN_SUCC_TIME,MAX_OVERDUE_DAY_CNT
         |from
         |edw_tinyv.e_user_integ_info_d
         |where dt='$pass' and FIRST_APPLY_CREDIT_SUCC_TIME is not null) a
         |join
         |(select USER_ID,ds,MOBILE from ods_tinyv.o_fuse_user_info_contrast_d
         |where dt='$pass') b
         |on a.USER_ID=b.USER_ID and a.ds=b.ds
      """.stripMargin

    // 通过授信的mobile
    val passCredit = spark.sql(callDetailSql).cache()

    // 过滤出通过授信但是没有借款的mobile,过滤条件为: 首次下单通过时间为空
    passCredit.filter(row => row.get(1) == null)
      .map("p" + _.getAs[String](0)).coalesce(1).distinct().write.text(savePath + s"/$pass/applycreditNoBorrow")

    // 过滤出通过授信有借款行为的mobile
    val borrow = passCredit.filter(row => row.get(1) != null).cache()

    // 在borrow基础上,过滤出历史最大逾期天数为7到15天的mobile ==> 借款但是不属于白名单也不属于黑名单
    borrow.filter(row => row.getAs[Int](2) > 7 && row.getAs[Int](2) <= 14)
      .map("p" + _.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass/borrowNotwb")

    // 在borrow基础上,过滤出历史最大逾期天数小于等于7天的mobile ==> 白名单
    borrow.filter(row => row.getAs[Int](2) <= 7)
      .map("p" + _.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass/whitelist")

    // 在borrow基础上,过滤出历史最大逾期天数超过15天的mobile ==> 黑名单
    borrow.filter(row => row.getAs[Int](2) > 14)
      .map("p" + _.getAs[String](0)).coalesce(1).distinct()
      // .map(row => {
      //   val mobile = "p" + row.getAs[String](0)
      //   val overdue = row.getAs[Int](2)
      //   mobile + "," + overdue
      //   })
      .write.text(savePath + s"/$pass/blacklist")

  }
}

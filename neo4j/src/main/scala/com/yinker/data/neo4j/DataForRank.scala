package com.yinker.data.neo4j

import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

/**
  * Created by lw on 17-11-25.
  *
  * 目标:
  * 求出计算trustrank需要的主叫被叫关系数据
  */
object DataForRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    // 参数形式“path=/home/reynold/Documents/complexnet/temp”
    val savePath = args(0).split("=")(1)

    val current_day = LocalDateTime.now().toString().substring(0, 10)
    val pass_day = LocalDateTime.now().minusDays(1).toString().substring(0, 10)
    import spark.implicits._

    // 申请授信通过的那部分人
    val applyPassSql =
      s"""
         |select
         |        b.MOBILE as MOBILE,
         |        max(FIRST_APPLY_LOAN_SUCC_TIME) as FIRST_APPLY_LOAN_SUCC_TIME,
         |        max(MAX_OVERDUE_DAY_CNT) as MAX_OVERDUE_DAY_CNT,
         |        max(CURR_INLOAN_CNT) as CURR_INLOAN_CNT,
         |        max(MAX_PREPAYMENT_DAY_CNT) as MAX_PREPAYMENT_DAY_CNT
         |from
         |        (select
         |                USER_ID,
         |                ds,
         |                FIRST_APPLY_CREDIT_SUCC_TIME,
         |                FIRST_APPLY_LOAN_SUCC_TIME,
         |                MAX_OVERDUE_DAY_CNT,
         |                CURR_INLOAN_CNT,
         |                MAX_PREPAYMENT_DAY_CNT
         |         from
         |                edw_tinyv.e_user_integ_info_d
         |         where
         |                dt='2017-10-31' and FIRST_APPLY_CREDIT_SUCC_TIME is not null
         |         ) a
         |join
         |        (select
         |                USER_ID,
         |                ds,MOBILE
         |         from
         |                ods_tinyv.o_fuse_user_info_contrast_d
         |         where
         |                dt='2017-10-31') b
         |on a.USER_ID=b.USER_ID and a.ds=b.ds group by b.MOBILE
      """.stripMargin

    val centerCreditBlackListSql =
      s"""
         |SELECT
         |        mobile
         |FROM
         |        ods_tinyv_univ.o_univ_xiaodai_black_mobile_d
         |WHERE
         |        dt = date_add(current_date(),-1)
       """.stripMargin
    //    val centerCreditBlackListRdd = spark.sql(centerCreditBlackListSql).map(_.getAs[String]("mobile"))


    // 通过授信的mobile
    val passCredit = spark.sql(applyPassSql).cache()

    // 过滤出通过授信有借款行为的mobile
    val borrow = passCredit.filter(row => row.get(1) != null).cache()

    // 在borrow基础上,过滤出历史最大逾期天数小于等于7天的mobile ==> 白名单
    borrow.filter(row => row.getAs[Int](2) <= 7 && row.getAs[Int](3) == 0 && row.getAs[Int](4) <= 3)
      .map(_.getAs[String](0))
      .distinct()
      .rdd
      .saveAsTextFile("/user/tinyv/test/lw/whitelist")
    //      .write.text(savePath + s"/$pass_day/whitelist")

    // 在borrow基础上,过滤出历史最大逾期天数超过15天的mobile ==> 黑名单
    borrow.filter(row => row.getAs[Int](2) >= 7)
      .map(_.getAs[String](0))
      //我们这边的黑名单信息和贷中黑名单（贷中黑名单又分为自有黑名单和第三方（单条手动更新 手动文件更新））
      //      .union(centerCreditBlackListRdd)
      .rdd
      .distinct()
      .saveAsTextFile("/user/tinyv/test/lw/blacklist")
    //      .write.text(savePath + s"/$pass_day/blacklist")
    passCredit.unpersist()
    borrow.unpersist()

    // 申请过授信的那帮人,不管有没有通过
    val applyCreditSql =
      s"""
         |select
         |  b.MOBILE as MOBILE
         |from
         |  (select
         |      USER_ID
         |      ,ds
         |    from
         |       edw_tinyv.e_user_integ_info_d
         |    where
         |       dt='2017-10-31'
         |        and
         |       FIRST_APPLY_CREDIT_TIME is not null
         |   ) a
         |join
         |  (select
         |    USER_ID
         |    ,ds
         |    ,MOBILE
         |   from
         |    ods_tinyv.o_fuse_user_info_contrast_d
         |    where dt='2017-10-31') b
         |on a.USER_ID=b.USER_ID and a.ds=b.ds
      """.stripMargin
    val brSet = spark.sql(applyCreditSql).map(x => {
      x.getAs[String](0)
    }).collect().toSet

    val br = spark.sparkContext.broadcast(brSet)


    // 各个节点之间的关系
    val callDetailRelsSql = s"select mobile,other_mobile,call_channel from bdw_tinyv_outer.b_fuse_call_detail_i where dt < '2017-11-01'"

    val creditUserCalls = spark.sql(callDetailRelsSql).map(x => {
      val mobile = x.getAs[String](0)
      val other_mobile = x.getAs[String](1)
      x.getAs[String](2) match {
        case "012001001" => ((mobile, other_mobile), 1)
        case "012001002" => ((other_mobile, mobile), 1)
      }

    })
      .filter(x => {
        val set = br.value
        set.contains(x._1._1) && set.contains(x._1._2)
      })
      .filter(x => {
        val phonePattern = "^1[0-9]{10},1[0-9]{10}".r
        x._1._1 + "," + x._1._2 match {
          case phonePattern(_*) => true
          case _ => false
        }
      })
      .rdd
      .reduceByKey(_ + _)
//      .filter(_._2 > 2)
      .map(x => {
        x._1._1 + "," + x._1._2
      })


    creditUserCalls.saveAsTextFile("/user/tinyv/test/lw/relationship")

  }
}

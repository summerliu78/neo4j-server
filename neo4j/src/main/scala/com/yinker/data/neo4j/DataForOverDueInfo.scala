package com.yinker.data.neo4j

import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/12/15.
  */
object DataForOverDueInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sql =
      s"""
         |select
         |	a.loan_id
         |	,a.ds
         |	,b.mobile
         |	,a.early_day
         |	,a.overdue_day
         |from
         |	(select
         |		loan_id
         |		,user_id
         |		,ds
         |		,max(early_day) early_day
         |		,max(overdue_day) overdue_day
         |	from edw_tinyv.e_repay_plan_detail_d
         |	where
         |		dt = date_add(current_date(),-1)
         |		and to_date(create_time) >= '2017-10-01'
         |		and (repay_status = '002005002'
         |			or
         |			to_date(due_day) <= date_add(current_date(),-1))
         |	group by loan_id,user_id,ds
         |	) a
         |join
         |	(select
         |		user_id
         |		,ds
         |		,udf.xv_decode(mobile_encode) mobile
         |	from edw_tinyv.e_user_integ_info_d
         |	where
         |		dt = date_add(current_date(),-1)
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
       """.stripMargin

    spark.sql(sql)
      .map(x => {
        val loan_id = x.getAs[Int](0)
        val ds = x.getAs[String](1)
        val mobile = x.getAs[String](2)
        val early_day = x.getAs[Int](3)
        val overdue_day = x.getAs[Int](4)

        s"$loan_id,$ds,$mobile,$early_day,$overdue_day"
      })
      .rdd.saveAsTextFile("/user/tinyv/test/lw/overInfo")


  }


}

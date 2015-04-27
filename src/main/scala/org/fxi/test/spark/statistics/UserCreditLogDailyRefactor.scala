package org.fxi.test.spark.statistics

import org.apache.spark.sql.DataFrame
import org.fxi.test.spark.handler.ResultHander
import org.fxi.test.spark.scheams.impl.{UserInfoSchemaLoader, UserCreditLogDailySchemaLoader}
import org.fxi.test.spark.util.SqlHelper

/**
 * Created by Administrator on 2015/4/26.
 */
object UserCreditLogDailyRefactor {
  def main(args: Array[String]) {
    extraDayLog("hq_user_credit_log_daily_04_22","hq_user_credit_log_daily_04_21");
  }

  def extraDayLog(path1: String, path2: String): Unit = {
    val loader: UserCreditLogDailySchemaLoader = new UserCreditLogDailySchemaLoader();
    loader.paths = List(path1, path2);

    SqlHelper.executeSql("select * from creditLogDaily limit 10", new ResultHander() {
      override def handler(df: DataFrame): Unit = {
        val count = df.first();
        println(count);
      }
    }, new UserInfoSchemaLoader());
  }
}

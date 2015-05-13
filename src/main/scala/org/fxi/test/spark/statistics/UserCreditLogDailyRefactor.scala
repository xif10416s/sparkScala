package org.fxi.test.spark.statistics

import java.util

import org.apache.spark.sql.{Row, DataFrame}
import org.fxi.test.spark.handler.ResultHander
import org.fxi.test.spark.scheams.impl.{UserInfoSchemaLoader, UserCreditLogDailySchemaLoader}
import org.fxi.test.spark.util.{Utils, FilePathConstants, SqlHelper}

/**
 * Created by Administrator on 2015/4/26.
 */
object UserCreditLogDailyRefactor {

  def main(args: Array[String]) {
    extraDayLog("04", "22", "04", "21", "04", "20");
  }

  def getPath(month: String, day: String): String = {
    FilePathConstants.USER_CREDIT_DAILY_BATH_PATH + "hq_user_credit_log_daily_" + month + "_" + day + ".txt";
  }

  def extraDayLog(month1: String, day1: String, month2: String, day2: String, month3: String, day3: String): Unit = {
    val path1 = getPath(month1, day1);
    val path2 = getPath(month2, day2);
    val loader: UserCreditLogDailySchemaLoader = new UserCreditLogDailySchemaLoader();
    loader.paths = List(path1);

    SqlHelper.executeSql("select userId,ctype ,adId,credit,serverLogTime,clientObtainTime,clientSyncTime from creditLogDaily  ", new ResultHander() {
      override def handler(df: DataFrame): Unit = {
        var sb1: String = "";
        var sb2: String = "";
        var sb3: String = "";

        for (r <- df.collect()) {
          if (r.getString(4).startsWith("2015-" + month1 + "-" + day1)) {
            sb1 += r.getString(0) + Utils.SPLIT_TAB + r.getInt(1) + Utils.SPLIT_TAB + r.getString(2) + Utils.SPLIT_TAB + r.getInt(3) + Utils.SPLIT_TAB + r.getString(4) + Utils.SPLIT_TAB + r.getString(5) + Utils.SPLIT_TAB + r.getString(6) + Utils.SPLIT_LINE;
          }
        }
        Utils.saveFile(sb1,FilePathConstants.RESULT_BASE_PATH+"/scala/creditLogDailyRefactory/"+"hq_user_credit_log_daily_"+month1 + "_" + day1+".txt");


        for (r <- df.collect()) {
          if (r.getString(4).startsWith("2015-" + month2 + "-" + day2)) {
            sb2 += r.getString(0) + Utils.SPLIT_TAB + r.getInt(1) + Utils.SPLIT_TAB + r.getString(2) + Utils.SPLIT_TAB + r.getInt(3) + Utils.SPLIT_TAB + r.getString(4) + Utils.SPLIT_TAB + r.getString(5) + Utils.SPLIT_TAB + r.getString(6) + Utils.SPLIT_LINE;
          }
        }

        Utils.saveFile(sb2,FilePathConstants.RESULT_BASE_PATH+"/scala/creditLogDailyRefactory/"+"hq_user_credit_log_daily_"+month2 + "_" + day2+".txt");

        for (r <- df.collect()) {
          if (r.getString(4).startsWith("2015-" + month3 + "-" + day3)) {
            sb3 += r.getString(0) + Utils.SPLIT_TAB + r.getInt(1) + Utils.SPLIT_TAB + r.getString(2) + Utils.SPLIT_TAB + r.getInt(3) + Utils.SPLIT_TAB + r.getString(4) + Utils.SPLIT_TAB + r.getString(5) + Utils.SPLIT_TAB + r.getString(6) + Utils.SPLIT_LINE;
          }
        }

        Utils.saveFile(sb3,FilePathConstants.RESULT_BASE_PATH+"/scala/creditLogDailyRefactory/"+"hq_user_credit_log_daily_"+month3 + "_" + day3+".txt");
      }
    }, loader);
  }
}

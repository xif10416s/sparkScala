package org.fxi.test.spark.scheams.impl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.fxi.test.spark.bean.UserInfo
import org.fxi.test.spark.scheams.SchemaLoader
import org.fxi.test.spark.util.FilePathConstants

/**
 * Created by Administrator on 2015/4/26.
 */
class UserInfoSchemaLoader extends SchemaLoader {
  override def loadSchema(ctx: SparkContext, sqlCtx: SQLContext): Unit = {
    println("=== Data source: UserInfoSchemaLoader RDD ===");
    // Load a text file and convert each line to a Java Bean.
    val userInfo = ctx.textFile(
      FilePathConstants.USER_INFO_PATH).map(_.split("	")).filter(_(0).length == 32).filter(!_(5).equals("\\N"))
      .map(u => UserInfo(u(0), u(5).toInt));



    // Apply a schema to an RDD of Java Beans and register it as a table.
    val schemaPeople: DataFrame = sqlCtx.createDataFrame(userInfo);
    schemaPeople.registerTempTable("userInfo");
  }
}

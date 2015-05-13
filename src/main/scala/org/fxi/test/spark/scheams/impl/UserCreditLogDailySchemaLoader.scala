package org.fxi.test.spark.scheams.impl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.fxi.test.spark.bean.{UserCreditLogDaily, UserInfo}
import org.fxi.test.spark.scheams.SchemaLoader
import org.fxi.test.spark.util.FilePathConstants
import org.fxi.test.spark.util.Utils

import scala.collection.mutable.ListBuffer

/**
 * Created by Administrator on 2015/4/26.
 */
class UserCreditLogDailySchemaLoader extends SchemaLoader {

  var paths = List[String]();

  override def loadSchema(ctx: SparkContext, sqlCtx: SQLContext): Unit = {
    println("=== Data source: UserInfoSchemaLoader RDD ===");
    val rddList:ListBuffer[RDD[UserCreditLogDaily]] =  ListBuffer[RDD[UserCreditLogDaily]]();
    for (path <- paths) {
      println(path);
      val creditLogDailyRdd = ctx.textFile(path).map(_.split(Utils.SPLIT_TAB)).map(u => UserCreditLogDaily(u(0), u(2), Utils.parseToInt(u(3)),
        Utils.parseToInt(u(1)), u(4), u(5), u(6)));
      rddList.append(creditLogDailyRdd);
    }
    println(rddList.length);

    var union: RDD[UserCreditLogDaily] = null;
    if(paths.length == 1){
      union= rddList.head;
    }
    if(paths.length == 2){
      union= ctx.union(rddList(0),(rddList(1)));
    }

    if(paths.length == 3){
      union= ctx.union(rddList(0),rddList(1),(rddList(2)));
    }



    // Apply a schema to an RDD of Java Beans and register it as a table.
    val schemaPeople: DataFrame = sqlCtx.createDataFrame(union);
    schemaPeople.registerTempTable("creditLogDaily");
    schemaPeople.cache();
  }

}

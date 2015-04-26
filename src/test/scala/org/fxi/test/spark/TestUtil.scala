package org.fxi.test.spark

import junit.framework.Test
import org.apache.spark.sql.DataFrame
import org.fxi.test.spark.handler.ResultHander
import org.fxi.test.spark.scheams.impl.UserInfoSchemaLoader
import org.fxi.test.spark.util.SqlHelper

/**
 * Created by Administrator on 2015/4/26.
 */
object TestUtil{

  def main(args: Array[String]) {
    println("Hello, world!");
    testSqlhelper();
  }

  def testSqlhelper(): Unit ={
      val sqlhelp = SqlHelper;
    sqlhelp.executeSql("select count(*) from userInfo limit 10" , new ResultHander(){
      override def handler(df: DataFrame): Unit = {
       val count = df.first();
        println(count);
      }
    },new UserInfoSchemaLoader() ); 
  }

}

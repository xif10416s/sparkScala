package org.fxi.test.spark

import org.apache.spark.sql.DataFrame
import org.fxi.test.spark.handler.ResultHander
import org.fxi.test.spark.scheams.impl.UserInfoSchemaLoader
import org.fxi.test.spark.util.{FilePathConstants, Utils, SqlHelper}

/**
  * Created by Administrator on 2015/4/26.
  */
object TestUtil{

   def main(args: Array[String]) {
     println("Hello, world!");
//     testSqlhelper();
     testSaveFile();
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

  def testSaveFile():Unit ={
     Utils.saveFile("aaa",FilePathConstants.RESULT_BASE_PATH+"/scala/test/test.txt")
  }

 }

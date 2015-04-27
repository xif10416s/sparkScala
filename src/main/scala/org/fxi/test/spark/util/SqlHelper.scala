package org.fxi.test.spark.util

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.fxi.test.spark.handler.{MutiResultHander, RunTask, ResultHander}
import org.fxi.test.spark.scheams.SchemaLoader

/**
 * Created by Administrator on 2015/4/26.
 */
object SqlHelper {


  def executeSql(sql: String, handler: ResultHander, scLoader: SchemaLoader*): Unit = {
    val sparkConf = new SparkConf().setMaster("local[6]").set("spark.driver.maxResultSize", "5000m")
      .setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
    val sc = new SparkContext(sparkConf);
    val sqlCtx: SQLContext = new SQLContext(sc);



    for (sl <- scLoader) {
       sl.loadSchema(sc, sqlCtx);
    }
    val scm : DataFrame = sqlCtx.sql(sql);

    handler.handler(scm);

    sc.stop();
  }

  def executeSql(runTasks:List[RunTask],scLoader: SchemaLoader*): Unit ={
    val sparkConf = new SparkConf().setMaster("local[6]").set("spark.driver.maxResultSize", "2500m")
      .setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
    val sc = new SparkContext(sparkConf);
    val sqlCtx: SQLContext = new SQLContext(sc);

    for (sl <- scLoader) {
      sl.loadSchema(sc, sqlCtx);
    }

    for(run <- runTasks){
      val scm : DataFrame= sqlCtx.sql(run.sql);
      run.handler.handler(scm);
    }

    sc.stop();
  }


  def executeSql(sqlList:List[String],mrsHandler:MutiResultHander,scLoader: SchemaLoader*): Unit ={
    val sparkConf = new SparkConf().setMaster("local[6]").set("spark.driver.maxResultSize", "2500m")
      .setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
    val sc = new SparkContext(sparkConf);
    val sqlCtx: SQLContext = new SQLContext(sc);

    for (sl <- scLoader) {
      sl.loadSchema(sc, sqlCtx);
    }

    val rsjavaScm = new Array[DataFrame](sqlList.size) ;
    var index :Integer = 0;
    for( sql <- sqlList) {
      rsjavaScm(index) = sqlCtx.sql(sql);
      index = index+1;
    }

    mrsHandler.handler(rsjavaScm:_*);


    sc.stop();
  }
}

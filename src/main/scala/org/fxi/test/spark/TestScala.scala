package org.fxi.test.spark

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by Administrator on 2015/4/26.
 */
object TestScala {
  def main(args: Array[String]) {
    println("Hello, world!");
    val conf = new SparkConf().setAppName("aaaa").setMaster("local[6]");
    val sc =  new SparkContext(conf);
    val data = Array(1, 2, 3, 4, 5, 6);
    val distData = sc.parallelize(data);
    println(distData.count());
  }

  def handler():Unit = {
    println("Hello, world!");
  } 
}

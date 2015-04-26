package org.fxi.test.spark.scheams

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2015/4/26.
 */
abstract class SchemaLoader {
  def loadSchema(ctx: SparkContext, sqlCtx: SQLContext);
}

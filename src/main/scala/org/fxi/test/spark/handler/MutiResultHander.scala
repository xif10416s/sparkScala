package org.fxi.test.spark.handler

import org.apache.spark.sql.DataFrame

/**
 * Created by Administrator on 2015/4/26.
 */
abstract  class MutiResultHander {
  def handler(df : DataFrame *)
}

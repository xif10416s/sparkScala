package org.fxi.test.spark.util

import java.io._

/**
 * Created by Administrator on 2015/4/26.
 */
object Utils {
  val SPLIT_TAB = "	";
  val SPLIT_LINE = "\r\n";

  def saveFile(content: String, path: String): Unit = {
    var file = new File(path);
    val parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    } else {
      System.out.println("//目录存在");
    }
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch {
        case e: Exception => e.printStackTrace();
      }
    };
    val writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true))));

    writer.write(content)
    writer.close()
  }

  def parseToInt(part: String): Integer = {
      if("\\N".equals(part)) {
        return 0
      }
      return part.toInt;
  }


}

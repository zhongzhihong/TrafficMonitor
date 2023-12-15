package com.zzh.ScalaLanguage.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * 从数据库中读取数据作为一个数据流
 */
class JdbcReadSource[T](classType: Class[_ <: T]) extends RichSourceFunction[T] {
  var flag: Boolean = true
  var conn: Connection = _
  var pst: PreparedStatement = _
  var ret: ResultSet = _

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    while (flag) {
      ret = pst.executeQuery()
      if (classType.getName.equals(classOf[MonitorLimitInfo].getName)) {
        val info = new MonitorLimitInfo(ret.getString(1), ret.getString(2), ret.getInt(3), ret.getString(4))
        sourceContext.collect(info.asInstanceOf[T])
      }
      ret.close()
      // 休眠一小时后从数据库中更新数据
      Thread.sleep(60 * 60 * 1000)
    }
  }

  override def cancel(): Unit = {
    flag = false
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:/traffic_monitor", "root", "123456zzh")
    if (classType.getName.equals(classOf[MonitorLimitInfo].getName)) {
      pst = conn.prepareStatement("SELECT * FROM t_monitor_info WHERE speed_limit > 0")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}

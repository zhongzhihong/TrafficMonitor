package com.zzh.util

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
      while (ret.next()) {
        if (classType.getName.equals(classOf[MonitorLimitInfo].getName)) {
          val info = MonitorLimitInfo(ret.getString(1), ret.getString(2), ret.getInt(3), ret.getString(4))
          sourceContext.collect(info.asInstanceOf[T])
        }
        if (classType.getName.equals(classOf[ViolationInfo].getName)) {
          val info = ViolationInfo(ret.getString("car"), ret.getString("violation"), ret.getLong("create_time"), ret.getInt("out_count"))
          sourceContext.collect(info.asInstanceOf[T])
        }
      }
      // 休眠一小时后从数据库中更新数据
      Thread.sleep(60 * 60 * 1000)
      ret.close()
    }
  }

  override def cancel(): Unit = {
    flag = false
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor?useSSL=false", "root", "123456zzh")
    if (classType.getName.equals(classOf[MonitorLimitInfo].getName)) {
      pst = conn.prepareStatement("SELECT * FROM t_monitor_info WHERE speed_limit > 0")
    }
    if (classType.getName.equals(classOf[ViolationInfo].getName)) {
      pst = conn.prepareStatement("SELECT * FROM t_violation_list")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}

package com.zzh.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}

class JdbcWriteDataSink[T](classType: Class[_ <: T]) extends RichSinkFunction[T] {

  var conn: Connection = _
  var pst: PreparedStatement = _

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      val info = value.asInstanceOf[OutOfLimitSpeedInfo]
      pst.setString(1, info.car)
      pst.setString(2, info.monitorId)
      pst.setString(3, info.roadId)
      pst.setDouble(4, info.realSpeed)
      pst.setInt(5, info.limitSpeed)
      pst.setLong(6, info.actionTime)
      pst.executeUpdate()
    }
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor", "root", "123456zzh")
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      pst = conn.prepareStatement("INSERT INTO t_speeding_info (car, monitor_id, road_id, real_speed, limit_speed, action_time) VALUES (?, ?, ?, ?, ?, ?)")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}

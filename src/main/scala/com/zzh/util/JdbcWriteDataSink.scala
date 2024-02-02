package com.zzh.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

class JdbcWriteDataSink[T](classType: Class[_ <: T]) extends RichSinkFunction[T] {

  var conn: Connection = _
  var pst: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor", "root", "123456zzh")
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      pst = conn.prepareStatement("INSERT INTO t_speeding_info (car, monitor_id, road_id, real_speed, limit_speed, action_time) VALUES (?, ?, ?, ?, ?, ?)")
    }
    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      pst = conn.prepareStatement("INSERT INTO t_average_speed (start_time, end_time, monitor_id, avg_speed, car_count) VALUES (?, ?, ?, ?, ?)")
    }
    if (classType.getName.equals(classOf[RepetitionCarWarningInfo].getName)) {
      pst = conn.prepareStatement("INSERT INTO t_violation_list (car, violation, create_time) VALUES (?, ?, ?)")
    }
    if (classType.getName.equals(classOf[ViolationInfo].getName)) {
      pst = conn.prepareStatement("INSERT INTO t_violation_list (car, violation, create_time) VALUES (?, ?, ?)")
    }
  }

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
    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      val info = value.asInstanceOf[AvgSpeedInfo]
      pst.setLong(1, info.start)
      pst.setLong(2, info.end)
      pst.setString(3, info.monitorId)
      pst.setDouble(4, info.avgSpeed)
      pst.setInt(5, info.carCount)
      pst.executeUpdate()
    }
    if (classType.getName.equals(classOf[RepetitionCarWarningInfo].getName)) {
      val info = value.asInstanceOf[RepetitionCarWarningInfo]
      pst.setString(1, info.car)
      pst.setString(2, info.warningMsg)
      pst.setLong(3, info.warningTime)
      pst.executeUpdate()
    }
    if (classType.getName.equals(classOf[ViolationInfo].getName)) {
      val info = value.asInstanceOf[ViolationInfo]
      // 在同一天内，同一个车牌如果存在危险驾驶的告警，只往数据库插入一条记录
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val day = sdf.parse(sdf.format(new Date(info.creatTime)))
      // 先查询数据库中是否有同样车牌的数据
      val statement = conn.prepareStatement("SELECT COUNT(1),out_count FROM t_violation_list WHERE car = ? AND create_time BETWEEN ? AND ?")
      statement.setString(1, info.car)
      statement.setLong(2, day.getTime)
      statement.setLong(3, day.getTime + (24 * 60 * 60 * 1000))
      val resultSet = statement.executeQuery()
      if (resultSet.next()) {
        pst.setString(1, info.car)
        pst.setString(2, info.msg)
        pst.setLong(3, info.creatTime)
        pst.executeUpdate()
      } else {
        val count = resultSet.getInt(2)
        if (count < info.outCount) {
          val statement1 = conn.prepareStatement("UPDATE t_violation_list SET out_count = ? WHERE car = ?")
          statement1.setInt(1, info.outCount)
          statement1.setString(2, info.car)
          statement1.executeUpdate()
          statement1.close()
        }
      }
      resultSet.close()
      statement.close()
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}

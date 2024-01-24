package com.zzh.util

import org.apache.flink.api.common.state.MapStateDescriptor

/**
 * 项目中的全局常量
 */
object GlobalConstant {
  // 状态描述
  val MONITOR_LIMIT_STATE_DESCRIPTION = new MapStateDescriptor[String, MonitorLimitInfo]("monitor_state", classOf[String], classOf[MonitorLimitInfo])
}

// 车辆经过卡口的日志数据
case class TrafficLog(actionTime: Long, monitorId: String, cameraId: String, car: String, speed: Double, roadId: String, areaId: String)

// 卡口限速信息表
case class MonitorLimitInfo(monitorId: String, roadId: String, speedLimit: Int, areaId: String)

// 超速车辆信息表
case class OutOfLimitSpeedInfo(car: String, monitorId: String, roadId: String, realSpeed: Double, limitSpeed: Int, actionTime: Long)

// 卡口的平均车速对象
case class AvgSpeedInfo(start: Long, end: Long, monitorId: String, avgSpeed: Double, carCount: Int)

// 套牌车告警信息对象
case class RepetitionCarWarningInfo(car: String, firstMonitor: String, secondMonitor: String, warningTime: Long, warningMsg: String)
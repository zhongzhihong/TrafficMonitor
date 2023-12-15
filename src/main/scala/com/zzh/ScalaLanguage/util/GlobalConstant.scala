package com.zzh.ScalaLanguage.util

/**
 * 项目中的全局常量
 */
class GlobalConstant {

}

// 车辆经过卡口的日志数据
case class TrafficLog(actionTime: Long, monitorId: String, cameraId: String, car: String, speed: Double, roadId: String, areaId: String)

// 卡口限速信息表
case class MonitorLimitInfo(monitorId: String, roadId: String, speedLimit: Int, areaId: String)
package com.zzh.util

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class CustomerTrigger extends Trigger[TrafficLog, TimeWindow] {

  // 当窗口中进入一条数据的回调函数（触发机制：只要有数据来了，直接出发窗口函数，同时不要把这条数据保存到状态中）
  override def onElement(t: TrafficLog, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
  // 基于运行时间的窗口触发的回调函数

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  // 基于时间时间的窗口触发的回调函数

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  // 当前窗口已经结束的回调函数

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

}

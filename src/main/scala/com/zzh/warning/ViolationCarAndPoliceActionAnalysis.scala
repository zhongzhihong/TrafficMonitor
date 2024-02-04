package com.zzh.warning

import com.zzh.util.{PoliceAction, ViolationInfo}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 假设有两个流：
 * 1、违法车辆的实时流（数据乱序2秒）    ：鲁H37985,涉嫌套牌车,1592515403000
 * 2、交警出警记录的实时流（数据乱序2秒） ：J0002,鲁H37985,处理完成,1592515404000
 * 需求：
 * 一、找出当前违法车辆在10分钟内有对应出警记录的数据
 * 二、当前违法车辆在10分钟内没有交警出警
 * 三、有交警的出警记录，但是没有找到对应的违法车辆
 */
object ViolationCarAndPoliceActionAnalysis {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = streamEnv.socketTextStream("localhost", 7777)
      .map(line => {
        val arr = line.split(",")
        new ViolationInfo(arr(0), arr(1), arr(2).toLong, 0)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(2)) {
        override def extractTimestamp(t: ViolationInfo): Long = t.creatTime
      })

    val stream2 = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
        val arr = line.split(",")
        new PoliceAction(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceAction](Time.seconds(2)) {
        override def extractTimestamp(t: PoliceAction): Long = t.actionTime
      })

    stream1.keyBy(_.car)
      // 上下10秒都可以关联，包括第10秒
      .intervalJoin(stream2.keyBy(_.car))
      // 设置一个时间边界，在这个边界内两个流的数据自动根据相同的车牌号进行关联
      .between(Time.seconds(0), Time.seconds(10))
      .process(new ProcessJoinFunction[ViolationInfo, PoliceAction, String] {
        override def processElement(in1: ViolationInfo, in2: PoliceAction, context: ProcessJoinFunction[ViolationInfo, PoliceAction, String]#Context, collector: Collector[String]): Unit = {
          // 关联成功
          collector.collect(s"违法车辆【${in1.car}】，已经有交警出警了，警号为【${in2.policedId}】，出警状态为【${in2.actionStatus}】，出警时间为【${in2.actionTime}】")
        }
      })
      .print()

    streamEnv.execute()
  }
}

package com.zzh.monitor

import com.zzh.util.{AvgSpeedInfo, JdbcWriteDataSink, TrafficLog}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object AvgSpeedMonitor {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.setProperty("group.id", "traffic01")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    // 假设数据出现乱序，但是不超过5秒
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val mainStream = streamEnv.socketTextStream("node1", 8888)
      .map(line => {
        val arr = line.split(",")
        TrafficLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficLog](Time.seconds(5)) {
        override def extractTimestamp(t: TrafficLog): Long = t.actionTime
      })

    // 统计每个卡口的平均车速
    mainStream.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      // 统计每个卡口经过车辆的数量以及这些车辆的车速之和（使用增量函数）
      .aggregate(
        // 累加器类型为二元组（累加车速之和，累加车辆数量）
        new AggregateFunction[TrafficLog, (Double, Long), (Double, Long)] {
          override def createAccumulator(): (Double, Long) = (0.0, 0)

          override def add(value: TrafficLog, acc: (Double, Long)): (Double, Long) = (acc._1 + value.speed, acc._2 + 1)

          override def getResult(acc: (Double, Long)): (Double, Long) = acc

          override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = (a._1 + b._1, a._2 + b._2)
        },
        // 全量函数，计算平均车速
        (key: String, win: TimeWindow, input: Iterable[(Double, Long)], out: Collector[AvgSpeedInfo]) => {
          val t = input.last
          val avg = (t._1 / t._2).formatted("%.2f").toDouble
          out.collect(new AvgSpeedInfo(win.getStart, win.getEnd, key, avg, t._2.toInt))
        }
      )
      //      .addSink(new JdbcWriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))
      .print()

    streamEnv.execute()
  }
}

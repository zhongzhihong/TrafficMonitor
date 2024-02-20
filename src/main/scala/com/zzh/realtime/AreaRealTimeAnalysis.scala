package com.zzh.realtime

import com.zzh.util.TrafficLog
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object AreaRealTimeAnalysis {
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

    // 消费kafka中的数据（注意topic需要提前存在）
    val mainStream = streamEnv.addSource(new FlinkKafkaConsumer[String]("traffic_zzh", new SimpleStringSchema(), props).setStartFromEarliest())
      .map(line => {
        val arr = line.split(",")
        TrafficLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })

    mainStream.keyBy(_.areaId)
      .timeWindow(Time.seconds(30))
      .apply((key: String, win: TimeWindow, input: Iterable[TrafficLog], out: Collector[String]) => {
        val set = scala.collection.mutable.Set()
        for (i <- input) {
          set += i
        }
        out.collect(s"区域id：${key}，时间范围为【起始时间：${win.getStart}----结束时间：${win.getEnd}】，一共上路的车辆为${set.size}")
      })
      .print()

    streamEnv.execute()
  }
}

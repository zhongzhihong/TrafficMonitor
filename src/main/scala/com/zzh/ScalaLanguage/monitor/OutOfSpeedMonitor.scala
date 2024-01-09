package com.zzh.ScalaLanguage.monitor

import com.zzh.ScalaLanguage.util.{GlobalConstant, JdbcReadSource, JdbcWriteDataSink, MonitorLimitInfo, OutOfLimitSpeedInfo, TrafficLog}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object OutOfSpeedMonitor {
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

    // 广播状态流（数据量少，更新频率不高）
    // 1.读取一个Source得到一个流
    // 2.通过JobManager把流广播到所有的TaskManager
    // 3.调用connect算子和主流中的数据连接计算
    val broadStream = streamEnv.addSource(new JdbcReadSource[MonitorLimitInfo](classOf[MonitorLimitInfo]))
      .broadcast(GlobalConstant.MONITOR_LIMIT_STATE_DESCRIPTION)

    Thread.sleep(2000)

    // 消费kafka中的数据（注意topic需要提前存在）
    // val mainStream = streamEnv.addSource(new FlinkKafkaConsumer[String]("traffic_zzh", new SimpleStringSchema(), props).setStartFromEarliest())
    // 读取本地的文件
    val mainStream = streamEnv.readTextFile("./data/trafficData")
      .map(line => {
        val arr = line.split(",")
        TrafficLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })

    // 只能使用connect，因为两个流的类型不一致
    mainStream.connect(broadStream)
      .process(new BroadcastProcessFunction[TrafficLog, MonitorLimitInfo, OutOfLimitSpeedInfo] {
        // 处理普通流
        override def processElement(value: TrafficLog, readOnlyContext: BroadcastProcessFunction[TrafficLog, MonitorLimitInfo, OutOfLimitSpeedInfo]#ReadOnlyContext, out: Collector[OutOfLimitSpeedInfo]): Unit = {
          // 首先从状态中得到卡口的限速信息
          val info = readOnlyContext.getBroadcastState(GlobalConstant.MONITOR_LIMIT_STATE_DESCRIPTION).get(value.monitorId)
          if (info != null) {
            // 当前车辆经过卡口的时候，该卡口有限速，需要判断是否超速
            val limitSpeed = info.speedLimit
            val realSpeed = value.speed
            if (limitSpeed * 1.1 < realSpeed) {
              // 如果超速通过卡口，输出一条超速的信息
              out.collect(OutOfLimitSpeedInfo(value.car, value.monitorId, value.roadId, realSpeed, limitSpeed, value.actionTime))
            }
          }
        }

        // 处理广播状态流
        override def processBroadcastElement(value: MonitorLimitInfo, context: BroadcastProcessFunction[TrafficLog, MonitorLimitInfo, OutOfLimitSpeedInfo]#Context, collector: Collector[OutOfLimitSpeedInfo]): Unit = {
          // 把流里面的数据保存到状态中
          val state = context.getBroadcastState(GlobalConstant.MONITOR_LIMIT_STATE_DESCRIPTION)
          state.put(value.monitorId, value)
        }
      })
      // 写入到MySQL中
      .addSink(new JdbcWriteDataSink[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo]))
    // 可以先根据本地的文件判断超速信息，打印到控制台中
    //      .print()

    streamEnv.execute()
  }
}

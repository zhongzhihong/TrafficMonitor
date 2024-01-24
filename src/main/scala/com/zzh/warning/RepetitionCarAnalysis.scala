package com.zzh.warning

import com.zzh.util.{JdbcWriteDataSink, RepetitionCarWarningInfo, TrafficLog}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

/**
 * 套牌车分析
 */
object RepetitionCarAnalysis {
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
      }).assignAscendingTimestamps(_.actionTime)

    mainStream.keyBy(_.car)
      .process(new KeyedProcessFunction[String, TrafficLog, RepetitionCarWarningInfo] {

        lazy val firstState: ValueState[TrafficLog] = getRuntimeContext.getState(new ValueStateDescriptor[TrafficLog]("first", classOf[TrafficLog]))

        override def processElement(value: TrafficLog, ctx: KeyedProcessFunction[String, TrafficLog, RepetitionCarWarningInfo]#Context, collector: Collector[RepetitionCarWarningInfo]): Unit = {
          val first = firstState.value()
          if (first == null) {
            // 表示当前数据就是第一次经过卡口的数据
            firstState.update(value)
          } else {
            // 表示该车辆已经在某个卡口中出现过了，需要判断时间差
            val firstTime = first.actionTime
            val secondTime = value.actionTime
            val less: Long = (secondTime - firstTime).abs / 1000
            if (less < 10) {
              // 涉嫌套牌车
              val info = new RepetitionCarWarningInfo(value.car, first.monitorId, value.monitorId, ctx.timerService().currentProcessingTime(), "涉嫌套牌车")
              collector.collect(info)
              firstState.clear()
            } else {
              // 暂时不是套牌车，仍需进一步后面的数据进行观察
              firstState.update(if (secondTime > firstTime) value else first)
            }
          }
        }
      })
      .addSink(new JdbcWriteDataSink[RepetitionCarWarningInfo](classOf[RepetitionCarWarningInfo]))

    streamEnv.execute()
  }
}

package com.zzh.ScalaLanguage.monitor

import com.zzh.ScalaLanguage.util.{JdbcReadSource, MonitorLimitInfo, TrafficLog}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object OutOfSpeedMonitor {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._

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
    streamEnv.addSource(new JdbcReadSource[MonitorLimitInfo](classOf[MonitorLimitInfo]))

    // 注意topic需要提前存在
    val mainStream = streamEnv.addSource(new FlinkKafkaConsumer[String]("traffic_zzh", new SimpleStringSchema(), props))
      .map(line => {
        val arr = line.split(",")
        TrafficLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })
  }
}

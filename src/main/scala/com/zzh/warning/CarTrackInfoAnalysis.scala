package com.zzh.warning

import com.zzh.util.{GlobalConstant, HBaseWriteDataSink, JdbcReadSource, MonitorLimitInfo, TrackInfo, TrafficLog, ViolationInfo}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object CarTrackInfoAnalysis {
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
    val broadStream = streamEnv.addSource(new JdbcReadSource[ViolationInfo](classOf[ViolationInfo]))
      .broadcast(GlobalConstant.VIOLATION_STATE_DESCRIPTION)

    Thread.sleep(2000)

    // 消费kafka中的数据（注意topic需要提前存在）
    // val mainStream = streamEnv.addSource(new FlinkKafkaConsumer[String]("traffic_zzh", new SimpleStringSchema(), props).setStartFromEarliest())
    // 读取本地的文件
    val mainStream = streamEnv.readTextFile("./data/trafficData")
      .map(line => {
        val arr = line.split(",")
        TrafficLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })

    mainStream.connect(broadStream)
      .process(new BroadcastProcessFunction[TrafficLog, ViolationInfo, TrackInfo] {
        override def processElement(in1: TrafficLog, readOnlyContext: BroadcastProcessFunction[TrafficLog, ViolationInfo, TrackInfo]#ReadOnlyContext, collector: Collector[TrackInfo]): Unit = {
          val info = readOnlyContext.getBroadcastState(GlobalConstant.VIOLATION_STATE_DESCRIPTION).get(in1.car)
          if (info != null) {
            collector.collect(new TrackInfo(in1.car, in1.actionTime, in1.monitorId, in1.roadId, in1.areaId, in1.speed))
          }
        }

        override def processBroadcastElement(in2: ViolationInfo, context: BroadcastProcessFunction[TrafficLog, ViolationInfo, TrackInfo]#Context, collector: Collector[TrackInfo]): Unit = {
          // 把广播状态流的数据存储在状态中
          context.getBroadcastState(GlobalConstant.VIOLATION_STATE_DESCRIPTION).put(in2.car, in2)
        }
      })
      // 使用批量写入把数据写入HBase，每次写入20条数据。（Flink中有countWindow，得到20条数据即开启window）
      .countWindowAll(20)
      .apply((window: GlobalWindow, input: Iterable[TrackInfo], out: Collector[java.util.List[Put]]) => {
        val list = new java.util.ArrayList[Put]
        for (info <- input) {
          val put = new Put(Bytes.toBytes(info.car + "_" + (Long.MaxValue - info.actionTime)))
          put.add("cf1".getBytes, "actionTime".getBytes, Bytes.toBytes(info.actionTime))
          put.add("cf1".getBytes, "monitorId".getBytes, Bytes.toBytes(info.monitorId))
          put.add("cf1".getBytes, "roadId".getBytes, Bytes.toBytes(info.roadId))
          put.add("cf1".getBytes, "areaId".getBytes, Bytes.toBytes(info.areaId))
          put.add("cf1".getBytes, "speed".getBytes, Bytes.toBytes(info.speed))
          put.add("cf1".getBytes, "car".getBytes, Bytes.toBytes(info.car))
          list.add(put)
        }
        out.collect(list)
      })
      .addSink(new HBaseWriteDataSink)
    //      .print()

    streamEnv.execute()
  }
}

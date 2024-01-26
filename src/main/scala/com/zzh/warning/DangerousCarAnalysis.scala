package com.zzh.warning

import com.zzh.util.{JdbcWriteDataSink, MonitorLimitInfo, OutOfLimitSpeedInfo, RepetitionCarWarningInfo, TrafficLog, ViolationInfo}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

import java.sql.DriverManager
import java.util.Properties
import scala.collection.mutable

/**
 * 套牌车分析
 */
object DangerousCarAnalysis {
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

    // 1、准备数据流
    val stream = mainStream.map(new OutOfSpeedFunction(80))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OutOfLimitSpeedInfo](Time.seconds(5)) {
        override def extractTimestamp(element: OutOfLimitSpeedInfo): Long = element.actionTime
      })

    // 2、定义模式
    val pattern = Pattern.begin[OutOfLimitSpeedInfo]("start")
      .where(t => {
        // 超速20%
        t.realSpeed > t.limitSpeed * 1.2
      })
      // 2分钟超速3次以上，则是危险驾驶
      .timesOrMore(3)
      // 使用贪婪模式：2分钟内尽可能匹配更多次的超速
      .greedy
      .within(Time.minutes(2))

    // 3、在数据流中匹配得到检测流
    val ps = CEP.pattern(stream.keyBy(_.car), pattern)

    // 4、选择结果
    ps.select(
        map => {
          val list = map("start").toList
          var sum = 0.0
          val count = list.size
          val builder = new StringBuilder()
          builder.append(s"当前车辆${list(0).car}，涉嫌危险驾驶，它在两分钟内经过的卡口数为：${list.size}")
          for (i <- 0 until (list.size)) {
            val info = list(i)
            sum += info.realSpeed
            builder.append(s"第${i + 1}个卡口${info.monitorId}，车速为：${info.realSpeed}，该卡口的限速为：${info.limitSpeed}")
          }
          val avg = (sum / count).formatted("%.2f").toDouble
          builder.append(s"它在两分钟内的平均车速为：${avg}")
          ViolationInfo(list(0).car, builder.toString(), System.currentTimeMillis())
        }
      )
      .print()

    streamEnv.execute()
  }

  class OutOfSpeedFunction(baseLimitSpeed: Int) extends RichMapFunction[TrafficLog, OutOfLimitSpeedInfo] {
    var map = scala.collection.mutable.Map[String, MonitorLimitInfo]()

    override def map(value: TrafficLog): OutOfLimitSpeedInfo = {
      /*
      1、如果集合中有当前卡口的限速，判断是否超速20%；
      2、如果集合中没有当前卡口的限速，城市道路应有一个基本的限速：80 km/h
       */
      val info = map.getOrElse(value.monitorId, MonitorLimitInfo(value.monitorId, value.roadId, baseLimitSpeed, value.areaId))
      OutOfLimitSpeedInfo(value.car, value.monitorId, value.roadId, value.speed, info.speedLimit, value.actionTime)
    }

    override def open(parameters: Configuration): Unit = {
      // 把数据库中所有的限速信息读取到map中
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor?useSSL=false", "root", "123456zzh")
      val pst = conn.prepareStatement("SELECT * FROM t_monitor_info WHERE speed_limit > 0")
      val ret = pst.executeQuery()
      while (ret.next()) {
        val info = MonitorLimitInfo(ret.getString(1), ret.getString(2), ret.getInt(3), ret.getString(4))
        map.put(info.monitorId, info)
      }
      ret.close()
      pst.close()
      conn.close()
    }
  }

}

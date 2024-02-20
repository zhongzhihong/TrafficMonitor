package com.zzh.realtime

import com.zzh.util.{CustomerBloomFilter, CustomerTrigger, TrafficLog}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer
import redis.clients.jedis.Jedis

import java.util.Properties

/**
 * 使用布隆过滤器的原因：
 * 解决内存受限的问题。如果对整个窗口的数据做全量计算的话，其实还存在内存受限。
 * 不要全量计算，可以通过改变窗口的触发机制，不让数据存在状态中
 */
object AreaRealTimeAnalysisUseBloomFilter {
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
      // 在默认情况下，窗口在没有触发的时候，会把所有数据保存到状态中，只能修改窗口的触发规则
      .trigger(new CustomerTrigger)
      .process(new ProcessWindowFunction[TrafficLog, String, String, TimeWindow] {

        var bloomFilter: CustomerBloomFilter = _
        var jedis: Jedis = _

        override def open(parameters: Configuration): Unit = {
          bloomFilter = new CustomerBloomFilter(1 << 25)
          jedis = new Jedis("node01", 6379)
          jedis.select(0)
        }

        // 每一条数据都执行一次，一个数据通过布隆过滤器去重。可以使用 Redis 做位图计算
        override def process(key: String, context: Context, elements: Iterable[TrafficLog], out: Collector[String]): Unit = {
          // TODO
        }

      })

    streamEnv.execute()
  }
}

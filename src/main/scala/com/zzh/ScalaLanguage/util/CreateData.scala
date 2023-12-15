package com.zzh.ScalaLanguage.util

import org.apache.commons.math3.random.{GaussianRandomGenerator, JDKRandomGenerator}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

/**
 * 模拟生成数据【时间戳，卡口ID，摄像头ID，车牌信息，车速，道路ID，区域ID】
 * 1.将模拟的数据生成到文件 trafficData 中
 * 2.将模拟的数据生成到Kafka中
 */
object CreateData {
  def main(args: Array[String]): Unit = {
    var out: FileOutputStream = null
    var pw: PrintWriter = null
    val r = new Random()
    var producer: KafkaProducer[Nothing, String] = null

    val locations = Array[String]("京", "津", "冀", "京", "鲁", "京", "京", "京", "京", "京")
    val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)
    try {
      // 初始化文件的输出流，执行代码前需要先创建data文件夹
      out = new FileOutputStream("./data/trafficData")
      pw = new PrintWriter(new OutputStreamWriter(out, "UTF-8"))
      producer = new KafkaProducer[Nothing, String](props)
      // 初始化高斯分布的对象
      val generator = new JDKRandomGenerator()
      // 设置随机数的种子
      generator.setSeed(10)
      // 初始化高斯分布的随机数对象
      val gaussianRandomGenerator = new GaussianRandomGenerator(generator)
      // 模拟生成一天的数据：假设一天有30000辆车上路
      for (i <- 1 to 3000) {
        // 构造随机的车牌，如粤L876NQ
        val car = locations(r.nextInt(locations.length)) + (65 + r.nextInt(26)).toChar + r.nextInt(100000).formatted("%05d")
        // 构造一辆车行驶的起始时间
        var startHour = r.nextInt(24).formatted("%02d")
        // 构造正态分布的随机数，模拟每辆车经过卡口的数据
        val g = gaussianRandomGenerator.nextNormalizedDouble()
        // 假设大多数车辆一天经过30左右的卡口，所以模拟均值为30
        val m_count = (30 + (30 * g)).abs.toInt + 1
        // 一辆车每经过卡口就会留下数据
        for (j <- 1 to m_count) {
          if (j % 30 == 0) {
            var newHour = startHour.toInt + 1
            if (newHour == 24) {
              newHour = 0
            }
            startHour = newHour.formatted("%02d")
          }
          // 得到车辆经过卡口的时间
          val actionTime = day + " " + startHour + ":" + r.nextInt(60).formatted("%02d") + ":" + r.nextInt(60).formatted("%02d")
          // 得到时间戳
          val realTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(actionTime)
          // 生成随机的卡口ID
          val monitorId = r.nextInt(50).formatted("%04d")
          // 生成随机车速
          val g2 = gaussianRandomGenerator.nextNormalizedDouble()
          val speed = ((50 + (50 * g2)).abs + 1).formatted("%.1f")
          // 得到随机的道路ID
          val roadId = r.nextInt(1000).formatted("%03d")
          // 得到随机的摄像头ID
          val cameraId = r.nextInt(100000).formatted("%05d")
          // 得到随机的区域ID
          val areaId = r.nextInt(8).formatted("%02d")
          // 得到一条完整的信息【时间戳，卡口ID，摄像头ID，车牌信息，车速，道路ID，区域ID】
          val content = realTime.getTime + "," + monitorId + "," + cameraId + "," + car + "," + speed + "," + roadId + "," + areaId
          // 写入文件
          pw.write(content + "\n")
          // 写入kafka
          val record = new ProducerRecord("traffic_zzh", content)
          producer.send(record)
        }
        pw.flush()
      }
      pw.flush()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      pw.close()
      out.close()
      producer.close()
    }
  }
}

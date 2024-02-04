package com.zzh.warning

import com.zzh.util.{PoliceAction, ViolationInfo}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ViolationCarAndPoliceActionAnalysis2 {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val stream1: DataStream[ViolationInfo] = streamEnv.socketTextStream("localhost", 7777)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new ViolationInfo(arr(0), arr(1), arr(2).toLong, 0)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(2)) {
        override def extractTimestamp(element: ViolationInfo) = element.creatTime
      })

    val stream2: DataStream[PoliceAction] = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new PoliceAction(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceAction](Time.seconds(2)) {
        override def extractTimestamp(element: PoliceAction) = element.actionTime
      })

    val tag1 = new OutputTag[PoliceAction]("No Violation Car!")
    val tag2 = new OutputTag[ViolationInfo]("No PoliceAction!")

    var mainStream = stream1.keyBy(_.car).connect(stream2.keyBy(_.car))
      .process(new KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String] {

        //需要两个状态，分别保存违法数据，出警记录
        lazy val vState: ValueState[ViolationInfo] = getRuntimeContext.getState(new ValueStateDescriptor[ViolationInfo]("v", classOf[ViolationInfo]))
        lazy val pState: ValueState[PoliceAction] = getRuntimeContext.getState(new ValueStateDescriptor[PoliceAction]("p", classOf[PoliceAction]))

        override def processElement1(value: ViolationInfo, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
          println("processElement1 ==> " + value)
          val policeAction: PoliceAction = pState.value()
          if (policeAction == null) { //可能出警的数据还没有读到，或者该违法处理还没有交警出警
            ctx.timerService().registerEventTimeTimer(value.creatTime + 5000) //5秒后触发提示
            vState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"违法车辆【${value.car}】，违法时间【${value.creatTime}】,已经有交警出警了，警号为【${policeAction.policedId}】,出警的状态为【${policeAction.actionStatus}】,出警的时间为【${policeAction.actionTime}】")
            vState.clear()
            pState.clear()
          }
        }

        //当从第二个流中读取一条出警记录数据
        override def processElement2(value: PoliceAction, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
          println("processElement2 ==> " + value)
          val info: ViolationInfo = vState.value()
          if (info == null) { //出警记录没有找到对应的违法车辆信息
            ctx.timerService().registerEventTimeTimer(value.actionTime + 5000)
            pState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"违法车辆【${info.car}】，违法时间【${info.creatTime}】,已经有交警出警了，警号为【${value.policedId}】,出警的状态为【${value.actionStatus}】,出警的时间为【${value.actionTime}】")
            vState.clear()
            pState.clear()
          }
        }

        //触发器触发的函数
        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#OnTimerContext, out: Collector[String]) = {
          val info: ViolationInfo = vState.value()
          val action: PoliceAction = pState.value()
          if (info == null && action != null) { //表示有出警记录，但是没有匹配的违法车辆
            ctx.output(tag1, action)
          }
          if (action == null && info != null) { //有违法车辆信息，但是5分钟内还没有出警记录
            ctx.output(tag2, info)
          }
          //清空状态
          pState.clear()
          vState.clear()
        }
      })

    mainStream.print("main")
    mainStream.getSideOutput(tag1).print("没有对应的违法车辆信息")
    mainStream.getSideOutput(tag2).print("该违法车辆在5分钟内没有交警出警")

    streamEnv.execute()

  }
}

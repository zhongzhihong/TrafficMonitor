package com.zzh.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager, Put}

import java.util

class HBaseWriteDataSink extends RichSinkFunction[java.util.List[Put]] {

  private var conf: org.apache.hadoop.conf.Configuration = _
  var conn: HConnection = _

  override def open(parameters: Configuration): Unit = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "node01:2181")
    conn = HConnectionManager.createConnection(conf)
  }

  override def close(): Unit = conn.close()


  override def invoke(value: util.List[Put], context: SinkFunction.Context[_]): Unit = {
    val table = conn.getTable("t_track_info")
    table.put(value)
    table.flushCommits()
    table.close()
  }

}

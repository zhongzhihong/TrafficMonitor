package com.zzh.JavaLanguage.monitor;

import com.zzh.ScalaLanguage.util.JdbcReadSource;
import com.zzh.ScalaLanguage.util.MonitorLimitInfo;
import com.zzh.ScalaLanguage.util.TrafficLog;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OutOfSpeedMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.setProperty("group.id", "traffic01");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        props.setProperty("auto.offset.reset", "latest");

        // 广播状态流（数据量少，更新频率不高）
        // 1.读取一个Source得到一个流
        // 2.通过JobManager把流广播到所有的TaskManager
        // 3.调用connect算子和主流中的数据连接计算
        streamEnv.addSource(new JdbcReadSource<>(MonitorLimitInfo.class));

        // 注意topic需要提前存在
        DataStream<TrafficLog> mainStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>("traffic_zzh", new SimpleStringSchema(), props))
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficLog(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.parseDouble(arr[4]), arr[5], arr[6]
                    );
                });

        streamEnv.execute("OutOfSpeedMonitorJob");
    }
}

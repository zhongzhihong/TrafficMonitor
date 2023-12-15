package com.zzh.JavaLanguage.util;

import com.zzh.JavaLanguage.entity.MonitorLimitInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从数据库中读取数据作为一个数据流
 */
public class JdbcReadSource<T> extends RichSourceFunction<T> {
    private boolean flag = true;
    private Connection conn;
    private PreparedStatement pst;
    private ResultSet ret;
    private final Class<? extends T> classType;

    public JdbcReadSource(Class<? extends T> classType) {
        this.classType = classType;
    }

    @Override
    public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {
        while (flag) {
            ret = pst.executeQuery();
            if (classType.getName().equals(MonitorLimitInfo.class.getName())) {
                MonitorLimitInfo info = new MonitorLimitInfo(ret.getString(1), ret.getString(2), ret.getInt(3), ret.getString(4));
                sourceContext.collect((T) info);
            }
            ret.close();
            // 休眠一小时后从数据库中更新数据
            Thread.sleep(60 * 60 * 1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:/traffic_monitor", "root", "123456zzh");
        if (classType.getName().equals(MonitorLimitInfo.class.getName())) {
            pst = conn.prepareStatement("SELECT * FROM t_monitor_info WHERE speed_limit > 0");
        }
    }

    @Override
    public void close() throws Exception {
        if (pst != null) {
            pst.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}

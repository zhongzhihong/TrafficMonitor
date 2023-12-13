# TrafficMonitor
Flink城市交通监控项目

一、卡口分析
	监控车辆经过卡口（道路上用于监控的点，比如带摄像头的）的车速、每个卡口车流量的情况统计

二、实时报警
	监控违法之后一直未处理的违法车辆（违法王车辆：违法未处理超过50次以上的车辆），如若这种车上路则需要实时检测出来、实时检测套牌车的轨迹、连续超速的车辆需实时检测出来

三、智能车辆布控

四、项目数据处理
	首先由摄像头采集车辆数据，传到交通服务器当中，服务器通过http接口将数据发到Flume集群，Flume集群采集数据发到kafka集群中，最后通过运行Flink程序读取kafka中的数据来进行实时的分布式计算

五、项目数据字典：
（1）卡口车辆数据：
	(
	 `action_time` long  --摄像头拍摄时间戳（车辆经过卡口的时间）,
	 `monitor_id` string  --卡口号, 
	 `camera_id` string   --摄像头编号, 
	 `car` string  --车牌号码, 
	 `speed` double  --通过卡扣的速度, 
	 `road_id` string  --道路id, 
	 `area_id` string  --区域id, 
	)

（2）城市交通管理数据：
	2.1 城市区域表（t_area_info）：
		DROP TABLE IF EXISTS `t_area_info`;
		CREATE TABLE `t_area_info` (
		  `area_id` varchar(255) DEFAULT NULL,
		  `area_name` varchar(255) DEFAULT NULL
		)
		--导入数据
		INSERT INTO `t_area_info` VALUES ('01', '海淀区');
		INSERT INTO `t_area_info` VALUES ('02', '昌平区');
		INSERT INTO `t_area_info` VALUES ('03', '朝阳区');
		INSERT INTO `t_area_info` VALUES ('04', '顺义区');
		INSERT INTO `t_area_info` VALUES ('05', '西城区');
		INSERT INTO `t_area_info` VALUES ('06', '东城区');
		INSERT INTO `t_area_info` VALUES ('07', '大兴区');
		INSERT INTO `t_area_info` VALUES ('08', '石景山');

	2.2 城市违法车辆表（t_violation_list）：
		DROP TABLE IF EXISTS `t_violation_list`;
		CREATE TABLE `t_violation_list` (
		  `car` varchar(255) NOT NULL,
		  `violation` varchar(1000) DEFAULT NULL,
		  `create_time` bigint(20) DEFAULT NULL,
		  PRIMARY KEY (`car`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		--导入数据
		INSERT INTO `t_violation_list` VALUES ('京P88888', '违章未处理超过89次',null);
		INSERT INTO `t_violation_list` VALUES ('京P99999', '违章未处理超过239次',null);
		INSERT INTO `t_violation_list` VALUES ('京P77777', '违章未处理超过567次',null);
		INSERT INTO `t_violation_list` VALUES ('京P66666', '嫌疑套牌车',null);
		INSERT INTO `t_violation_list` VALUES ('京P55555', '嫌疑套牌车',null);
		INSERT INTO `t_violation_list` VALUES ('京P44444', '嫌疑套牌车',null);
		INSERT INTO `t_violation_list` VALUES ('京P33333', '违章未处理超过123次',null);
		INSERT INTO `t_violation_list` VALUES ('京P22222', '违章未处理超过432次',null);

	2.3 城市卡口限速表：
		DROP TABLE IF EXISTS `t_monitor_info`;
		CREATE TABLE `t_monitor_info` (
		  `monitor_id` varchar(255) NOT NULL,
		  `road_id` varchar(255) NOT NULL,
		  `speed_limit` int(11) DEFAULT NULL,
		  `area_id` varchar(255) DEFAULT NULL,
		  PRIMARY KEY (`monitor_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		--导入数据
		INSERT INTO `t_monitor_info` VALUES ('0000', '02', 60, '01');
		INSERT INTO `t_monitor_info` VALUES ('0001', '02', 60, '02');
		INSERT INTO `t_monitor_info` VALUES ('0002', '03', 80, '01');
		INSERT INTO `t_monitor_info` VALUES ('0004', '05', 100, '03');
		INSERT INTO `t_monitor_info` VALUES ('0005', '04', 0, NULL);
		INSERT INTO `t_monitor_info` VALUES ('0021', '04', 0, NULL);
		INSERT INTO `t_monitor_info` VALUES ('0023', '05', 0, NULL);

（3）车辆轨迹数据：
	DROP TABLE IF EXISTS `t_track_info`;
	CREATE TABLE `t_track_info` (
	  `id` int(11) NOT NULL AUTO_INCREMENT,
	  `car` varchar(255) DEFAULT NULL,
	  `action_time` bigint(20) DEFAULT NULL,
	  `monitor_id` varchar(255) DEFAULT NULL,
	  `road_id` varchar(255) DEFAULT NULL,
	  `area_id` varchar(255) DEFAULT NULL,
	  `speed` double DEFAULT NULL,
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;


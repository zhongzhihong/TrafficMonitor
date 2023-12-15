package com.zzh.JavaLanguage.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficLog {
    private long actionTime;
    private String monitorId;
    private String cameraId;
    private String car;
    private double speed;
    private String roadId;
    private String areaId;
}

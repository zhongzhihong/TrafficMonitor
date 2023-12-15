package com.zzh.JavaLanguage.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonitorLimitInfo {
    private String monitorId;
    private String roadId;
    private int speedLimit;
    private String areaId;
}

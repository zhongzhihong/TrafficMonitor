package com.zzh.JavaLanguage;

import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class MakeData {
    public static void main(String[] args) throws IOException {
        FileOutputStream outputStream = null;
        PrintWriter printWriter = null;
        Random random = new Random();
        KafkaProducer<String, String> producer = null;

        String[] locations = {"京", "津", "冀", "京", "鲁", "京", "京", "京", "京", "京"};
        String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.84.136:9092,172.16.84.137:9092,172.16.84.138:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        try {
            outputStream = new FileOutputStream("./data/test");
            printWriter = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            producer = new KafkaProducer<>(properties);
            GaussianRandomGenerator gaussianRandomGenerator = new GaussianRandomGenerator(new JDKRandomGenerator(10));
            for (int i = 0; i < 3000; i++) {
                String car = locations[random.nextInt(locations.length)] + (65 + random.nextInt(26)) + String.format("%05d", random.nextInt(100000));
                String startHour = String.format("%02d", random.nextInt(24));
                double g = gaussianRandomGenerator.nextNormalizedDouble();
                int m_count = (int) (Math.abs(30 + (30 * g)) + 1);
                for (int j = 0; j < m_count; j++) {
                    if (j % 30 == 0) {
                        int newHour = Integer.parseInt(startHour) + 1;
                        if (newHour == 24) {
                            newHour = 0;
                        }
                        startHour = String.format("%02d", newHour);
                    }
                    String actionTime = day + " " + startHour + ":" + String.format("%02d", random.nextInt(60)) + ":" + String.format("%02d", random.nextInt(60));
                    Date realTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(actionTime);
                    String monitorId = String.format("%04d", random.nextInt(50));
                    String speed = String.format("%.1f", Math.abs(50 + (50 * g) + 1));
                    String roadId = String.format("%03d", random.nextInt(1000));
                    String cameraId = String.format("%05d", random.nextInt(100000));
                    String areaId = String.format("%02d", random.nextInt(8));
                    String content = realTime.getTime() + "," + monitorId + "," + cameraId + "," + car + "," + speed + "," + roadId + "," + areaId;
                    printWriter.write(content + "\n");
                    ProducerRecord<String, String> record = new ProducerRecord<>("traffic_zzh", content);
                    producer.send(record);
                }
                printWriter.flush();
            }
            printWriter.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (printWriter != null) {
                printWriter.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
            if (producer != null) {
                producer.close();
            }
        }
    }
}

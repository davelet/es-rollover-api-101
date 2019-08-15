package com.j11.es.rollover;

import com.j11.es.dto.RiderLocationTimeSerialDTO;
import com.j11.test.es.Controller;

import java.time.LocalDateTime;
import com.alibaba.fastjson.JSON;
/**
 * <pre>
 * <b>Description</b>
 * </pre>
 * <pre>
 * 创建时间 2019-08-15 17:17
 * 所属工程： esrollover  </pre>
 *
 * @author sheldon yhid: 80752866
 */
public class App {
    public static void main(String[] args) {
        Controller controller = new Controller();

        RiderLocationTimeSerialDTO dto = new RiderLocationTimeSerialDTO();
        dto.setRiderId(444L);
        dto.setLongitude("120.8438474");
        dto.setLatitude("29.9834793");
        dto.setAccuracy(3.0D);

        LocalDateTime now = LocalDateTime.now();
        while (now.isBefore(LocalDateTime.of(2019, 10, 10, 20, 9))) {
            dto.setTime(now);
            controller.save(JSON.toJSONString(dto));
            now = now.plusHours(10);
        }
    }
}

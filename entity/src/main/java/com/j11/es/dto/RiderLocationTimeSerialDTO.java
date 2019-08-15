package com.j11.es.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Getter
@Setter
@ToString
public class RiderLocationTimeSerialDTO {
    private static final long serialVersionUID = 8470753004224336630L;

    private Long riderId;
    private LocalDateTime time;
    private String longitude;
    private String latitude;
    private Double accuracy;
}
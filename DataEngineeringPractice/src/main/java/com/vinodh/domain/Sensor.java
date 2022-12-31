package com.vinodh.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Date;


@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {
    private Date reportTs;
    private String reportType;
    private int reportValue;
}

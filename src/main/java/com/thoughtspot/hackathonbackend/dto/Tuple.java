package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

import java.util.List;

@Data
public class Tuple {
    int dimenstion;
    List<String> originalValues;
    List<Double> coordinates;
    int clusterId;
}

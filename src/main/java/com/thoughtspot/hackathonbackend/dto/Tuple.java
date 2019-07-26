package com.thoughtspot.hackathonbackend.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class Tuple {
    int dimension;
    List<String> originalValues;
    List<Double> coordinates;
    int clusterId;

    public Tuple(int dimension, List<String> originalValues, List<Double> coordinates, int clusterId) {
        this.dimension = dimension;
        this.originalValues = originalValues;
        this.coordinates = coordinates;
        this.clusterId = clusterId;
    }
}

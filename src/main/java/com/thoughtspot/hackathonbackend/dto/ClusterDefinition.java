package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

import java.util.List;

@Data
public class ClusterDefinition {
    int numberOfCluster;
    List<Tuple> centroid;
    List<Tuple> datapoints;
    // TODO capture min max etc.
}

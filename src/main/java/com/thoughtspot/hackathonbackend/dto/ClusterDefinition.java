package com.thoughtspot.hackathonbackend.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class ClusterDefinition {
    int numberOfCluster;
    List<Tuple> centroid;
    List<Tuple> datapoints;
    // TODO capture min max etc.


    public ClusterDefinition(int numberOfCluster, List<Tuple> centroid, List<Tuple> datapoints) {
        this.numberOfCluster = numberOfCluster;
        this.centroid = centroid;
        this.datapoints = datapoints;
    }
}

package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

import java.util.List;

@Data
public class ClusterDefinition {
    public int numberOfCluster;
    public List<Tuple> centroid;
    // TODO capture min max etc.
}

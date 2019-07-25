package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

@Data
public class ClusteringInput {
    public enum ClusteringMethod {
        KMEANS, DBSCAN
    }
    private ClusteringMethod clusteringMethod;
    private int targetDims;
    private int numClusters;
    private int numIters;
    private int dimsThreshold;
    private CustomDataset data;
}

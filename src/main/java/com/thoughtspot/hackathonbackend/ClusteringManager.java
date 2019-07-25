package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.dto.ClusterDefinition;
import com.thoughtspot.hackathonbackend.dto.ClusteringInput.*;
import com.thoughtspot.hackathonbackend.dto.Column;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.stream.Collectors;

public class ClusteringManager {

    public ClusterDefinition start(Dataset<Row> df, ClusteringMethod m,
                                    int targetDims, int numClusters,
                                    int numIter, int dimsThreshold) {

        long count = dataset.count();
        System.out.println(count);
        List<String> cols = data.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        VectorizeDS vectorHelper = new VectorizeDS(50, cols);
        vectorHelper.getVectorizedDS(dataset);
        return clusterDiffs;
    }
}

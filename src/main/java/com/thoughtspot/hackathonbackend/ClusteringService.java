package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.dto.ClusterDefinition;
import com.thoughtspot.hackathonbackend.dto.ClusteringInput;
import com.thoughtspot.hackathonbackend.dto.Column;
import com.thoughtspot.hackathonbackend.dto.CustomDataset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.spark.mllib.linalg.Vector;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ClusteringService implements Serializable{

    @Autowired
    transient JavaSparkContext sc;

    public ClusterDefinition getClustering(ClusteringInput input) {

        ClusteringManager manager = new ClusteringManager(sc);
        manager.configureManager(input);
        manager.createDataFrame(input);
        manager.createDataCols(input);
        return manager.createClusters();

    }

}

package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.dto.ClusteringInput;
import com.thoughtspot.hackathonbackend.dto.Column;
import com.thoughtspot.hackathonbackend.dto.CustomDataset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClusteringService {

    @Autowired
    JavaSparkContext sc;

    public Map<String, Long> getCount(List<String> wordList) {
        JavaRDD<String> words = sc.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();
        return wordCounts;

    }

    public Map<String,Long> getClustering(ClusteringInput input) {

        JavaRDD<Row> stringRdd = sc.parallelize(input.getData().getValues()).map(x -> {
            return RowFactory.create(x.split(","));
        });
        SparkSession spark = SparkSession.builder().config(sc.getConf()).getOrCreate();
        Dataset<Row> dataset = spark.createDataFrame(stringRdd, getSchema(input.getData()));

        long count = dataset.count();
        System.out.println(count);
        return null;
    }

    public StructType getSchema(CustomDataset  customDataset) {
        StructField[] fields = new StructField[customDataset.columns.size()];
        int i = 0;
        for (Column columns : customDataset.columns) {
            DataType dataType;
            switch (columns.dataType) {
                case INT64: dataType = DataTypes.IntegerType; break;
                case VARCHAR: dataType = DataTypes.StringType; break;
                case DOUBLE: dataType = DataTypes.DoubleType; break;
                default: dataType = DataTypes.StringType;
            }
            fields[i] = new StructField(columns.name, dataType, true, Metadata.empty());
            i++;
        }
        return new StructType(fields);
    }
}

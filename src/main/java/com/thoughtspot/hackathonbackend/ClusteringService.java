package com.thoughtspot.hackathonbackend;

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

    public Map<String, Long> getCount(List<String> wordList) {
        JavaRDD<String> words = sc.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();
        Test.fun(sc);
        return wordCounts;

    }

    public Map<String,Long> getClustering(CustomDataset customDataset) {
        JavaRDD<Row> stringRdd = sc.parallelize(customDataset.getValues()).map(x -> {
            List<Column> columns = customDataset.getColumns();
            int i = 0;
            Object[] row = new Object[x.size()];
            for (String col : x) {
                row[i] = getDatatype(col , columns.get(i).getDataType());
                i++;
            }
            return RowFactory.create(row);
        });
        SparkSession spark = SparkSession.builder().config(sc.getConf()).getOrCreate();
        Dataset<Row> dataset = spark.createDataFrame(stringRdd, getSchema(customDataset));

        long count = dataset.count();
        System.out.println(count);
        List<String> cols = customDataset.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        VectorizeDS vectorHelper = new VectorizeDS(50, cols);
        vectorHelper.getVectorizedDS(dataset);
        return null;
    }

    public StructType getSchema(CustomDataset  customDataset) {
        StructField[] fields = new StructField[customDataset.getColumns().size()];
        int i = 0;
        for (Column columns : customDataset.getColumns()) {
            DataType dataType;
            switch (columns.getDataType()) {
                case INT64: dataType = DataTypes.IntegerType; break;
                case VARCHAR: dataType = DataTypes.StringType; break;
                case DOUBLE: dataType = DataTypes.DoubleType; break;
                default: dataType = DataTypes.StringType;
            }
            fields[i] = new StructField(columns.getName(), dataType, true, Metadata.empty());
            i++;
        }
        return new StructType(fields);
    }

    public Object getDatatype(String colValue, com.thoughtspot.hackathonbackend.dto.DataType type) {
       switch (type) {
                case INT64: return Integer.parseInt(colValue);
                case VARCHAR: return colValue;
                case DOUBLE: return Double.parseDouble(colValue);
                default: return colValue;
        }
    }
}

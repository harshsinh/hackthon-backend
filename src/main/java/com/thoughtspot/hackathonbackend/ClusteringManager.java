package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.ClusteringAlgorithm.KMeansClustering;
import com.thoughtspot.hackathonbackend.DimensionReduction.PCAAlgorithm;
import com.thoughtspot.hackathonbackend.dto.*;
import com.thoughtspot.hackathonbackend.dto.ClusteringInput.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.spark_project.guava.primitives.Doubles;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClusteringManager implements Serializable {

    public ClusteringManager (JavaSparkContext sc) {
        sc_ = sc;
    }

    public void configureManager (ClusteringInput input) {
        meth_  = input.getClusteringMethod();
        thres_ = input.getDimsThreshold();
        numC_  = input.getNumClusters();
        tarD_  = input.getTargetDims();
        numI_  = input.getNumIters();
    }

    public void createDataFrame (ClusteringInput input) {

        CustomDataset data = input.getData();
        JavaRDD<Row> stringRdd = sc_.parallelize(data.getValues()).map(x -> {
            List<Column> columns = data.getColumns();
            int i = 0;
            Object[] row = new Object[x.size()];
            for (String col : x) {
                row[i] = getDatatype(col , columns.get(i).getDataType());
                i++;
            }
            return RowFactory.create(row);
        });
        SparkSession spark = SparkSession.builder().config(sc_.getConf()).getOrCreate();
        df_ = spark.createDataFrame(stringRdd, getSchema(input.getData()));


        long count = df_.count();
        System.out.println(count);
    }

    public void createDataCols (ClusteringInput input) {
        cols_ = input.getData().getColumns().stream().map(Column::getName).collect(Collectors.toList());
    }

    public ClusterDefinition createClusters () {

        VectorizeDS vectorHelper = new VectorizeDS(thres_, cols_);
        JavaRDD<Vector> vectorizedDS = vectorHelper.getVectorizedDS(df_);

        RDD<LabeledPoint> clusteredPts;
        System.out.println(meth_.toString());
        switch (meth_) {
            case KMEANS: clusteredPts = KMeansClustering.kmeansImp(vectorizedDS.rdd(), numC_, numI_); break;
            default: clusteredPts = KMeansClustering.kmeansImp(vectorizedDS.rdd(), numC_, numI_);
        }

//        LabeledPoint[] clusteredCollected = clusteredPts.take(((int) clusteredPts.count()));

        List<LabeledPoint> clusteredCollected = clusteredPts.toJavaRDD().collect();

        RDD<LabeledPoint> clusteredPtsPCA = PCAAlgorithm.applyPCA(tarD_, clusteredPts);
        ClusterDefinition clusterDefinition = new ClusterDefinition();
        clusterDefinition.setNumberOfCluster(numC_);

        List<LabeledPoint> clusteredPtsPCAJ = clusteredPtsPCA.toJavaRDD().collect();

        List<Tuple> dataPoints = new ArrayList<Tuple>();
        int i=0;
        for (LabeledPoint lp : clusteredPtsPCAJ) {
            Tuple temp = new Tuple();
            temp.setClusterId(((int) lp.label()));
            temp.setDimenstion(tarD_);
            temp.setOriginalValues(Arrays.asList(clusteredCollected.get(i).features().toString().split(",")));
            temp.setCoordinates(Doubles.asList(lp.features().toArray()));
            dataPoints.add(temp);
            i++;
        }
        clusterDefinition.setDatapoints(dataPoints);
        return clusterDefinition;
    }

    private StructType getSchema(CustomDataset  customDataset) {
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

    private Object getDatatype(String colValue, com.thoughtspot.hackathonbackend.dto.DataType type) {
        switch (type) {
            case INT64: return Integer.parseInt(colValue);
            case VARCHAR: return colValue;
            case DOUBLE: return Double.parseDouble(colValue);
            default: return colValue;
        }
    }

    private Dataset<Row> df_;
    private List<String> cols_;
    private static JavaSparkContext sc_;
    private int numC_, numI_, thres_, tarD_;
    private ClusteringMethod meth_;
}

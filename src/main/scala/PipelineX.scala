package com.thoughtspot.hackathonbackend

import com.thoughtspot.hackathonbackend.dto.{ClusterDefinition, Tuple}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{Normalizer, PCA}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by vikas.singh on 26/07/19.
  */
object PipelineX extends Serializable {

  def createClusters(df : Dataset[Row], threshold : Int, cols : java.util.List[String], pca_dimensions:Int) : ClusterDefinition = {
    val encoded = setEncoding(df, threshold, cols);
    println( "Count after encoding:" + encoded.count())
    val eCols : Seq[String] = encoded.columns.toSeq
    val col2 = eCols.map( x => x);
    encoded.foreach(x => println(x.length));
    var rowDataset:org.apache.spark.rdd.RDD[( org.apache.spark.mllib.linalg.Vector)] = encoded.rdd.map(x => getVectorizedRow(x, col2))
    val labeledPoints:Array[LabeledPoint] = createClusterDef(rowDataset, pca_dimensions);
    val originalPoints:Array[Row] = df.collect();
    val datapoints:Array[Tuple] = labeledPoints.zip(originalPoints).map(x => {
      val l:Seq[java.lang.Double] = x._1.features.toArray.toSeq.map(x => x.asInstanceOf[java.lang.Double])
      new Tuple(
        3,
        scala.collection.JavaConverters.seqAsJavaList[String](x._2.toSeq.map(String.valueOf(_))),
        l,
        x._1.label.toInt)
    });
    new ClusterDefinition(4, new java.util.ArrayList(), scala.collection.JavaConverters.seqAsJavaList[Tuple](datapoints.toSeq));
  }

  def createClusterDef(rowDataset: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector], pca_dimensions:Int) : Array[LabeledPoint] = {
    val clusters = KMeans.train(rowDataset, 4, 100)
    var ret = rowDataset.map(x => warappedPredict(clusters, x))
    val pca = new PCA(pca_dimensions).fit(ret.map(_.features))
    val projected:RDD[LabeledPoint] = ret.map(p => p.copy(features = pca.transform(p.features)))
    val normalizer = new Normalizer()
    projected.collect.map(x => new LabeledPoint(x.label, normalizer.transform(x.features)))
  }

  def warappedPredict(clusters: org.apache.spark.mllib.clustering.KMeansModel, x: org.apache.spark.mllib.linalg.Vector): LabeledPoint = {
    val pred = clusters.predict(x)
    new LabeledPoint(pred, x)
  }


  def setEncoding(df : Dataset[Row], threshold : Int, cols : java.util.List[String]): Dataset[Row] = {
    var transformedDf = df
    var columnCount = cols.size - 1
    var j = 0
    val columnClassification = classifyColumnsBasedOnCardinality(df, threshold, cols)

    while (j <= columnCount) {
      if  (columnClassification(j) == 1) {
        val indexer = new StringIndexer()
          .setInputCol(cols(j))
          .setOutputCol(cols(j)+"Index")
          .fit(transformedDf)
        val indexed = indexer.transform(transformedDf)
        val encoder = new OneHotEncoder()
          .setInputCol(cols(j)+"Index")
          .setOutputCol(cols(j)+"Vec")

        transformedDf = encoder.transform(indexed).drop(cols(j)).drop(cols(j)+ "Index")
      }
      else if (columnClassification(j) == -1) {
        transformedDf = transformedDf.drop(cols(j))
      }
      j = j + 1
      println("TransformedDf" + transformedDf.first());
      println("Done");
    }
    transformedDf
  }

  def classifyColumnsBasedOnCardinality(ds: Dataset[Row], threshold : Int, cols : java.util.List[String]): Array[Int] = {
    var i = 0
    val df_col_types = Array.fill(cols.size)(-1)
    var dataset = ds.collectAsList()
    println("Cols size : " + cols.size())
    for (i <- cols.indices) {
      val distinctCount = ds.select(cols(i)).collect().distinct.size
      println("Distinct count : "+ distinctCount + " for : " + cols(i))
      if (distinctCount <= threshold && distinctCount >= 2) {
        df_col_types(i) = 1
      }
      else if (distinctCount < 2) {
        println("no unique values more than 2 for index " + i)
      }
      else {
        try {
          if (ds.select(cols(i)).collect()(2)(0).getClass.getName.equals("java.lang.Integer")
            || ds.select(cols(i)).collect()(2)(0).getClass.getName.equals("java.lang.Double")) {
            df_col_types(i) = 2
          }
        }
        catch {
          case x: Exception => {
            println("Culd not cast high dimentality into int for index " + i)
          }
        }
      }
    }
    df_col_types
  }

  val getVectorizedRow = (x : Row, eCols : Seq[String]) => {
    var i = 0
    var arr = ArrayBuffer[Double]()
    while(i < eCols.size) {
      if(x.get(i).isInstanceOf[org.apache.spark.ml.linalg.SparseVector]) {
        arr ++= x.getAs[org.apache.spark.ml.linalg.SparseVector](i).toDense.toArray
      } else {
        arr ++= Array(x.getAs[Int](i).toDouble)
      }
      i += 1
    }
    org.apache.spark.mllib.linalg.Vectors.dense(arr.toArray)
  }

}

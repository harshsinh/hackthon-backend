package com.thoughtspot.hackathonbackend

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class VectorizeDS(threshold : Int, cols : java.util.List[String]) extends Serializable {

  def setCardinalityType(ds: Dataset[Row]): Array[Int] = {
    var i = 0
    val df_col_types = Array.fill(cols.size)(-1)
    var dataset = ds.collectAsList()
    println("Cols size : " + cols.size())
    for (i <- cols.indices) {
      val distinctCount = ds.select(cols(i)).distinct.count
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

  def setEncoding(df : DataFrame): DataFrame = {
    var encoded = df
    var col =  cols.size - 1
    var j = 0
    val df_col_types = setCardinalityType(df)
    while (j <= col) {
      if  (df_col_types(j) == 1) {
//        println(j)
        val indexer = new StringIndexer()
          .setInputCol(cols(j))
          .setOutputCol(cols(j)+"Index")
          .fit(encoded)
        val indexed = indexer.transform(encoded)
        val encoder = new OneHotEncoder()
          .setInputCol(cols(j)+"Index")
          .setOutputCol(cols(j)+"Vec")

        encoded = encoder.transform(indexed).drop(cols(j)).drop(cols(j)+ "Index")
      }
      else if (df_col_types(j) == -1) {
        encoded = encoded.drop(cols(j))
      }
      j = j + 1
    }
    encoded
  }

  val getVectorizedRow = (x : Row, eCols : Seq[String]) => {
    var i = 0
    var arr = ArrayBuffer[Double]()
    while(i < eCols.size) {
      if(x.get(i).isInstanceOf[org.apache.spark.ml.linalg.SparseVector]) {
        //arr.addAll(x.getAs[org.apache.spark.ml.linalg.SparseVector](i).toDense.toArray.)
        arr ++= x.getAs[org.apache.spark.ml.linalg.SparseVector](i).toDense.toArray
      } else {
        arr ++= Array(x.getAs[Int](i).toDouble)
      }
      i += 1
    }
    org.apache.spark.mllib.linalg.Vectors.dense(arr.toArray)

  }

  def getVectorizedDS(df : DataFrame): JavaRDD[org.apache.spark.mllib.linalg.Vector] = {
//    val ds:Dataset[Row] = df.as[Row]
    df.printSchema()
    val encoded = setEncoding(df);
    println( "Count after encoding:" + encoded.count())
    val eCols : Seq[String] = encoded.columns.toSeq
//    val col2 = eCols.map( x => x);
//    encoded.foreach(x => println(x.length));
    var rowDataset = encoded.rdd.map(x => getVectorizedRow(x, eCols))
//    val take = rowDataset.take(1)
//    for (row <- take) {
//      System.out.println(row.toJson)
//    }
    rowDataset.toJavaRDD()
  }
}

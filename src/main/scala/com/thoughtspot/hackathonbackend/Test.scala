package com.thoughtspot.hackathonbackend

import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.{JavaSparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object Test {

  def fun(sc:JavaSparkContext):Unit = {
    println("hello world" + sc.appName);
  }

  def clusterBackend(ds: Dataset[Row], spark: SparkSession):Unit = {
    import spark.implicits._
    println("Count inside Test class : " + ds.count())

  }
}

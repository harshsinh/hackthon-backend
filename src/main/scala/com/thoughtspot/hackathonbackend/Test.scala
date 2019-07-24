package com.thoughtspot.hackathonbackend

import org.apache.spark.api.java.JavaSparkContext

object Test {

  def fun(sc:JavaSparkContext):Unit = {
    println("hello world" + sc.appName);
  }
}

package com.thoughtspot.hackathonbackend.DimensionReduction

import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object PCAAlgorithm {

  def applyPCA(pcaDimensions:Int, vectorizedClusters: RDD[LabeledPoint]):RDD[LabeledPoint] = {
    val pca = new PCA(pcaDimensions).fit(vectorizedClusters.map(_.features))
    val projected = vectorizedClusters.map(p => p.copy(features = pca.transform(p.features)))
    projected
  }
}

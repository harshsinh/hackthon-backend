package com.thoughtspot.hackathonbackend.ClusteringAlgorithm

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object KMeansClustering {

  def getCentroids(
                    vectorizedMap: RDD[Vector],
                    numClusters: Int,
                    numIterations : Int):KMeansModel = {
    KMeans.train(vectorizedMap, numClusters, numIterations)
  }

  def getClusters(centroid: Vector, clusters: KMeansModel): LabeledPoint = {
    val pred = clusters.predict(centroid)
    new LabeledPoint(pred, centroid)
  }

  def kmeansImp(vectorizedMap: RDD[Vector], numClusters: Int, numIterations : Int): RDD[LabeledPoint] = {
    val clusters = getCentroids(vectorizedMap, numClusters, numIterations)
    val vectorizedClusters = vectorizedMap.map(centroid => getClusters(centroid, clusters))
    vectorizedClusters
  }
}

package com.github.mrpowers.spark.spec.ml.clustering

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.scalatest.FunSpec

class IrisKMeansClusteringSpec
  extends FunSpec
  with SparkSessionTestWrapper
  with ColumnComparer {

  describe("withVectorizedFeatures") {

    it("converts all the features to a vector without blowing up") {

      val df = spark.createDF(
        List(
          (5.1, 3.5, 1.4, 0.2)
        ), List(
          ("SepalLengthCm", DoubleType, true),
          ("SepalWidthCm", DoubleType, true),
          ("PetalLengthCm", DoubleType, true),
          ("PetalWidthCm", DoubleType, true)
        )
      ).transform(IrisKMeansClustering.withVectorizedFeatures())

      df.show()
      df.printSchema()

    }

  }

  describe("model") {

    it("prints the cluster centers") {

      println("Cluster Centers: ")
      IrisKMeansClustering.model().clusterCenters.foreach(println)

    }

    it("trains a KMeans Clustering model that's Silhouette with squared euclidean distance above 0.70 percent") {

      val trainData: DataFrame = IrisKMeansClustering.trainingDF
        .transform(IrisKMeansClustering.withVectorizedFeatures())
        .select("features")

      val testData: DataFrame = IrisKMeansClustering.testDF
        .transform(IrisKMeansClustering.withVectorizedFeatures())
        .select("features")

      val predictions: DataFrame = IrisKMeansClustering
        .model()
        .transform(testData)
        .select(
          col("features"),
          col("prediction")
        )

      val res = new ClusteringEvaluator()
        .evaluate(predictions)

      assert(res >= 0.60)
    }

  }

}

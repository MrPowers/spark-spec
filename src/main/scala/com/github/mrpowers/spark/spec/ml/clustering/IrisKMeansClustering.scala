package com.github.mrpowers.spark.spec.ml.clustering

import com.github.mrpowers.spark.spec.sql.SparkSessionWrapper
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object IrisKMeansClustering
  extends SparkSessionWrapper {

  val irisDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./src/test/resources/iris/iris.csv")

  val Array(trainingDF, testDF) = irisDF.randomSplit(Array(0.7, 0.3), seed = 12345)

  def withVectorizedFeatures(
    featureColNames: Array[String] = Array("SepalLengthCm", "SepalLengthCm", "PetalLengthCm", "PetalWidthCm"),
    outputColName: String = "features"
  )(df: DataFrame): DataFrame = {
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureColNames)
      .setOutputCol(outputColName)
    assembler.transform(df)
  }

  def model(df: DataFrame = trainingDF): KMeansModel = {
    val trainFeatures: DataFrame = df
      .transform(withVectorizedFeatures())

    new KMeans()
      .setK(3) // # of clusters
      .setSeed(2L)
      .fit(trainFeatures)
  }

  def persistModel(): Unit = {
    model().save("./tmp/iris_kMeans_model/")
  }

}

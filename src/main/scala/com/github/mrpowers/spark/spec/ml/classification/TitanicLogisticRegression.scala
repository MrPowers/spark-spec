package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.sql.SparkSessionWrapper
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object TitanicLogisticRegression extends SparkSessionWrapper {

  def vectorizeFeatures()(df: DataFrame): DataFrame = {
    val featureCols: Array[String] = Array("Gender", "Age", "SibSp", "Parch", "Fare")
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    assembler.transform(df)
  }

  def model(): LogisticRegressionModel = {
    val trainFeatures: DataFrame = TitanicData
      .trainingDF()
      .transform(vectorizeFeatures())

    val labelIndexer: StringIndexer = new StringIndexer()
      .setInputCol("Survived")
      .setOutputCol("label")

    val trainWithFeaturesAndLabel: DataFrame = labelIndexer
      .fit(trainFeatures)
      .transform(trainFeatures)

    new LogisticRegression()
      .fit(trainWithFeaturesAndLabel)
  }

}

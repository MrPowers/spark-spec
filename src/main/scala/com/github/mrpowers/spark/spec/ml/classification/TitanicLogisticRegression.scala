package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.sql.SparkSessionWrapper
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object TitanicLogisticRegression extends SparkSessionWrapper {

  def withVectorizedFeatures(
    featureColNames: Array[String] = Array("Gender", "Age", "SibSp", "Parch", "Fare"),
    outputColName: String = "features"
  )(df: DataFrame): DataFrame = {
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureColNames)
      .setOutputCol(outputColName)
    assembler.transform(df)
  }

  def withLabel(
    inputColName: String = "Survived",
    outputColName: String = "label"
  )(df: DataFrame) = {
    val labelIndexer: StringIndexer = new StringIndexer()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    labelIndexer
      .fit(df)
      .transform(df)
  }

  def model(): LogisticRegressionModel = {
    val trainFeatures: DataFrame = TitanicData
      .trainingDF()
      .transform(withVectorizedFeatures())
      .transform(withLabel())

    new LogisticRegression()
      .fit(trainFeatures)
  }

}

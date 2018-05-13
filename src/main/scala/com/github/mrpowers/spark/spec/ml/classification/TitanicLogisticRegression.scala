package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.sql.SparkSessionWrapper
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object TitanicLogisticRegression extends SparkSessionWrapper {

  // training

  val featureCols = Array("Gender", "Age", "SibSp", "Parch", "Fare")

  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

  val trainFeatures = assembler.transform(TitanicData.trainingDF())

  val labelIndexer = new StringIndexer()
    .setInputCol("Survived")
    .setOutputCol("label")

  val trainWithFeaturesAndLabel = labelIndexer
    .fit(trainFeatures)
    .transform(trainFeatures)

  val model = new LogisticRegression()
    .fit(trainWithFeaturesAndLabel)

  // test

  val testFeatures = assembler.transform(TitanicData.testDF())

  val predictions = model
    .transform(testFeatures)

}

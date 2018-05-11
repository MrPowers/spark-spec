package com.github.mrpowers.spark.spec.sql

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

object TitanicLogisticRegression
  extends SparkSessionWrapper {

  private val base_path = "./src/test/resources/titanic/"

  val trainDF = spark
    .read
    .option("header", "true")
    .csv(base_path + "train.csv")

  val testDF = spark
    .read
    .option("header", "true")
    .csv(base_path + "test.csv")

  private[spec] val resultDF = spark
    .read
    .option("header", "true")
    .csv(base_path + "gender_submission.csv")

  val trainCleanDF = trainDF
    .withColumn("Gender", when(col("Sex").equalTo("male"), 0)
      .when(col("Sex").equalTo("female"), 1)
      .otherwise(null))
    .select(
      col("Gender").cast("double"),
      col("Survived").cast("double"),
      col("Pclass").cast("double"),
      col("Age").cast("double"),
      col("SibSp").cast("double"),
      col("Parch").cast("double"),
      col("Fare").cast("double")
    )
    .filter(col("Gender").isNotNull &&
      col("Survived").isNotNull &&
      col("Pclass").isNotNull &&
      col("Age").isNotNull &&
      col("SibSp").isNotNull &&
      col("Parch").isNotNull &&
      col("Fare").isNotNull)

  val testCleanDF = testDF
    .join(resultDF, Seq("PassengerId"))
    .withColumn("Gender", when(col("Sex").equalTo("male"), 0)
      .when(col("Sex").equalTo("female"), 1)
      .otherwise(null))
    .select(
      col("Gender").cast("double"),
      col("Survived").cast("double"),
      col("Pclass").cast("double"),
      col("Age").cast("double"),
      col("SibSp").cast("double"),
      col("Parch").cast("double"),
      col("Fare").cast("double")
    )
    .filter(col("Gender").isNotNull &&
      col("Pclass").isNotNull &&
      col("Age").isNotNull &&
      col("SibSp").isNotNull &&
      col("Parch").isNotNull &&
      col("Fare").isNotNull)

  val featureCols = Array("Gender", "Age", "SibSp", "Parch", "Fare")
  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

  val trainFeatures = assembler.transform(trainCleanDF)

  val testFeatures = assembler.transform(testCleanDF)

  val labelIndexer = new StringIndexer().setInputCol("Survived")
    .setOutputCol("label")

  val trainWithFeatuersAndLabel = labelIndexer.fit(trainFeatures)
    .transform(trainFeatures)

  val model = new LogisticRegression()
    .fit(trainWithFeatuersAndLabel)

  val predictions = model
    .transform(testFeatures)

}

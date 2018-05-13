package com.github.mrpowers.spark.spec.ml.classification

import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.spec.sql.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

object TitanicData extends SparkSessionWrapper {

  def trainingDF(
    titanicDataDirName: String = "./src/test/resources/titanic/"
  ): DataFrame = {
    spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "train.csv")
      .withColumn(
        "Gender",
        when(
          col("Sex").equalTo("male"), 0
        )
          .when(col("Sex").equalTo("female"), 1)
          .otherwise(null)
      )
      .select(
        col("Gender").cast("double"),
        col("Survived").cast("double"),
        col("Pclass").cast("double"),
        col("Age").cast("double"),
        col("SibSp").cast("double"),
        col("Parch").cast("double"),
        col("Fare").cast("double")
      )
      .filter(
        col("Gender").isNotNull &&
          col("Survived").isNotNull &&
          col("Pclass").isNotNull &&
          col("Age").isNotNull &&
          col("SibSp").isNotNull &&
          col("Parch").isNotNull &&
          col("Fare").isNotNull
      )
  }

  def testDF(
    titanicDataDirName: String = "./src/test/resources/titanic/"
  ): DataFrame = {
    val rawTestDF = spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "test.csv")

    val genderSubmissionDF = spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "gender_submission.csv")

    rawTestDF
      .join(
        genderSubmissionDF,
        Seq("PassengerId")
      )
      .withColumn(
        "Gender",
        when(col("Sex").equalTo("male"), 0)
          .when(col("Sex").equalTo("female"), 1)
          .otherwise(null)
      )
      .select(
        col("Gender").cast("double"),
        col("Survived").cast("double"),
        col("Pclass").cast("double"),
        col("Age").cast("double"),
        col("SibSp").cast("double"),
        col("Parch").cast("double"),
        col("Fare").cast("double")
      )
      .filter(
        col("Gender").isNotNull &&
          col("Pclass").isNotNull &&
          col("Age").isNotNull &&
          col("SibSp").isNotNull &&
          col("Parch").isNotNull &&
          col("Fare").isNotNull
      )

  }

}

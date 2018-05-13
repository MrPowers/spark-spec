package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.scalatest.FunSpec
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TitanicLogisticRegressionSpec
  extends FunSpec
  with SparkSessionTestWrapper {

  describe("vectorizeFeatures") {

    it("converts all the features to a vector without blowing up") {

      val df = spark.createDF(
        List(
          (1.0, 12.0, 3.0, 4.0, 10.0)
        ), List(
          ("Gender", DoubleType, true),
          ("Age", DoubleType, true),
          ("SibSp", DoubleType, true),
          ("Parch", DoubleType, true),
          ("Fare", DoubleType, true)
        )
      ).transform(TitanicLogisticRegression.vectorizeFeatures())

    }

  }

  it("trains a logistic regression model that's more than 80 percent accurate") {

    val testFeatures: DataFrame = TitanicData
      .testDF()
      .transform(
        TitanicLogisticRegression.vectorizeFeatures()
      )

    val predictions: DataFrame = TitanicLogisticRegression
      .model()
      .transform(testFeatures)
      .select(
        col("Survived").as("label"),
        col("rawPrediction"),
        col("prediction")
      )

    val res = new BinaryClassificationEvaluator()
      .evaluate(predictions)

    assert(res >= 0.80)
  }

}

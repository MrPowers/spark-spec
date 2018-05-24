package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class TitanicLogisticRegressionSpec
  extends FunSpec
  with SparkSessionTestWrapper
  with ColumnComparer {

  describe("withVectorizedFeatures") {

    it("converts all the features to a vector without blowing up") {

      val df = spark.createDF(
        List(
          (1.0, 12.0, 3.0, 4.0, 10.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 12.0, 3.0, 4.0, 10.0))
        ), List(
          ("Gender", DoubleType, true),
          ("Age", DoubleType, true),
          ("SibSp", DoubleType, true),
          ("Parch", DoubleType, true),
          ("Fare", DoubleType, true),
          ("expected", new org.apache.spark.mllib.linalg.VectorUDT, true)
        )
      ).transform(TitanicLogisticRegression.withVectorizedFeatures())

      // assertColumnEquality doesn't work for vector columns
      //      assertColumnEquality(df, "expected", "features")

    }

  }

  describe("withLabel") {

    it("adds a label column") {

      val df = spark.createDF(
        List(
          (0.0, 1.0),
          (1.0, 0.0)
        ), List(
          ("Survived", DoubleType, true),
          ("expected", DoubleType, true)
        )
      )
        .transform(TitanicLogisticRegression.withLabel())

      assertColumnEquality(df, "expected", "label")

    }

    it("works if the label column is a string") {

      val df = spark.createDF(
        List(
          ("no", 0.0),
          ("yes", 1.0),
          ("hi", 2.0),
          ("no", 0.0)
        ), List(
          ("Survived", StringType, true),
          ("expected", DoubleType, true)
        )
      )
        .transform(TitanicLogisticRegression.withLabel())

      assertColumnEquality(df, "expected", "label")

    }

  }

  describe("model") {

    it("returns the coefficients") {

      println(TitanicLogisticRegression.model().coefficients)
      println(TitanicLogisticRegression.model().intercept)
      println(TitanicLogisticRegression.model().summary.accuracy)

    }

    it("trains a logistic regression model that's more than 80 percent accurate") {

      val testDF: DataFrame = TitanicData
        .testDF()
        .transform(TitanicLogisticRegression.withVectorizedFeatures())
        .transform(TitanicLogisticRegression.withLabel())
        .select("features", "label")

      val predictions: DataFrame = TitanicLogisticRegression
        .model()
        .transform(testDF)
        .select(
          col("label"),
          col("rawPrediction"),
          col("prediction")
        )

      // we can also use a model that's been persisted
      //      val predictions: DataFrame = LogisticRegressionModel
      //        .load("./tmp/titanic_model/")
      //        .transform(testDF)
      //        .select(
      //          col("label"),
      //          col("rawPrediction"),
      //          col("prediction")
      //        )

      val res = new BinaryClassificationEvaluator()
        .evaluate(predictions)

      assert(res >= 0.80)

    }

  }

}

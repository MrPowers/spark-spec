package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.scalatest.FunSpec
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import com.github.mrpowers.spark.spec.sql.TitanicLogisticRegression._
import org.apache.spark.sql.functions._

class TitanicLogisticRegressionSpec
  extends FunSpec
  with SparkSessionTestWrapper {

  it("can train a Logistic Regression Model & test using Kaggle Titanic data") {

    assert(new BinaryClassificationEvaluator()
      .evaluate(predictions
        .select(
          col("Survived").as("label"),
          col("rawPrediction"),
          col("prediction")
        )) >= 0.80)
  }

}

package com.github.mrpowers.spark.spec.ml.classification

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

class LinearRegressionWithSGDSpec extends FunSpec with DataFrameSuiteBase {

  it("can train a model and run a linear regression with SGD") {

    //From http://spark.apache.org/docs/latest/mllib-linear-methods.html

    val path = new java.io.File("./src/test/resources/lpsa.data").getCanonicalPath

    val training = sc.textFile(path)

    val parsedData = training.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // Building the model
    val numIterations = 10
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()

    assert(MSE === 7.4510328101026015)

  }

}
package com.github.mrpowers.spark.spec.ml.classification

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.scalatest.FunSpec
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, SparseVector}

class LogisticRegressionSpec extends FunSpec with SparkSessionTestWrapper {

  it("can train a model and run a logistic regression") {

    val path = new java.io.File("./src/test/resources/sample_libsvm_data.txt").getCanonicalPath
    val training = spark.read.format("libsvm").load(path)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    assert(lrModel.intercept === 0.22456315961250325)
    val c = new SparseVector(
      692,
      Array(244, 263, 272, 300, 301, 328, 350, 351, 378, 379, 405, 406, 407, 428, 433, 434, 455, 456, 461, 462, 483, 484, 489, 490, 496, 511, 512, 517, 539, 540, 568),
      Array(-7.353983524188197E-5, -9.102738505589466E-5, -1.9467430546904298E-4, -2.0300642473486668E-4, -3.1476183314863995E-5, -6.842977602660743E-5, 1.5883626898239883E-5, 1.4023497091372047E-5, 3.5432047524968605E-4, 1.1443272898171087E-4, 1.0016712383666666E-4, 6.014109303795481E-4, 2.840248179122762E-4, -1.1541084736508837E-4, 3.85996886312906E-4, 6.35019557424107E-4, -1.1506412384575676E-4, -1.5271865864986808E-4, 2.804933808994214E-4, 6.070117471191634E-4, -2.008459663247437E-4, -1.421075579290126E-4, 2.739010341160883E-4, 2.7730456244968115E-4, -9.838027027269332E-5, -3.808522443517704E-4, -2.5315198008555033E-4, 2.7747714770754307E-4, -2.443619763919199E-4, -0.0015394744687597765, -2.3073328411331293E-4)
    )
    assert(lrModel.coefficients === c)

  }

  it("can train a multinomial family for binary classification") {

    val path = new java.io.File("./src/test/resources/sample_libsvm_data.txt").getCanonicalPath
    val training = spark.read.format("libsvm").load(path)

    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    assert(mlrModel.interceptVector === new DenseVector(Array(-0.12065879445860686, 0.12065879445860686)))
    //    assert(mlrModel.coefficientMatrix === new DenseMatrix(2, 692, Array(2.0)))

  }

}

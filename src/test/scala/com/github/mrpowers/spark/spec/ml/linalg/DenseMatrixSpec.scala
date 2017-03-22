package com.github.mrpowers.spark.spec.ml.linalg

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}
import org.scalatest.FunSpec

class DenseMatrixSpec extends FunSpec {

  describe("new") {

    it("creates a DenseMatrix") {

      // new DenseMatrix(numRows: Int, numCols: Int, values: Array[Double])

      val m = new DenseMatrix(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
      assert(m.getClass().getName() === "org.apache.spark.ml.linalg.DenseMatrix")

    }

  }

  describe("#apply") {

    it("gets the (i,j)th element of a matrix") {

      val m = new DenseMatrix(4, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      val actual = m.apply(2, 2)
      val expected = 11.0
      assert(actual === expected)

    }

  }

  describe("#copy") {

    it("copies the matrix") {

      val m = new DenseMatrix(4, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      val actual = m.copy
      val expected = new DenseMatrix(4, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      assert(actual === expected)

    }

  }

  describe("#multiply") {

    it("multiplies two dense matrices") {

      val m = new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0))
      val n = new DenseMatrix(2, 1, Array(1.0, 10.0))

      val actual = m.multiply(n)
      val expected = new DenseMatrix(2, 1, Array(31.0, 42.0))
      assert(actual === expected)

    }

    it("multiplies a matrix with a vector") {

      val a = new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0))
      val b = new DenseVector(Array(1.0, 100.0))

      val actual = a.multiply(b)
      val expected = new DenseVector(Array(301.0, 402.0))
      assert(actual === expected)

    }

  }

  describe("#numActives") {

    it("gets the number of values stored explicitly") {

      val m = new DenseMatrix(4, 3, Array(0.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      assert(m.numActives === 12)

    }

  }

  describe("#numCols") {

    it("gets the number of columns") {

      val m = new DenseMatrix(4, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      assert(m.numCols === 3)

    }

  }

  describe("#numRows") {

    it("gets the number of rows") {

      val m = new DenseMatrix(4, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0))
      assert(m.numRows === 4)

    }

  }

  describe("#transpose") {

    it("transposes the matrix") {

      val m = new DenseMatrix(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
      val actual = m.transpose
      val expected = new DenseMatrix(2, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
      assert(actual === expected)

    }

  }

}

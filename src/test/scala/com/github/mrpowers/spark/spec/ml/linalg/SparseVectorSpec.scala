package com.github.mrpowers.spark.spec.ml.linalg

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.ml.linalg.SparseVector

class SparseVectorSpec extends FunSpec with DataFrameSuiteBase {

  describe("new") {

    it("creates a SparseVector") {

      // new SparseVector(size: Int, indices: Array[Int], values: Array[Double])
      // size: size of the vector.
      // indices: index array, assume to be strictly increasing.
      // values: value array, must have the same length as the index array.

      val s = new SparseVector(50, Array(3, 10, 40), Array(8, -14, 32))
      assert(s.getClass().getName() === "org.apache.spark.ml.linalg.SparseVector")

    }

  }

  describe("#argmax") {

    it("returns the size of the vector") {

      val s = new SparseVector(50, Array(3, 10, 40), Array(8, -14, 32))
      assert(s.argmax === 40)

    }

  }
  describe("#size") {

    it("returns the size of the vector") {

      val s = new SparseVector(50, Array(3, 10, 40), Array(8, -14, 32))
      assert(s.size === 50)

    }

  }

}

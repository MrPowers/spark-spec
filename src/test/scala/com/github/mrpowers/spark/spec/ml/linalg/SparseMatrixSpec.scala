package com.github.mrpowers.spark.spec.ml.linalg

import org.apache.spark.ml.linalg.{SparseMatrix}
import org.scalatest.FunSpec


class SparseMatrixSpec extends FunSpec  {

  describe("new") {

    it("creates a SparseMatrix") {

    val s = new SparseMatrix(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    assert(s.getClass().getName() === "org.apache.spark.ml.linalg.SparseMatrix")

    }

  }

  describe("creates a dense matrix from a given sparse matrix") {

    it ("creates a Dense Matrix from a Sparse Matrix") {

    val s = new SparseMatrix(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val d = s.toDense
    assert(d.getClass().getName() ==="org.apache.spark.ml.linalg.DenseMatrix")
     }

  }

}

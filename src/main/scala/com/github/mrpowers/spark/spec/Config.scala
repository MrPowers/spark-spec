package com.github.mrpowers.spark.spec

object Config {

  var test: Map[String, String] = {
    Map(
      "libsvmData" -> new java.io.File("./src/test/resources/sample_libsvm_data.txt").getCanonicalPath,
      "irisData" -> new java.io.File("./src/test/resources/iris/iris.csv").getCanonicalPath,
      "somethingElse" -> "hi"
    )
  }

  var production: Map[String, String] = {
    Map(
      "libsvmData" -> "s3a://my-cool-bucket/fun-data/libsvm.txt",
      "somethingElse" -> "whatever"
    )
  }

  var environment = sys.env.getOrElse("PROJECT_ENV", "production")

  def get(key: String): String = {
    if (environment == "test") {
      test(key)
    } else {
      production(key)
    }
  }

}

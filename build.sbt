name := "spark-spec"

version := "0.0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"

libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "1.0.0"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1" % "test"

fork in Test := true
envVars in Test := Map("PROJECT_ENV" -> "test")
javaOptions ++= Seq("-Xms1g", "-Xmx2g", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

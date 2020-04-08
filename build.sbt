
resolvers += "jitpack" at "https://jitpack.io"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "spark-spec"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4" % "provided"

libraryDependencies += "mrpowers" % "spark-daria" % "0.37.1-s_2.11" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
envVars in Test := Map("PROJECT_ENV" -> "test")
javaOptions ++= Seq("-Xms1g", "-Xmx2g", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")
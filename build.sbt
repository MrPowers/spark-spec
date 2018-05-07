import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(SpacesAroundMultiImports, false)
  .setPreference(DanglingCloseParenthesis, Force)

resolvers += "jitpack" at "https://jitpack.io"

name := "spark-spec"

version := "0.0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"
val sparkFastTestsVersion = s"v${sparkVersion}_0.9.0"
val sparkDariaVersion = s"v${sparkVersion}_0.20.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

libraryDependencies += "com.github.mrpowers" % "spark-daria" % sparkDariaVersion % "test"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % sparkFastTestsVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
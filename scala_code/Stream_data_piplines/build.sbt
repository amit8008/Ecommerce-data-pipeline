ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "Stream_data_pipelines",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.scala-lang" % "scala-reflect" % "2.13.14"
    ),
    assembly / assemblyJarName := "spark-streamingdata-pipeline-1.0.jar",
  )

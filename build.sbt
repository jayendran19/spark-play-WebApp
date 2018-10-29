name := "productanalysis"
 
version := "1.0" 
      
lazy val `productanalysis` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
      
scalaVersion := "2.11.11"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.7",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
)
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.0.0"


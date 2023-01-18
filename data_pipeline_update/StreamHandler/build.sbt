name := "Stream Handler"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.2.2" % "provided",
	"org.apache.spark" %% "spark-sql" % "3.2.2" % "provided",
	"com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0",
	"com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0"
)
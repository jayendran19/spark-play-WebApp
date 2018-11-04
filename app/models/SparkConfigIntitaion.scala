package models
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


class SparkConfigIntitaion {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkAndHive")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://localhost:9999/Default")
    .enableHiveSupport()
    .getOrCreate()

  val spark= SparkSession.builder()
    .master("local[*]")
    .appName("ProductAnalysis")
    .config(conf = new SparkConf())
    .getOrCreate()

  def toStartSparkHive(tname: String): DataFrame = {

    val tablename: DataFrame = sparkSession.sql("select * from analysis." +tname)
    tablename
  }
  def toCustomHive(tname: String): DataFrame = {

    val tablename: DataFrame = sparkSession.sql("select Sales_location_id,Byno,Bill_No,Item_Quantity as quantity,Item_Pieces as Pieces,Sales_Value as sales_amount,Customer_Id,Bill_Date,Product_Code,Section_Id from analysis." +tname+" limit 10")
    tablename
  }




   def toStartSparkSql(tname: String) = {

    val sqltable = spark.read.format("jdbc")
      .option("url", "jdbc:sqlserver://localhost;database=Analysis;user="";password="")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable",tname)
      .option("header",true)
      .option("inferSchema",true)
      .load()

     sqltable
    }






}

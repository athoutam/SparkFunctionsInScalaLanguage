package com.bigdata.spark.Git

import breeze.numerics.abs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

object SparkFunctionsInScala {

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkFunctionsInScala").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SparkFunctionsInScala").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val input = "C:\\work\\datasets\\JSON\\stocks.json"
    val df = spark.read.format("json").load(input)
    //Header columns in the data has special symbols. To remove all special symbols, spaces on column names using below regex.
    val reg = "[^\\p{L}\\p{Nd}]+"
    val col = df.columns.map(x=>x.replaceAll(reg,""))
    //toDF() used to convert rdd to dataframe, and to rename columns
    val newdf = df.toDF(col:_*)
    newdf.show(5)
    newdf.printSchema()
    //withColumnRenamed() is used to rename only one column
    val newdf1 = newdf.withColumnRenamed("id","UniqueId")
    newdf1.show(2)
    newdf1.printSchema()
    newdf.createOrReplaceTempView("stocks")
    //nvl(column,0) is used to replace null values with 0
    val res = spark.sql("select 52WeekLow, AnalystRecom, 52WeekLow+nvl(AnalystRecom,0) SUMTWOCOLUMNS from stocks")
    res.show(5, false)
    //withColumn() is used to update the values
    val upd = newdf.withColumn("AnalystRecom", when(($"AnalystRecom").isNull,0).otherwise($"AnalystRecom")).withColumn("20DaySimpleMovingAverage", abs($"20DaySimpleMovingAverage"))
    upd.show(10,false)
    upd.printSchema()
    //UserDefinedFunction
    def offer(col:String)=col match {
      case "USA"=>"10% OFF"
      case "Canada"=> "20% OFF"
      case "Switzerland"=> "30% OFF"
      case _ => "NO OFFER"
    }
    val offudf = spark.udf.register("OfferFunction", offer _)
    val res1 = spark.sql("Select Company,Country,offer(Country) todayoffer from stocks")
    res1.show(5)
    res1.printSchema()
    spark.stop()
  }
}

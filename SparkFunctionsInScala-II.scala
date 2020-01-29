package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SparkScalaFunctions {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DataframeOperationInScala").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DataframeOperationInScala").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val input = "C:\\work\\datasets\\JSON\\zips.json"
    val zipdf = spark.read.format("json").load(input)
    zipdf.createOrReplaceTempView("zipdata")


    val zipdf1 = spark.sql("select  _id id, city, abs(loc[0]) lang, loc[1] lati,  pop, conv(pop,10,8) octalnum, conv(pop,10,16) hexagonalnum, bin(pop) binpop, cbrt(pop) cuberootofpop, state, 2 lit from zipdata") // no no negative values iin lang
    zipdf1.show(5, false)
    zipdf1.printSchema()
    val zipdf2 = zipdf1.withColumn("ceilvalue",ceil($"lati")).withColumn("floornum",floor($"lang")).withColumn("rinttest",rint($"lang")).
      withColumn("expolati",exp($"lati")).withColumn("floornum",expm1($"lati")).withColumn("monotid",monotonically_increasing_id()+1).
      withColumn("factorialofid",factorial($"monotid")).withColumn("hypottest",hypot($"lang",$"lati")).withColumn("leastvalue",least($"lang",$"lati")).
      withColumn("langpowlit",pow($"lang", $"lit")).withColumn("hypottest",pmod($"pop", $"lit")).
      withColumn("langsignum",signum($"lang")).withColumn("latisignum",signum($"lati")).withColumn("binpopmd5password",md5($"binpop")).
      withColumn("binpopsha1password",sha1($"binpop")).withColumn("binpopsha2password",sha2($"binpop",512)).
      withColumn("binpophashpassword",hash($"binpop")).withColumn("binpopbase64password",base64($"binpop")).withColumn("cityascii",ascii($"city"))
    zipdf2.show(5, false)
    zipdf2.printSchema()
    //val zipdf3 = zipdf2.groupBy($"state").count().select(last($"count").alias("last"),first($"count").alias("first")).show()
    spark.stop()
  }
}


/*  abs: abs(Column e)
        Computes the absolute value of a numeric value. means if data /value contains negative values convert to integer.
    lit: lit("defaultvalue")
        Creates a Column of literal value. Here, column name is 'age' and value is 18.
    bin:
        Its convert decimal number to binary value
    cbrt:
        Computes the cube-root of the given value.
    ceil:
        Computes the ceiling of the given value. It means it maps to the least integer greater than or equal to , denoted. Let eg 3.01 pointing to 4 or 2.99 pointing to 3;
    floor :
        floor reverse to ceil it ignore decimal values and poingint go floor value let eg: 3.03 point to 3, 4.9 pointing to 4, but if u mention =3.4 its pointing to 4
    rint or round or bround :
        bround Returns the value of the column e rounded to 0 decimal places with HALF_EVEN round mode.
        8888.4 its consider as 8888
        7777.7 its consider as 7778
    conv, hex, unhex :
        Convert a number in a string column from one base to another.
        means convert into to octal number hexagonal numbers and more
        syntax: conv(Column num, int fromBase, int toBase)
    exp :
        Computes the exponential of the given column.
        most frequently used in stastics algorithms more info about this function just follow this link https://keisan.casio.com/exec/system/1223447896
    expm1 :
        same like exp function, but substract by 1 means exp-1 thats it
        Computes the exponential of the given value minus one.
        monotonically_increasing_id() :
        lets example i want to add 1 2, 3, 4, 5, .. like this as a column values at that time use this please note it starting from 0 so add +
    first() && last() && floor() :
        The function by default returns the first values it sees. It will return the first non-null value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
        Computes the floor of the given column.
        let eg: yesterday last record end with 12-12-1999 now get last value means use floor
    hypot :
        hypot(Column l,Column r)
        Computes sqrt(a^2^ + b^2^) without intermediate overflow or underflow.
        let eg: -72.622739|42.07020 you have now -72.622739*-72.622739+42.0702042*42.0702042 = 5274.06221986+1769.90208143=7043.96430129 sqrt(7043.96430129)=83.9283283599
    least :
        compare two columns find least value in two row
        let eg : col1,col2 = 4, 5
        now least value 4
    pow :
        one column power another column use pow
    pmod :
        Returns the positive value of dividend mod divisor.
        now 15338/3 reminder 2 so you will get result 2
    signum :
        you will get either 1, 0 or -1 thats it
        its math function simply f(x) = |x|/x
        let eg -78.0234 return -1
        else +1
        if x = 0 at that time 0
    md5 & sha1 & sha2 & hash && base64 :
        used to encrypt a value like password , input must binary value 
        sha2 ..numBits - one of 224, 256, 384, or 512.
        base64 returns it as a string column
    ascii :
        take first character in each column and return ascii character
        Agra here first character A so it return 65
*/
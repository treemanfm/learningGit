

package com.hsbc.basic

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode._
import com.hsbc.basic._
import org.apache.spark.sql.hive.HiveContext



object datacleaner {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("datacleaner").setMaster("spark://Centos01:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val links = sc.textFile("hdfs://Centos01:9000/data/links.txt", 16).map( _.split(",")).map(x => Links(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toInt)).toDF()
    val movies = sc.textFile("hdfs://Centos01:9000/data/movies.txt",16).map(_.split(",")).map(x => Movies(x(0).trim.toInt,x(1).trim,x(2).trim)).toDF()
    val ratings = sc.textFile("hdfs://Centos01:9000/data/ratings.txt",16).map(_.split(",")).map(x => Ratings(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toFloat,x(3).trim.toInt)).toDF()
    val tags = sc.textFile("hdfs://Centos01:9000/data/tags.txt",16).map(_.split(",")).map(x => Tags(x(0).trim.toInt,x(1).trim.toInt,x(2).trim,x(3).trim.toInt)).toDF()
    
    
    links.write.parquet("hdfs://Centos01:9000/tmp/links")
    movies.write.parquet("hdfs://Centos01:9000/tmp/movies")
    ratings.write.parquet("hdfs://Centos01:9000/tmp/ratings")
    tags.write.parquet("hdfs://Centos01:9000/tmp/tags")

    val hc = new HiveContext(sc)
    hc.sql("drop table if exists links")
    hc.sql("create table if not exists links(movieId int, imdbId int, tmdbId int) stored as parquet");
    hc.sql("load data inpath'hdfs://master:9000/tmp/links' overwrite into table links")


  }
  
 
}

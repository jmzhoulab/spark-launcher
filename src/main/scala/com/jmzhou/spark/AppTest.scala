package com.jmzhou.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import mu.atlas.graph.utils.Props

import scala.reflect.io.File
import scala.collection.JavaConverters._

/**
  * Created by zhoujiamu on 2019/9/5.
  */
object AppTest {

  def getSparkSession(prop: Props): SparkSession ={

    val sparkConf = new SparkConf()

    val config = new Configuration()
    config.addResource(File(prop.getProperty("core.site")).inputStream())
    config.addResource(File(prop.getProperty("hdfs.site")).inputStream())
    config.addResource(File(prop.getProperty("hive.site")).inputStream())
    config.addResource(File(prop.getProperty("yarn.site")).inputStream())

    for (c <- config.iterator().asScala){
      println(c.getKey + ": " + c.getValue)
      sparkConf.set(c.getKey, c.getValue)
    }

    prop.keys().filter(_.startsWith("spark"))
      .foreach(key => sparkConf.set(key, prop.getProperty(key)))

    val spark = SparkSession.builder().config(sparkConf)
      .enableHiveSupport().getOrCreate()

    spark

  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.setAppName("AppTest")
    sparkConf.setMaster("local")

    val spark = SparkSession.builder()
      .config(sparkConf)
//      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.makeRDD(1 to 20).count



  }



}

package com.apache.spark.AdUsageStatistics

/**
 * @author KRISHNAN
 * @Version 1.0
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.log4j.{Level, Logger}
import scala.util.{Try, Success, Failure}
import java.io.FileNotFoundException
import java.io.IOException


object AdUsageStatAppDF {
  
  def main(args: Array[String]) {
    
    //check the number of input args:
    if (args.length < 3) {
      System.err.println("Usage: spark2-submit AdUsageStatAppDF <input_path> <output_path> <master>")
      System.exit(1)
    }
    
    val log = Logger.getLogger(getClass.getName)
    log.setLevel(Level.ERROR)
    
    // Initialize the spark session and set app name before starting the spark job:
    val spark = SparkSession
               .builder()
               .appName("AdUsageStatAppDF")
               .master(args(2))
               .getOrCreate()
    //assigning input path to variables:
    val inputpath = args(0)
    val outputpath = args(1)
        
    // starting the data processing steps:
    log.info("starting the AdUsageStatApp data processing")
    
    // creating schema for input data:
    val inputSchema = StructType(Array(StructField("campaign_id", DataTypes.IntegerType, nullable=true),
                                   StructField("guid", DataTypes.StringType, nullable=true),
                                   StructField("timestamp", DataTypes.StringType, nullable=true),
                                   StructField("ad_id", DataTypes.StringType, nullable=true),
                                   StructField("site_id", DataTypes.StringType, nullable=true),
                                   StructField("site_url", DataTypes.StringType, nullable=true),
                                   StructField("ad_type", DataTypes.StringType, nullable=true),
                                   StructField("tag_type", DataTypes.StringType, nullable=true),
                                   StructField("placement_id", DataTypes.StringType, nullable=true),
                                   StructField("wild_card", DataTypes.StringType, nullable=true),
                                   StructField("custom_1", DataTypes.StringType, nullable=true),
                                   StructField("custom_2", DataTypes.StringType, nullable=true),
                                   StructField("custom_3", DataTypes.StringType, nullable=true),
                                   StructField("custom_4", DataTypes.StringType, nullable=true),
                                   StructField("custom_5", DataTypes.StringType, nullable=true),
                                   StructField("custom_6", DataTypes.StringType, nullable=true),
                                   StructField("custom_7", DataTypes.StringType, nullable=true),
                                   StructField("custom_8", DataTypes.StringType, nullable=true),
                                   StructField("custom_9", DataTypes.StringType, nullable=true),
                                   StructField("custom_10", DataTypes.StringType, nullable=true),
                                   StructField("pass", DataTypes.StringType, nullable=true),
                                   StructField("opt_out", DataTypes.StringType, nullable=true),
                                   StructField("iframe", DataTypes.StringType, nullable=true),
                                   StructField("weight", DataTypes.StringType, nullable=true),
                                   StructField("size", DataTypes.StringType, nullable=true),
                                   StructField("tactic", DataTypes.StringType, nullable=true),
                                   StructField("visible", DataTypes.StringType, nullable=true),
                                   StructField("exposure_time", DataTypes.StringType, nullable=true),
                                   StructField("pre_ad_exposure", DataTypes.StringType, nullable=true),
                                   StructField("post_ad_exposure", DataTypes.StringType, nullable=true),
                                   StructField("campaign_imp_count", DataTypes.StringType, nullable=true),
                                   StructField("referrer", DataTypes.StringType, nullable=true),
                                   StructField("user_agent", DataTypes.StringType, nullable=true),
                                   StructField("city", DataTypes.StringType, nullable=true),
                                   StructField("state", DataTypes.StringType, nullable=true),
                                   StructField("country", DataTypes.StringType, nullable=true),
                                   StructField("zip_code", DataTypes.StringType, nullable=true),
                                   StructField("language", DataTypes.StringType, nullable=true),
                                   StructField("ip_address", DataTypes.StringType, nullable=true),
                                   StructField("guidsource", DataTypes.StringType, nullable=true),
                                   StructField("ad_server_cd", DataTypes.StringType, nullable=true)
                                  )
                           )   
                           
    log.info("reading the input file")
    
    try {
          val inputDF = spark.read.option("header","false")                //reading the input files into dataframe
                        .option("delimiter","\t")
                        .option("comment","#")
                        .schema(inputSchema)
                        .csv(inputpath)
          val filterDF = inputDF.select("ad_id","site_id","site_url","guid")
                                .filter(col("guid").rlike("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"))  //using rlike to filter all non-standard guids
                                .toDF("ad_id","site_id","site_url","guid")  //converting the dataset to dataframe.
                                
          log.info("getting the frequency information from input data")
          
          //finding the frequency of site viewed more the 5x by using count on site_url by grouping ad_id, site_id, guid:
          val FreqDF = filterDF.groupBy("ad_id", "site_id", "guid").count().withColumnRenamed("count", "frequency").filter("frequency > 5").toDF("ad_id","site_id","guid","frequency")
          
          //finding the count of users who viewed the ad at same frequency and sorting the output in descending frequency
          val userDF = FreqDF.groupBy("ad_id", "site_id", "frequency").count().withColumnRenamed("count", "total_views").orderBy(col("frequency").desc).toDF("ad_id","site_id","frequency","total_views")
          
          log.info("writing output data to a file")
          
          //writing the output to a tab delimited csv file.
          val writeOutput = userDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").option("header", "true").csv(outputpath)
                   
          log.info("completed loading the output to file")
                    
          log.info("exiting the spark job")
          
          System.exit(0)
          
    } catch {
      case ex: FileNotFoundException => {
        log.error(s"path does not exist : $ex")
        System.exit(1)
      }
      
      case ex1: IOException => {
        log.error(s"Failed due to : $ex1")
        System.exit(1)
      }
      
      case unknown: Exception => {
        log.error(s"Unknown exception: $unknown")
        System.exit(1)
      }
    }
  }
}

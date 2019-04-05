package com.apache.spark.AdUsageStatistics

/**
 * @author KRISHNAN
 * @Version 1.0
 * @Description This app gets the frequency of ads viewed by users from the input log files.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.log4j.{Level, Logger}
import scala.util.{Try, Success, Failure}
import java.io.FileNotFoundException


object AdUsageStatApp {
  
  def main(args: Array[String]) {
    
    //check the number of input args:
    if (args.length < 2) {
      System.err.println("Usage: spark2-submit AdUsageStatApp <input_file> <output_file> <master>")
      System.exit(1)
    }
    
    val log = Logger.getLogger(getClass.getName)
    log.setLevel(Level.ERROR)
    
    // Initialize the spark session and set app name before starting the spark job:
    val spark = SparkSession
               .builder()
               .appName("AdUsageStatApp")
               .master(args(1))
               .getOrCreate()
    //assigning input path to variables:
    val inputpath = args(0)
    
    
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
          val inputDF = spark.read.option("header","false")
                        .option("delimiter","\t")
                        .option("comment","#")
                        .schema(inputSchema)
                        .csv(inputpath)
          val filterDF = inputDF.select("ad_id","site_id","site_url","guid")
                                .filter(col("guid") !== "unsupported")      //using filter to eliminate unsupported guids
                                .filter(col("guid") !== "-")                //using filter to eliminate '-' guids
                                .filter(col("guid").rlike("^[a-z0-9-]*$"))  //using rlike to filter special character guids
                                .filter(col("guid") !== "")                 //using filter to remove null guids
                                
          log.info("getting the frequency information from input data")
          
          filterDF.createOrReplaceTempView("ad_usage_data")           //creating view for querying data
          val outputDF = spark.sql("""select ad_id,
                                             site_id,
                                             frequency,
                                             count(*) as total_users
                                    from (
                                    select ad_id, site_id, guid, count(*) as frequency 
                                    from ad_usage_data
                                    group by ad_id, site_id, guid
                                    ) where frequency > 5
                                    group by ad_id, site_id, frequency
                                    order by frequency desc""").toDF("ad_id","site_id","frequency","total_users")
          
          log.info("writing output data to table")
          
          val writeOutput = outputDF.coalesce(2).write.mode("Append").option("delimiter", "\t").saveAsTable("psol_sandbox.ad_usage_output")
          spark.catalog.refreshTable("psol_sandbox.ad_usage_output")
          
          log.info("completed loading the output to table")
          
          
          //clearing all cache before exiting:
          outputDF.unpersist()
          filterDF.unpersist()
          inputDF.unpersist()
          
          log.info("exiting the spark job")
          
          System.exit(0)
          
    } catch {
      case ex: FileNotFoundException => {
        log.error(s"input file $inputpath does not exist : $ex")
        System.exit(1)
      } 
    
      case unknown: Exception => {
        log.error(s"Unknown exception: $unknown")
        System.exit(1)
      }
    }
  }
}

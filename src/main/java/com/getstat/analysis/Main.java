package com.getstat.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;

public class Main {

	public static void main(String[] args) throws Exception {
		
		if (args.length != 1) {
			System.out.println("Parameter with file is missing! [hdfs - CSV file]");
			return;
		}
		
		
		/* ETL Flow:
		 	1 - Extract the CSV information
		 	2 - Generate Dataset's with the analytic information
		 	3 - Load the CSV file in Hive tables and the Analytics informations in other Hive Tables
		*/
		String hdfsFile = args[0];
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkHive").setMaster("local[2]");  
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext  spark = new org.apache.spark.sql.SQLContext(sc);
        HiveContext hive = new org.apache.spark.sql.hive.HiveContext(sc);
        
		StructType customSchema = new StructType(new StructField[] {
		    new StructField("Keyword", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Market", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Location", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Device", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("CrawlDate", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Rank", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("URL", DataTypes.StringType, true, Metadata.empty())
		});
        
		DataFrame df = hive.read()
				        	   .format("com.databricks.spark.csv")
				        	   .schema(customSchema)
				        	   .option("header", "true")
				        	   .load(hdfsFile);
    	
    	//df.printSchema();
    	df.registerTempTable("stat_csv_temp");
    	
    	//Save all in Hive Database...
    	hive.sql("insert into stat.stat_csv Select * from stat_csv_temp");
    	
    	//SQL commands to extract all information that SEO needs....
    	// Question 1 - Which URL has the most ranks in the top 10 across all keywords over the period?
    	
    	
    	hive.sql("insert into stat.stat_question_one Select t.* from ( select URL, count(0) as total from stat_csv_temp " +
			    "where Rank <= 10 "+
			    "group by URL ) t order by t.total desc");
    	
    	
    	/* Question 2 -  Provide the set of keywords (keyword information) where the rank 1 URL changes 
    	 				 the most over the period. A change, for the purpose of this question, is when 
    	 				 a given keyword's rank 1 URL is different from the previous day's URL.*/
    	
    	
    	
		 hive.sql("insert into stat.stat_question_two Select t.URL, collect_list(struct(t.CrawlDate, t.Keyword)) as list_items from ( " +
		   "	Select URL, CrawlDate, Keyword, count(0) as total from stat_csv_temp "+ 
		   "	where Rank = 1 " +
		   "	group by URL, CrawlDate, Keyword "+
		   "	order by URL, CrawlDate "+
		   ") t group by t.URL");
		 
    	
    	/* Question 3 -  We would like to understand how similar the results returned for the same keyword, 
    	  				 market, and location are across devices. For the set of keywords, markets, and 
    	  				 locations that have data for both desktop and smartphone devices, please devise a 
    	  				 measure of difference to indicate how similar these datasets are, and please use 
    	  				 this to show how the mobile and desktop results in our provided data set converge 
    	  				 or diverge over time.*/
    	
    	hive.sql(
    			"insert into stat.stat_question_three Select * from (" +
    			"	Select Keyword, Market, Location, Device, CrawlDate, count(0) as total from stat_csv_temp " +
    			"	where Device in ('smartphone', 'desktop') " +
    			"	group by Keyword, Market, Location, Device, CrawlDate " +
    			") t order by t.CrawlDate");
    	
    	
	}
	
}

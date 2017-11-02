STAT - STAT   Search   Analytics

# Task Description
Our goal with the project is two fold:

1. Design and build an ETL process to ingest and store this data in an efficient format for analysis
2. Analyze the data to answer three important SEO questions
In doing so, we want to give you a feel for the kind of work you will be doing in the Data Services team at STAT, and we want to get an idea of how you would solve these problems.

Although we have provided a very tiny subset of data to you to limit the size and scope of the project, we would like you to design the solution considering the scale of data that we store and analyze in production today.

The SEO questions that we would like you to answer are:

1. Which URL has the most ranks in the top 10 across all keywords over the period?

2. Provide the set of keywords (keyword information) where the rank 1 URL changes the most over
the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is
different from the previous day's URL.

3. We would like to understand how similar the results returned for the same keyword, market, and
location are across devices. For the set of keywords, markets, and locations that have data for both desktop and smartphone devices, please devise a measure of difference to indicate how similar these datasets are, and please use this to show how the mobile and desktop results in our provided data set converge or diverge over time.

## 1.Design and build an ETL process
 
#### 1.1 Design

The ideia is create a project to execute the ETL process below:

* 1 - The file CSV have to exist in HDFS (Hadoop Distributed File System) 
* 1.1 - Why? Because read file in HDFS have more performance than read a File in Common File System and HDFS have Fail tolerance.

* 2 - Create a Apache Spark Rotine in Java (Job) managed by YARN (Yet Another Resource Negotiator) that read CSV file and save all the result of process in Hive Tables (Hive Database)
* 2.1 - Read a CSV file using StructType, and create a temp table to use Apache Spark SQL to execute analysis in this Dataset all in memory
* 2.2 - Execute the analysis using Apache Spark SQL
* 2.2.1 - Execute the process to get the informations about the 1 question (strutured data)
* 2.2.2 - Execute the process to get the informations about the 2 question (semi strutured data - JSON)
* 2.2.3 - Execute the process to get the informations about the 3 question (strutured data) 
* 2.3 - Save all the CSV data in a strutured data (table) and save the other informations extracted in other tables to provide a easy analysis and all in Hive Database.
 
##### 1.1.1 Technologies Used:
* 1 - CentOS 6.5 + Cloudera Hadoop 5.12 (install Hadoop, YARN, MapReduce, Apache Spark, Apache Hive and Apache Hue)
* 1.1 - Recomendations (for production): 1 Server for NameNode/Master (16Gb RAM) and 4 Servers for DataNode/Slave (4Gb RAM) 
* 1.2 - Set the parameters: yarn.scheduler.maximum-allocation-mb and yarn.nodemanager.resource.memory-mb = 2Gb
* 2 - HDFS
* 3 - Apache Spark 1.6.0 + Java
* 4 - Maven
* 5 - Apache Hive 1.1

##### 1.1.2 Install Cloudera:

__Use this commands:__


wget http://archive.cloudera.com/cm5/installer/latest/cloudera-manager-installer.bin

chmod u+x cloudera-manager-installer.bin

./cloudera-manager-installer.bin


__After follow the instructions and install in "Single mode" in http://localhost:7180/cmf/login__


##### 1.1.2 Create a database in Hive:

Using Apache Hue in Cloudera we create our database with the commands below:

* create database stat;
* create table stat.stat_csv (
    Keyword string,
    Market string,
    Location string,
    Device string,
    CrawlDate date,
    Rank string,
    URL string
);
* create table stat.question_one (
    URL string,
    Total int
);
* create table stat.question_two (
    URL string,
    items array<struct<CrawlDate:string, Keyword:string>>
);
* create table stat.question_three (
    Keyword string,
    Market string,
    Location string,
    Device string,
    CrawlDate date,
    Total int
);

#### 1.2 Build

Execute a command: mvn clean package

Maven will generate a JAR called stat-test-0.0.1-SNAPSHOT-jar-with-dependencies.jar

###### 1.2.2 Create a HDFS directory
sudo -u hdfs hadoop fs mkdir /user/outros
hadoop dfs -copyFromLocal /root/getstat_com_serp_report_201707.csv /user/outros
sudo -u hdfs hadoop fs -chown admin /user/outros
sudo -u hdfs hadoop fs -chown admin /user/outros/getstat_com_serp_report_201707.csv


###### 1.2.2 Publish the Apache Spark rotine into YARN

by Command (in the NameNode): 

spark-submit --class com.getstat.analysis.Main --master yarn \

--deploy-mode cluster --conf "spark.yarn.am.waitTime=500s" target/stat-test-0.0.1-SNAPSHOT-jar-with-dependencies.jar /user/outros/getstat_com_serp_report_201707.csv


#### 1.3 Cloudera Environment working:

I create a Cloudera hadoop environment working using cluster with 1 Server NameNode and 3 server Datanode.

To see the environment update your /etc/hosts and add the lines below:

165.227.110.4   hadoop1.virtual.com     hadoop1

165.227.109.170 hadoop2.virtual.com     hadoop2

165.227.109.246 hadoop3.virtual.com     hadoop3

After save /etc/hosts this access in your browser the URL: __http://hadoop1.virtual.com:7180/cmf/login__

User: admin

password: admin


## 2.Analyze the data to answer three important SEO questions

Question 1:

-- Get the URL that be part of TOP 10 and how many times this URL appear in the top 10

Select t.* from ( select URL, count(0) as total from stat_csv_temp
where Rank <= 10
group by URL ) t order by t.total desc

Question 2:

-- With this Select it's possible get by URL all the Keyword changes by Date

-- ROW: www.getstat.com | [{CrawlDate: "01-01-2017", Keyword: "search word"}]


Select t.URL, collect_list(struct(t.CrawlDate, t.Keyword)) as list_items from ( 
	Select URL, CrawlDate, Keyword, count(0) as total from stat_csv_temp 
	where Rank = 1
	group by URL, CrawlDate, Keyword
order by URL, CrawlDate

Question 3:
-- With this Select we get all the Keywords by Market, Location and Device

Select * from (
	Select Keyword, Market, Location, Device, CrawlDate, count(0) as total from stat_csv_temp
	where Device in ('smartphone', 'desktop')
    group by Keyword, Market, Location, Device, CrawlDate
) t order by t.CrawlDate

## 3.PPT Presentation + Other Design Solutions

I created a presentation where I show how to solve the problem passed by STAT, as I show other alternatives to solve the problem like:

- External Table
- Streaming
- Real Time Processing (RTP)

Please Take a look;)
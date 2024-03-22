-- 1.) Download Hive: https://downloads.apache.org/hive/stable-2/apache-hive-2.3.9-bin.tar.gz
-- 2.) Follow installation guide: https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-InstallingHivefromaStableRelease
-- 3.) replace guava in hive with the one in hadoop. (NO NEED TO DO THIS)
-- rm lib/guava-14.0.1.jar
-- cp ../hadoop-3.3.4/share/hadoop/hdfs/lib/guava-27.0-jre.jar ./lib/
-- 4.) Re-create metastore. For to use Derby so we don't have to create a real DB like MySQL or PostgreSQL
-- rm -rf metastore_db
-- ./bin/schematool -initSchema -dbType derby

-- 5.) you need to download the files from AWS into your own HDFS logs/ directory. You don't need a lot. But,
-- make sure you include the partitions.
-- s3://elasticmapreduce/samples/hive-ads/tables/impressions 

-- 6.) set proper path for hcatalog jar. Need for the SERDE.
-- export CLASSPATH="/home/wyy/apache-hive-2.3.9-bin/lib/hive-hcatalog-core-2.3.9.jar"
-- 7.) can run this via ./bin/hive

ADD JAR /home/wyy/apache-hive-2.3.9-bin/lib/hive-hcatalog-core-2.3.9.jar;

ADD JAR /home/mugaddan/apache-hive-2.3.9-bin/lib/hive-hcatalog-core-2.3.9.jar;

DROP TABLE IF EXISTS impressions;
CREATE EXTERNAL TABLE impressions (
    requestbegintime string,
    adid string,
    impressionid string,
    referrer string,
    useragent string,
    usercookie string,
    ip string,
    number string,
    processid string,
    browsercookie string,
    requestendtime string,
    timers struct
                <
                 modellookup:string, 
                 requesttime:string
                >,
    threadid string, 
    hostname string,
    sessionid string
)   
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'hdfs:///user/mugaddan/impressions/';

MSCK REPAIR TABLE impressions;

SELECT COUNT(requestbegintime) FROM impressions;

ADD JAR /usr/lib/hive/lib/hive-hcatalog-core.jar;

rm -rf metastore_db
schematool -initSchema -dbType derby

hadoop dfsadmin -safemode leave

ADD JAR /home/mugaddan/apache-hive-2.3.9-bin/lib/hive-hcatalog-core-2.3.9.jar;
ADD JAR /home/mugaddan/hive-json-serde-0.2.jar;

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
LOCATION 'hdfs:///user/mugaddan/impressions';

MSCK REPAIR TABLE impressions;

SELECT COUNT(requestbegintime) FROM impressions;





DROP TABLE IF EXISTS kloutscore;
CREATE EXTERNAL TABLE kloutscore (
    interactions array,

)   
CREATE EXTERNAL TABLE kloutscore ( 
    interactions array<
        struct<
            demographic:struct<
                gender:string
            >,
            interaction:struct<
                author:struct<
                    username:string, 
                    name:string,
                    id:string
                >,
                id:bigint
            >,
            klout:struct<
                score:int
            >
            twitter:struct<
                created_at:string
            >,
        >
    >
)


interactions [
    {
        "demographic": {
            "gender": "female"
        },
        "interaction": {
            "schema": {
                "version": 3
            },
            "source": "web",
            "author": {
                "username": "Arlisse",
                "name": "Arlisse ",
                "id": 3730879,
                "avatar": "http://a0.twimg.com/profile_images/682637113/image_normal.jpg",
                "link": "http://twitter.com/arlisse"
            },
            "type": "twitter",
            "created_at": "Thu, 14 Feb 2013 15:48:15 +0000",
            "content": "@blitziana verdade, td bem??",
            "id": 59869115194,
            "link": "http://twitter.com/arlisse/statuses/59869115194"
        },


PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'hdfs:///user/mugaddan/kloutscore';

MSCK REPAIR TABLE kloutscore;
SELECT COUNT(requestbegintime) FROM kloutscore;

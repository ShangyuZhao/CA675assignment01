

SELECT * FROM posts WHERE posts.ViewCount > 127402 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 74595 and posts.ViewCount < 127402 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 53210 and posts.ViewCount < 74595 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 41321 and posts.ViewCount < 53210 order by posts.ViewCount desc


 

beeline -u jdbc:hive2://localhost:10000/default -n shangyu_zhao4@datapro-ca675-m org.apache.hive.jdbc.HiveDriver

 

create database if not exists myhive;
create external table posts (Id int, Score int, ViewCount int, Body string, OwnerUserId int, OwnerDisplayName string, Title string, Tags string) row format delimited fields terminated by ',' collection items terminated by '|' map keys terminated by ':' ;


load data local inpath '/home/shangyu_zhao4/posts.csv' overwrite into table posts ;
 


Select score, id, title from posts order by score desc limit 10;
 

Select viewcount, owneruserid from posts order by viewcount desc limit 10；
 

Select count (owneruserid) from posts where body like ‘%cloud%’;
 


create table shangyu as select score, owneruserid from posts order by score desc limit 10;


create table finaldata as select shangyu.owneruserid, posts.body from shangyu left join posts on shangyu.owneruserid =posts.owneruserid;
 
 

hdfs dfs -get /user/hive/warehouse/finaldata /home/shangyu_zhao4/
 



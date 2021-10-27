# CA675assignment01
CA675 Assignment 01: Data Analysis
Student ID: 20210833 
Name: Shangyu Zhao 
Email: shangyu.zhao4@mail.dcu.ie
Task 1.data collection   
In this section, we are required to collect top 200000 posts in StackExchange system, which is a website of question and answer. But because of the fact that we can download only 50000 data at one time, it is necessary to perform four query and download operations. The query commands should as follow:

SELECT * FROM posts WHERE posts.ViewCount > 127402 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 74595 and posts.ViewCount < 127402 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 53210 and posts.ViewCount < 74595 order by posts.ViewCount desc
SELECT * FROM posts WHERE posts.ViewCount > 41321 and posts.ViewCount < 53210 order by posts.ViewCount desc
Here is an example of a screenshot of the results
 

 Task 2. Process the data
I chose hive to finish these two tasks because hive is better suited for database tasks. It is primarily used for static structures and jobs that require frequent analysis. Hive's similarity to SQL makes it an ideal intersection between Hadoop and other BI tools

Step1. Clean the data
Before transferring our dataset to server, it is necessary to clean the dataset due to I found there are a number of “commas” in the “body” column. However, CSV files are separated by commas when load using Hive. So, if the data is uncleaned there would be loads of errors while loading our dataset. As shown in the figure below:
 
In order to deal with this problem, I wrote a python program to clean my data:
import csv
with open('merge.csv', newline='', encoding='utf-8') as f:
    with open('new.csv', 'w', encoding='utf-8', newline='') as fw:
        reader = csv.reader(f)
        writer = csv.writer(fw)
        for row in reader:
            row[8] = row[8].replace(',', ' ')
            row[8] = row[8].replace('\n', ' ')
            row[8] = row[8].replace('\r', ' ')
            row[8] = row[8].replace('<p>', ' ')
            row[8] = row[8].replace('</p >', ' ')
            row[8] = row[8].replace('<li>', ' ')
            row[8] = row[8].replace('</li>', ' ')
            row[8] = row[8].replace('<ul>', ' ')
            row[8] = row[8].replace('</ul>', ' ')
            row[15] = row[15].replace(',', ' ')
            row[15] = row[15].replace('\n', ' ')
            row[15] = row[15].replace('\r', ' ')
            row[16] = row[16].replace(',', ' ')
            row[16] = row[16].replace('\n', ' ')
            row[16] = row[16].replace('\r', ' ')
            writer.writerow(row)
This program is to change the “,” in the “body” column to” ”(space).
Before cleaning:
 
After cleaning:
 

Step 2. loading dataset to hive
Firstly, create the Hadoop cluster (one master with 2 workers)
 
Then get linked to Hive with command 
beeline -u jdbc:hive2://localhost:10000/default -n shangyu_zhao4@datapro-ca675-m org.apache.hive.jdbc.HiveDriver

 
Next, create a database and then create a table to store our data in Hive:
create database if not exists myhive;
create external table posts (Id int, Score int, ViewCount int, Body string, OwnerUserId int, OwnerDisplayName string, Title string, Tags string) row format delimited fields terminated by ',' collection items terminated by '|' map keys terminated by ':' ;
Upload our dataset 
 
Load the dataset:
load data local inpath '/home/shangyu_zhao4/posts.csv' overwrite into table posts ;
 

Task 3. Querying with HIVE
> get top 10 posts by score:
Select score, id, title from posts order by score desc limit 10;
 
> get top 10 users by post score:
Select viewcount, owneruserid from posts order by viewcount desc limit 10；
 
>get the number of distinct users, who used the word “cloud” in one of their posts:
Select count (owneruserid) from posts where body like ‘%cloud%’;
 

Task 4. calculating top10 user’s TF-IDF
Before processing the dataset, it is necessary to collect the fewer data we need from the original dataset.

Firstly, create a table to store information the top ten users:
create table shangyu as select score, owneruserid from posts order by score desc limit 10;

Then, create another table to link “shangyu” table with each user’s posts information:
create table finaldata as select shangyu.owneruserid, posts.body from shangyu left join posts on shangyu.owneruserid =posts.owneruserid;
 
 
Next, copy that file from HDFS to local and download it:
hdfs dfs -get /user/hive/warehouse/finaldata /home/shangyu_zhao4/
 
Lastly, write a program in Java to get the value of top10 user’s TF-IDF


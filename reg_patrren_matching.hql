-- daily check:
add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
select count(*) from XXXXX

@@ Query for list of CP.Content names that are not available in lookup
add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
select CP.content_name from xxxx.RT_Content_name CP 
where CP.content_name not in (select upper(AL.content_name) from XXXX AL )

add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
insert into table XXXX.YYYY
select CP.content_name as content_name,sc.owners as author, 'SalesConnect' as source,'Matching' as Flag
from XXXX.RT_Content_name CP inner join XXXX.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) = regexp_extract(SC.Filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where CP.content_name like 'DATASHEET-C78%'and cp.content_name not in (select AL.content_name from xxxx.moh_author_lookup AL); 


select *
from xxxxx.RT_Content_name CP inner join xxxxx.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) = regexp_extract(SC.filename,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where CP.content_name like 'DATASHEET_C78%' and cp.content_name not in (select AL.content_name from xxxxx.moh_author_lookup AL);

Pattern 2.1:
add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
insert into table xxxx.yyyy
select CP.content_name as content_name,sc.owners as author, 'SalesConnect' as source,'Matching' as Flag
from yyyy.RT_Content_name CP inner join xxxx.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET ]+)([C]+)([0-9]+)([ ]+)([0-9]+)',5) = regexp_extract(SC.Filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where CP.content_name like 'DATASHEET C78%'and cp.content_name not in (select AL.content_name from xxxx.moh_author_lookup AL);


Pattern 2.2: DATASHEET_C78-737332_3.JPG

select CP.content_name as content_name,sc.owners as author, 'SalesConnect' as source,'Matching' as Flag
from xxxx.RT_Content_name CP inner join xxxx.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) = regexp_extract(SC.filename,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where CP.content_name like 'DATASHEET C78%'and cp.content_name not in (select AL.content_name from xxxx.moh_author_lookup AL);

regexp_extract(CP.content_name,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)





select CP.content_name as content_name,sc.owners as author, 'SalesConnect' as source,'Matching' as Flag
from xxxx.RT_Content_name CP inner join xxxx.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET_]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) = regexp_extract(SC.Filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where CP.content_name like 'DATASHEET-C78%'and cp.content_name not in (select AL.content_name from xxxxx.moh_author_lookup AL);



Testing:

select CP.content_name, regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
from xxxx.RT_Content_name CP
where cp.content_name like 'DATASHEET %'





add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
select CP.content_name as content_name,sc.owners as author, 'SalesConnect' as source,'Matching' as Flag
from xxxx.RT_Content_name CP inner join xxxxx.RT_sales_connect SC
on regexp_extract(CP.content_name,'([DATASHEET ]+)([C]+)([0-9]+)([ ]+)([0-9]+)',5) = regexp_extract(SC.Filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != ''and cp.content_name not in (select AL.content_name from mca_misc.moh_author_lookup AL);


select CP.content_name, regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
from xxxx.RT_content_Name CP
where regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != '' and cp.content_name not in (select AL.content_name from mca_misc.moh_author_lookup AL);


select sc.filename, regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
from xxxx.RT_sales_connect SC
where regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != ''


select * from 
(
select CP.content_name, regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
from xxxx.RT_content_Name CP
where regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != '' and cp.content_name not in (select AL.content_name from mca_misc.moh_author_lookup AL)) A
where regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) IN (select regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) 
from xxxx.RT_sales_connect SC
where regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != '')



add jar /hdfs/app/MCAnalytics/lib/csv-serde-1.1.2-0.11.0-all.jar;
insert into table xxxxx.moh_author_lookup
select distinct content_name as content_name,A.owners as author, 'SalesConnect' as source,'Matching' as Flag from (
select CP.content_name, regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) as exp_1,SC.owners
from xxxx.RT_content_Name CP join xxxxx.RT_sales_connect SC 
on regexp_extract(CP.content_name,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) = regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5)
where regexp_extract(SC.filename,'([DATASHEET-]+)([C]+)([0-9]+)([-]+)([0-9]+)',5) != '' and cp.content_name not in (select AL.content_name from mca_misc.moh_author_lookup AL))A











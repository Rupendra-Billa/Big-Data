final table - final_table
stg_table

sample data
product

1000 colgate 100 0  05-06-2019  1 05-06-2019
1000 colgate 100 10 05-06-2019  2 null
1001 pepsident 80 10 05-06-2019 1 null


create final_tmp_table as
select * from final_table where active_flag='yes'


intial step:

create table stg_tmp
SELECT *,
Lead(timestamp,1) over(partition by p1,p2 order by row_num) as last_effective_date
FROM ( select *,
row_number() over(partitioned by p1,p2 order by timestamp_col) as row_num
from stg_table );


-- if records are existed in stg but not in final
create table final_tmp
select stg.id,
stg.name,
stg.timestamp as eff_strt_date,
case when last_effective_date is null then '9999-12-31' else last_effective_date end as eff_end_date,
case when last_effective_date is null then 'yes' else 'no' end as active_flag,
stg.price,
stg.discount
from stg_table stg
left outer join 
final_tmp_table final_tmp
on stg.id = final_tmp.id
and stg.name = final_tmp.name 
where final_tmp.id is null;

-- if records are existed in final and stage.
insert into final_tmp
select tmp.id,
tmp.name,
tmp.eff_strt_date,
(stg.timestamp-1) as eff_end_date,
'no' as active_flag,
tmp.price,
tmp.discount
from 
final_tmp_table tmp
JOIN
stg_table stg
on stg.id = tmp.id
and stg.name = tmp.name
where stg.price != tmp.price OR
stg.discount != tmp.discount
-- above query will in active existed record in final table
UNION ALL
select stg.id,
stg.name,
stg.timestamp as eff_strt_date,
'9999-12-31' as eff_end_date,
'YES' as active_flag,
stg.price,
stg.discount
FROM 
final_tmp_table tmp
JOIN
stg_table stg
on stg.id = tmp.id
and stg.name = tmp.name
where stg.price != tmp.price OR
stg.discount != tmp.discount
-- above query will insert active record in final table


insert overwrite into final_table
select * from final_table WHERE active_flag='NO'
UNION ALL
SELEcT * FROM final_tmp;



=====
CREATE EXTERNAL TABLE hbase_hive_names(hbid INT, id INT,  fn STRING, ln STRING, age INT) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,id:id,name:fn,name:ln,age:age") TBLPROPERTIES("hbase.table.name" = "hbase_2_hive_names");


-----
{
"name" : "rupen"
address:{
"city":"zzz",
"country" : "canada",
"street":"ser"
}


}




























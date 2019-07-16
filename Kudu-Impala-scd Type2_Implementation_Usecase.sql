flag = '0' ====== No change in record
flag = '1' ====== Change in record


CREATE TABLE group_fo_curated_dc (
group_code STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,   
date_from STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,   
date_to STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,   
flag STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
group_name STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,   
PRIMARY KEY (group_code, date_from) ) PARTITION BY HASH (group_code, date_from) 
PARTITIONS 2  COMMENT 'This table stores data for Group' 
STORED AS KUDU;

1. Create temparory table as final table and insert records from final table where records might get changed in the future

CREATE TABLE TEMP_GROUP_FO_CURATED_DC AS 
SELECT * FROM group_fo_curated_dc WHERE date_to = '9999-12-31';

2. Check for changes in the existed records and update date_to column to the old record in the temp table And Insert the new record 

INSERT INTO TEMP_GROUP_FO_CURATED_DC 
SELECT tmp.group_code, tmp.date_from, from_timestamp(to_date(adddate(now(),-1)), 'yyyy-MM-dd') as date_to, '0' as flag, temp.group_name
FROM TEMP_GROUP_FO_CURATED_DC tmp
JOIN group_fo stg
on tmp.group_code = stg.group_code
WHERE tmp.flag='1' 
AND tmp.group_name != stg.group_name
UNION ALL
SELECT stg.group_code, insert_ts as date_from, '9999-12-31' as date_to, '1' as flag, stg.group_name
FROM TEMP_GROUP_FO_CURATED_DC tmp
JOIN group_fo stg
on tmp.group_code = stg.group_code
WHERE tmp.flag='1' 
AND tmp.group_name != stg.group_name;


3. Check for new records in stage table and Insert them into temp table

INSERT INTO TEMP_GROUP_FO_CURATED_DC 
SELECT stg.group_code, insert_ts as date_from, '9999-12-31' as date_to, '1' as flag, stg.group_name
FROM group_fo stg
LEFT OUTER JOIN TEMP_GROUP_FO_CURATED_DC tmp
on tmp.group_code = stg.group_code
WHERE tmp.group_code is null;

4. UPSERT all records from temp table into Kudu final table.

UPSERT INTO group_fo_curated_dc 
SELECT * FROM TEMP_GROUP_FO_CURATED_DC;


USE GENDER_DB;


-- FIRST VERSION OF USING MYSQL 
-- PREVIOUS HIVE COMMANDS FOR A NEW TABLE
-- NOTE: ORIGINAL TABLE MUST EXIST - CHECK create.sql

DROP IF EXISTS DATA_BY_REQ;

-- USING PARTIONING TO GET SPECIFIC DATA
CREATE TABLE DATA_BY_REQ
(COUNTRY_NAME STRING, COUNTRY_CODE STRING, 
INDICATOR_NAME STRING, 
`2000` DOUBLE,
`2001` DOUBLE, `2002` DOUBLE, `2003` DOUBLE, `2004` DOUBLE,
`2005` DOUBLE, `2006` DOUBLE, `2007` DOUBLE, `2008` DOUBLE,
`2009` DOUBLE, `2010` DOUBLE, `2011` DOUBLE, `2012` DOUBLE,
`2013` DOUBLE, `2014` DOUBLE, `2015` DOUBLE, `2016` DOUBLE)
PARTITIONED BY (INDICATOR_CODE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "quotechar" = "\"",
    "seperatorChar" = ","
);


-- USAGE OF VARIABLE MAY NOT WORK, BUT 
-- WANTED TO KEEP INDICATOR CODE HERE FOR
-- SAFEKEEPING
STRING indicatorCode = 'SL.EMP.TOTL.SP.FE.ZS';

INSERT INTO DATA_BY_REQ
-- NOTE: MAY NEED TO HARDCODE INDICATOR CODE
PARTITION (indicator_code = indicatorCode)
SELECT COUNTRY_NAME, COUNTRY_CODE, INDICATOR_NAME, 
`2000`, `2001` , `2002` , `2003` , 
`2004` , `2005` , `2006` , `2007` , `2008` ,
`2009` , `2010` , `2011` , `2012` ,
`2013` , `2014` , `2015` , `2016`
FROM data_by_country
WHERE indicator_code = 'SL.EMP.TOTL.SP.FE.ZS';


CREATE TABLE REQ_SOLUTION
LOCATION 'user/cloudera/hive-share/tables/'
AS
<SELECT COMMAND THAT WILL ANSWER BUSINESS QUESTION>
-- SELECT(((`2016` - `2000`)/ `2000`) * 100)
-- FROM DATA_BY_REQ
-- WHERE country_code = 'WLD';

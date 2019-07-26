USE GENDER_DB;

CREATE TABLE REQ_SOLUTION
-- LOCATION '/home/cloudera/hive-share/tables/'
LOCATION 'user/cloudera/hive-share/tables/'
AS SELECT COUNTRY_NAME, INDICATOR_NAME, INDICATOR_CODE,
((`2000`+`2001`+`2002`+`2003`+`2004`+`2005`+`2006`+`2007`
+`2008`+`2009`+`2010`+`2011`+`2012`+`2013`+`2014`+`2015`+`2016`)/16)
AS 'AVERAGE (2000-2016)' FROM DATA_BY_COUNTRY 
WHERE INDICATOR_CODE='SL.EMP.SELF.FE.ZS';
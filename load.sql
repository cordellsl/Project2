USE GENDER_DB;

LOAD DATA LOCAL INPATH
'/home/cloudera/Data/Gender/Gender_StatsData.csv'
INTO TABLE DATA_BY_COUNTRY;

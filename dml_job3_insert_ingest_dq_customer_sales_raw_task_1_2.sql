-----------------------------------------------------------------------------------------------------------
---START DDL
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS WORKFLOW;
CREATE TABLE WORKFLOW ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , NAME VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TASK;
CREATE TABLE TASK ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , NAME VARCHAR(999) , TASK_TYPE VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS WORKFLOW_TASK;
CREATE TABLE WORKFLOW_TASK ( WF_ID INT , TASK_ID INT , TASK_SEQ INT );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TASK_IO;
CREATE TABLE TASK_IO ( TASK_ID INT , IO_ID INT , IO_SEQ INT , IO_TYPE VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS IO_INFO;
CREATE TABLE IO_INFO ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , STORAGE_TYPE VARCHAR(999) , FILE_PATH VARCHAR(999) , DB_SCHEMA VARCHAR(999) , TABLE_NAME VARCHAR(999) , PARTITIONC_COLS VARCHAR(999) , DATA_NAME VARCHAR(999) , CONN_TYPE VARCHAR(999) , KERBOROS VARCHAR(999) , JNDI VARCHAR(999) , JDBC_URL VARCHAR(999) , USER VARCHAR(999) , PASSWORD VARCHAR(999) , JDBC_DRIVER_CLASS VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS JOB;
CREATE TABLE JOB ( JOB_ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (JOB_ID) , JOB_UID VARCHAR(999) , WF_ID INT , TASK_ID INT , TASK_SEQ INT , SCHEDULE_TIME TIMESTAMP , SUBMIT_TIME TIMESTAMP , START_TIME TIMESTAMP , END_TIME TIMESTAMP , LAST_UPD_TIME TIMESTAMP , STATUS VARCHAR(999) , RESPONSE_JSON TEXT );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS DQ_SUMMARY;
CREATE TABLE DQ_SUMMARY ( JOB_ID INT , JOB_UID VARCHAR(999) , WF_ID INT , TASK_ID INT , DATA_NAME VARCHAR(999) , COLUMN_NAME VARCHAR(999) , DQ_ERROR_CODE VARCHAR(999) , TOTAL_REC_COUNT INT , FAILED_REC_COUNT INT , SUCCESS_REC_COUNT INT , UPD_TIME INT );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER ( ID INT , NAME VARCHAR(100) , EMAIL VARCHAR(100) , CREATED_DATE DATE );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS DATA;
CREATE TABLE DATA ( NAME VARCHAR(999) , TANSFORM_TYPE VARCHAR(999) , TRANSFORM_INPUTS VARCHAR(999) , FITER_FORMULA VARCHAR(999) , COMPOSITE_JOIN_FORMULA VARCHAR(999) , ING_FILE_READ_FORMULA VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS DATA_COLUMN;
CREATE TABLE DATA_COLUMN ( DATA_NAME VARCHAR(999) , NAME VARCHAR(999) , SEQ INT , DATA_TYPE VARCHAR(999) , ENRICH_FORMULA VARCHAR(999) , AGGR_FORMULA VARCHAR(999) , AGGR_DM_TYPE VARCHAR(999) , SET_OP_FORMULA VARCHAR(999) , READ_FORMAT VARCHAR(999) , WRITE_FORMAT VARCHAR(999) , LOOKUP_IO_TYPE VARCHAR(999) , ING_COL_READ_FORMULA VARCHAR(999) , DQ_ERROR_CODE VARCHAR(999) , DQ_ERROR_FORMULA VARCHAR(999) , DISPOSITION_FORMULA VARCHAR(999) );
-----------------------------------------------------------------------------------------------------------
---END DDL
-----------------------------------------------------------------------------------------------------------
---START DML
-----------------------------------------------------------------------------------------------------------
---START DML TASK1
-----------------------------------------------------------------------------------------------------------

delete from DATA where NAME='CUSTOMER1_RAW';
insert into DATA(NAME,ING_FILE_READ_FORMULA) 
values('CUSTOMER1_RAW','
LAYOUT(
					HEADER_LINE_COUNT(2),
					HEADER_IDENTIFIER("H"),
					TAILOR_LINE_COUNT(2),
					TAILOR_IDENTIFIER("T"),
					RECORD_IDENTIFIER("D"),
					RECORD_DELIMITER("|"),
					CHECK_COLUMN_COUNT(),
					TRIM_SPACES()
				)
');



delete from DATA_COLUMN where DATA_NAME='CUSTOMER1_RAW';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('CUSTOMER1_RAW','CUSTOMER_ID',1,'I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('CUSTOMER1_RAW','FIRST',2,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('CUSTOMER1_RAW','LAST',3,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
	values('CUSTOMER1_RAW','ADDRESS',4,'S','NULL_VALUE','IS_EMPTY()','WARN()');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
	values('CUSTOMER1_RAW','ZIPCODE',5,'S','INVALID_ZIP_FROM_LOOKUP','IS_NULL(LOOKUP("LOOKUP_ZIPCODE_STATE"))','REJECT()');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,ING_COL_READ_FORMULA,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
values('CUSTOMER1_RAW','CUST_TYPE',6,'S','LAYOUT(UPPER_CASE())','NULL_VALUE','IS_NULL()','REPLACE("REGULAR")');
-----------------------------------------------------------------------------------------------------------
delete from DATA where NAME='LOOKUP_ZIPCODE_STATE';
insert into DATA(NAME,TANSFORM_TYPE) 
values('LOOKUP_ZIPCODE_STATE','LOOKUP');
delete from DATA_COLUMN where DATA_NAME='LOOKUP_ZIPCODE_STATE';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_ZIPCODE_STATE','ZIPCODE',1,'S','I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_ZIPCODE_STATE','STATE',2,'S','O');
-----------------------------------------------------------------------------------------------------------
delete from DATA where NAME='LOOKUP_ZIPCODE_CITY';
insert into DATA(NAME,TANSFORM_TYPE) 
values('LOOKUP_ZIPCODE_CITY','LOOKUP');
delete from DATA_COLUMN where DATA_NAME='LOOKUP_ZIPCODE_CITY';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_ZIPCODE_CITY','ZIPCODE',1,'S','I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_ZIPCODE_CITY','CITY',2,'S','O');
-----------------------------------------------------------------------------------------------------------
delete from DATA where NAME='LOOKUP_CITYSTATE_ZIPCODE';
insert into DATA(NAME,TANSFORM_TYPE) 
values('LOOKUP_CITYSTATE_ZIPCODE','LOOKUP');
delete from DATA_COLUMN where DATA_NAME='LOOKUP_CITYSTATE_ZIPCODE';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_CITYSTATE_ZIPCODE','CITY',1,'S','I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_CITYSTATE_ZIPCODE','STATE',2,'S','I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_CITYSTATE_ZIPCODE','ZIPCODE',3,'S','O');
-----------------------------------------------------------------------------------------------------------
--trigger
delete from WORKFLOW where NAME='WorkFlow100_IngestDQProc';
insert into WORKFLOW(NAME) values('WorkFlow100_IngestDQProc');
-----------------------------------------------------------------------------------------------------------
delete from TASK where NAME='Task1_Ingest_Customer1_Raw';
insert into TASK(NAME,TASK_TYPE) values('Task1_Ingest_Customer1_Raw','INGEST_DQ');
delete from WORKFLOW_TASK where WF_ID=(select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc');
insert into WORKFLOW_TASK(WF_ID,TASK_ID,TASK_SEQ) 
	select (select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc') as WF_ID,
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	1 as TASK_SEQ;
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/customer_raw.txt' and DATA_NAME='CUSTOMER1_RAW';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('RAW','/tmp/data/raw/customer_raw.txt','CUSTOMER1_RAW');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.csv';
--delete from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.avro';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/customer_ingested_dq.csv');
--insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('AVRO','/tmp/data/curated/customer_ingested_dq.avro');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingestion_dq_errors.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/customer_ingestion_dq_errors.csv');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_CITYSTATE_ZIPCODE.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('CSV','/tmp/data/ref/LOOKUP_CITYSTATE_ZIPCODE.csv','LOOKUP_CITYSTATE_ZIPCODE');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_CITY.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('CSV','/tmp/data/ref/LOOKUP_ZIPCODE_CITY.csv','LOOKUP_ZIPCODE_CITY');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_STATE.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('CSV','/tmp/data/ref/LOOKUP_ZIPCODE_STATE.csv','LOOKUP_ZIPCODE_STATE');
-----------------------------------------------------------------------------------------------------------
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/customer_raw.txt' and DATA_NAME='CUSTOMER1_RAW')
	and IO_SEQ=1
	and IO_TYPE='INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/customer_raw.txt' and DATA_NAME='CUSTOMER1_RAW') as IO_ID,
	1 as IO_SEQ,
	'INPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------	
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.csv')
--	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.avro')
	and IO_SEQ=2
	and IO_TYPE='OUTPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.csv') as IO_ID,
--	(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/customer_ingested_dq.avro') as IO_ID,
	2 as IO_SEQ,
	'OUTPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------	
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingestion_dq_errors.csv')
	and IO_SEQ=3
	and IO_TYPE='REJECT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/customer_ingestion_dq_errors.csv') as IO_ID,
	3 as IO_SEQ,
	'REJECT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------	
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_CITYSTATE_ZIPCODE.csv' and DATA_NAME='LOOKUP_CITYSTATE_ZIPCODE')
	and IO_SEQ=4
	and IO_TYPE='LOOKUP_INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_CITYSTATE_ZIPCODE.csv' and DATA_NAME='LOOKUP_CITYSTATE_ZIPCODE') as IO_ID,
	4 as IO_SEQ,
	'LOOKUP_INPUT' as IO_TYPE;	
-----------------------------------------------------------------------------------------------------------
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_CITY.csv' and DATA_NAME='LOOKUP_ZIPCODE_CITY')
	and IO_SEQ=5
	and IO_TYPE='LOOKUP_INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_CITY.csv' and DATA_NAME='LOOKUP_ZIPCODE_CITY') as IO_ID,
	5 as IO_SEQ,
	'LOOKUP_INPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_STATE.csv' and DATA_NAME='LOOKUP_ZIPCODE_STATE')
	and IO_SEQ=6
	and IO_TYPE='LOOKUP_INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/LOOKUP_ZIPCODE_STATE.csv' and DATA_NAME='LOOKUP_ZIPCODE_STATE') as IO_ID,
	6 as IO_SEQ,
	'LOOKUP_INPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------
---END DML TASK1
-----------------------------------------------------------------------------------------------------------
---START DML - TASK2
-----------------------------------------------------------------------------------------------------------

delete from DATA where NAME='SALES1_RAW';
insert into DATA(NAME,ING_FILE_READ_FORMULA) 
values('SALES1_RAW','
LAYOUT(
					HEADER_LINE_COUNT(2),
					HEADER_IDENTIFIER("H"),
					TAILOR_LINE_COUNT(2),
					TAILOR_IDENTIFIER("T"),
					RECORD_IDENTIFIER("D"),
					RECORD_DELIMITER("|"),
					CHECK_COLUMN_COUNT(),
					TRIM_SPACES()
				)
');
--ORDER_ID|ITEM_TYPE|SALES_CHANNEL|ORDER_PRIORITY|CUSTOMER_ID|ORDER_DATE|SHIP_DATE|UNITS_SOLD|UNIT_PRICE|UNIT_COST
delete from DATA_COLUMN where DATA_NAME='SALES1_RAW';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','ORDER_ID',1,'I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','ITEM_TYPE',2,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','SALES_CHANNEL',3,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','ORDER_PRIORITY',4,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','CUSTOMER_ID',5,'I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','ORDER_DATE',6,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('SALES1_RAW','SHIP_DATE',7,'S');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) values('SALES1_RAW','UNITS_SOLD',8,'N','NULL_VALUE','IS_NULL()','REJECT()');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) values('SALES1_RAW','UNIT_PRICE',9,'N','NULL_VALUE','IS_NULL()','REJECT()');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) values('SALES1_RAW','UNIT_COST',10,'N','NULL_VALUE','IS_NULL()','REJECT()');
-----------------------------------------------------------------------------------------------------------
delete from TASK where NAME='Task2_Ingest_SALES1_RAW';
insert into TASK(NAME,TASK_TYPE) values('Task2_Ingest_SALES1_RAW','INGEST_DQ');
delete from WORKFLOW_TASK where WF_ID=(select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc');
insert into WORKFLOW_TASK(WF_ID,TASK_ID,TASK_SEQ) 
	select (select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc') as WF_ID,
	(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') as TASK_ID,
	2 as TASK_SEQ;
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/sales_raw.txt' and DATA_NAME='SALES1_RAW';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('RAW','/tmp/data/raw/sales_raw.txt','SALES1_RAW');
-----------------------------------------------------------------------------------------------------------
--delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.csv';
delete from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.avro';
--insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/sales_ingested_dq.csv');
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('AVRO','/tmp/data/curated/sales_ingested_dq.avro');
-----------------------------------------------------------------------------------------------------------
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingestion_dq_errors.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/sales_ingestion_dq_errors.csv');
-----------------------------------------------------------------------------------------------------------
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/sales_raw.txt' and DATA_NAME='SALES1_RAW')
	and IO_SEQ=1
	and IO_TYPE='INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/sales_raw.txt' and DATA_NAME='SALES1_RAW') as IO_ID,
	1 as IO_SEQ,
	'INPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------	
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') 
--	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.csv')
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.avro')
	and IO_SEQ=2
	and IO_TYPE='OUTPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') as TASK_ID,
--	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.csv') as IO_ID,
	(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/sales_ingested_dq.avro') as IO_ID,
	2 as IO_SEQ,
	'OUTPUT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------	
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingestion_dq_errors.csv')
	and IO_SEQ=3
	and IO_TYPE='REJECT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/sales_ingestion_dq_errors.csv') as IO_ID,
	3 as IO_SEQ,
	'REJECT' as IO_TYPE;
-----------------------------------------------------------------------------------------------------------
---END DML TASK2
-----------------------------------------------------------------------------------------------------------	
---Add TASK1 & TASK2 TO JOB
-----------------------------------------------------------------------------------------------------------	
delete from JOB where JOB_UID='JOB_BE17AA764B194FCAAC2E679B9653F26A';
insert into JOB(JOB_UID,WF_ID,TASK_ID,TASK_SEQ,SCHEDULE_TIME,LAST_UPD_TIME,STATUS)
select 'JOB_BE17AA764B194FCAAC2E679B9653F26A' as JOB_UID, 
		(select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc') as WF_ID,
		(select ID from TASK where NAME='Task1_Ingest_Customer1_Raw') as TASK_ID,
		1 as TASK_SEQ,
		now() SCHEDULE_TIME,
		now() LAST_UPD_TIME,
		'SCHEDULED'
		;
insert into JOB(JOB_UID,WF_ID,TASK_ID,TASK_SEQ,SCHEDULE_TIME,LAST_UPD_TIME,STATUS)
select 'JOB_BE17AA764B194FCAAC2E679B9653F26A' as JOB_UID, 
		(select ID from WORKFLOW where NAME='WorkFlow100_IngestDQProc') as WF_ID,
		(select ID from TASK where NAME='Task2_Ingest_SALES1_RAW') as TASK_ID,
		2 as TASK_SEQ,
		now() SCHEDULE_TIME,
		now() LAST_UPD_TIME,
		'SCHEDULED'
		;
-----------------------------------------------------------------------------------------------------------
---END DML
-----------------------------------------------------------------------------------------------------------


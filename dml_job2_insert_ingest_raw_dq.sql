DROP TABLE IF EXISTS WORKFLOW;
CREATE TABLE WORKFLOW ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , NAME VARCHAR(999) );
DROP TABLE IF EXISTS TASK;
CREATE TABLE TASK ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , NAME VARCHAR(999) , TASK_TYPE VARCHAR(999) );
DROP TABLE IF EXISTS WORKFLOW_TASK;
CREATE TABLE WORKFLOW_TASK ( WF_ID INT , TASK_ID INT , TASK_SEQ INT );
DROP TABLE IF EXISTS TASK_IO;
CREATE TABLE TASK_IO ( TASK_ID INT , IO_ID INT , IO_SEQ INT , IO_TYPE VARCHAR(999) );
DROP TABLE IF EXISTS IO_INFO;
CREATE TABLE IO_INFO ( ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (ID) , STORAGE_TYPE VARCHAR(999) , FILE_PATH VARCHAR(999) , DB_SCHEMA VARCHAR(999) , TABLE_NAME VARCHAR(999) , PARTITIONC_COLS VARCHAR(999) , DATA_NAME VARCHAR(999) , CONN_TYPE VARCHAR(999) , KERBOROS VARCHAR(999) , JNDI VARCHAR(999) , JDBC_URL VARCHAR(999) , USER VARCHAR(999) , PASSWORD VARCHAR(999) , JDBC_DRIVER_CLASS VARCHAR(999) );
DROP TABLE IF EXISTS JOB;
CREATE TABLE JOB ( JOB_ID INT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (JOB_ID) , JOB_UID VARCHAR(999) , WF_ID INT , TASK_ID INT , TASK_SEQ INT , SCHEDULE_TIME TIMESTAMP , SUBMIT_TIME TIMESTAMP , START_TIME TIMESTAMP , END_TIME TIMESTAMP , LAST_UPD_TIME TIMESTAMP , STATUS VARCHAR(999) , RESPONSE_JSON TEXT );
DROP TABLE IF EXISTS DQ_SUMMARY;
CREATE TABLE DQ_SUMMARY ( JOB_ID INT , JOB_UID VARCHAR(999) , WF_ID INT , TASK_ID INT , DATA_NAME VARCHAR(999) , COLUMN_NAME VARCHAR(999) , DQ_ERROR_CODE VARCHAR(999) , TOTAL_REC_COUNT INT , FAILED_REC_COUNT INT , SUCCESS_REC_COUNT INT , UPD_TIME INT );
DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER ( ID INT , NAME VARCHAR(100) , EMAIL VARCHAR(100) , CREATED_DATE DATE );
DROP TABLE IF EXISTS DATA;
CREATE TABLE DATA ( NAME VARCHAR(999) , TANSFORM_TYPE VARCHAR(999) , TRANSFORM_INPUTS VARCHAR(999) , FITER_FORMULA VARCHAR(999) , COMPOSITE_JOIN_FORMULA VARCHAR(999) , ING_FILE_READ_FORMULA VARCHAR(999) );
DROP TABLE IF EXISTS DATA_COLUMN;
CREATE TABLE DATA_COLUMN ( DATA_NAME VARCHAR(999) , NAME VARCHAR(999) , SEQ INT , DATA_TYPE VARCHAR(999) , ENRICH_FORMULA VARCHAR(999) , AGGR_FORMULA VARCHAR(999) , AGGR_DM_TYPE VARCHAR(999) , SET_OP_FORMULA VARCHAR(999) , READ_FORMAT VARCHAR(999) , WRITE_FORMAT VARCHAR(999) , LOOKUP_IO_TYPE VARCHAR(999) , ING_COL_READ_FORMULA VARCHAR(999) , DQ_ERROR_CODE VARCHAR(999) , DQ_ERROR_FORMULA VARCHAR(999) , DISPOSITION_FORMULA VARCHAR(999) );


delete from DATA where NAME='DATA2';
insert into DATA(NAME,ING_FILE_READ_FORMULA) 
values('DATA2','
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


delete from DATA_COLUMN where DATA_NAME='DATA2';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('DATA2','ID',1,'I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE) values('DATA2','AGE',2,'I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,ING_COL_READ_FORMULA,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
values('DATA2','GENDER',3,'S','LAYOUT(REPLACE("M","Male"),REPLACE("F","Female"))','NOT_IN_LOOKUP','NOT_IN_LIST_COMMA(LOOKUP("LOOKUP_GENDER"),"M,F")','REPLACE("UNKNOWN")');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,ING_COL_READ_FORMULA,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
values('DATA2','INCOME',4,'N','LAYOUT(STRIP_CHARS("$#{},"))','NULL_VALUE','IS_NULL()','REJECT()');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,ING_COL_READ_FORMULA) 
values('DATA2','STATE',5,'S','LAYOUT(UPPER_CASE())');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
values('DATA2','HIGHT',6,'N','VALUE_NOT_IN_RANGE','AND(IS_NOT_NULL(),GREATER_THAN(COL("WEIGHT"),100),LESS_THAN(180))','REJECT()');	
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,DQ_ERROR_CODE,DQ_ERROR_FORMULA,DISPOSITION_FORMULA) 
values('DATA2','WEIGHT',7,'N','NULL_VALUE','IS_NULL()','WARN()');

delete from DATA where NAME='LOOKUP_GENDER';
insert into DATA(NAME,TANSFORM_TYPE) 
values('LOOKUP_GENDER','LOOKUP');
delete from DATA_COLUMN where DATA_NAME='LOOKUP_GENDER';
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_GENDER','GENDER_LONG',1,'S','I');
insert into DATA_COLUMN(DATA_NAME,NAME,SEQ,DATA_TYPE,LOOKUP_IO_TYPE) values('LOOKUP_GENDER','GENDER_SHORT',2,'S','O');

--trigger
delete from WORKFLOW where NAME='WorkFlow2_IngestDQ';
insert into WORKFLOW(NAME) values('WorkFlow2_IngestDQ');

delete from TASK where NAME='Task2_IngestDQ';
insert into TASK(NAME,TASK_TYPE) values('Task2_IngestDQ','INGEST_DQ');
delete from WORKFLOW_TASK where WF_ID=(select ID from WORKFLOW where NAME='WorkFlow2_IngestDQ');
insert into WORKFLOW_TASK(WF_ID,TASK_ID,TASK_SEQ) 
	select (select ID from WORKFLOW where NAME='WorkFlow2_IngestDQ') as WF_ID,
	(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
	1 as TASK_SEQ;
delete from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/rawdata.txt' and DATA_NAME='DATA2';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('RAW','/tmp/data/raw/rawdata.txt','DATA2');
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingested_dq.csv';
--delete from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/rawdata_ingested.avro';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/rawdata_ingested_dq.csv');
--insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('AVRO','/tmp/data/curated/rawdata_ingested.avro');
delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingestion_dq_errors.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH) values('CSV','/tmp/data/curated/rawdata_ingestion_dq_errors.csv');

delete from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/lookup_gender.csv';
insert into IO_INFO(STORAGE_TYPE,FILE_PATH,DATA_NAME) values('CSV','/tmp/data/ref/lookup_gender.csv','LOOKUP_GENDER');


delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_IngestDQ') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/rawdata.txt' and DATA_NAME='DATA2')
	and IO_SEQ=1
	and IO_TYPE='INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='RAW' and FILE_PATH='/tmp/data/raw/rawdata.txt' and DATA_NAME='DATA2') as IO_ID,
	1 as IO_SEQ,
	'INPUT' as IO_TYPE;
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_IngestDQ') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingested_dq.csv')
--	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/rawdata_ingested.avro')
	and IO_SEQ=2
	and IO_TYPE='OUTPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingested_dq.csv') as IO_ID,
--	(select ID from IO_INFO where STORAGE_TYPE='AVRO' and FILE_PATH='/tmp/data/curated/rawdata_ingested.avro') as IO_ID,
	2 as IO_SEQ,
	'OUTPUT' as IO_TYPE;
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_IngestDQ') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingestion_dq_errors.csv')
	and IO_SEQ=3
	and IO_TYPE='REJECT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/curated/rawdata_ingestion_dq_errors.csv') as IO_ID,
	3 as IO_SEQ,
	'REJECT' as IO_TYPE;
delete from TASK_IO where 1=1 
	and TASK_ID=(select ID from TASK where NAME='Task2_IngestDQ') 
	and IO_ID=(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/lookup_gender.csv' and DATA_NAME='LOOKUP_GENDER')
	and IO_SEQ=4
	and IO_TYPE='LOOKUP_INPUT';
insert into TASK_IO(TASK_ID,IO_ID,IO_SEQ,IO_TYPE) select  
	(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
	(select ID from IO_INFO where STORAGE_TYPE='CSV' and FILE_PATH='/tmp/data/ref/lookup_gender.csv' and DATA_NAME='LOOKUP_GENDER') as IO_ID,
	4 as IO_SEQ,
	'LOOKUP_INPUT' as IO_TYPE;	
delete from JOB where JOB_UID='JOB_9ED2CB4BB3374B88B3FB1210D36F663E';
insert into JOB(JOB_UID,WF_ID,TASK_ID,TASK_SEQ,SCHEDULE_TIME,LAST_UPD_TIME,STATUS)
select 'JOB_9ED2CB4BB3374B88B3FB1210D36F663E' as JOB_UID, 
		(select ID from WORKFLOW where NAME='WorkFlow2_IngestDQ') as WF_ID,
		(select ID from TASK where NAME='Task2_IngestDQ') as TASK_ID,
		1 as TASK_SEQ,
		now() SCHEDULE_TIME,
		now() LAST_UPD_TIME,
		'SCHEDULED'
		;



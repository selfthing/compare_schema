#! /usr/bin/python
#coding:utf-8

# load module
import MySQLdb
import sys,getopt
import commands

# variables 
tgt_dbname = "t2"
src_dbname = "t1"
num_batch = 10 

# create database source , tmp ,target database connections 
src_db = MySQLdb.connect("127.0.0.1","root","","",port=3306)
src_cursor = src_db.cursor()
tmp_db = MySQLdb.connect("127.0.0.1","root","","tmp_t1",port=3306)
tmp_cursor = tmp_db.cursor()
# connect to target database , get table name who got a primary key 
tgt_db = MySQLdb.connect("127.0.0.1","root","","",port=3306)
tgt_cursor = tgt_db.cursor()
tgt_cursor.execute("select distinct table_name from information_schema.columns where table_schema='"+tgt_dbname+"' and COLUMN_KEY='PRI' and table_name not like 'tmp%' and table_name not in ('keep','smsmTMP','payment_tmp','tb1','tempc','SMSMTArchive');")
tgt_tables = tgt_cursor.fetchall()
if(len(tgt_tables)==0):
	print "there is no table in target database , please create table first!"
	sys.exit(1)
for row in tgt_tables :
	tgt_table = row[0]
	print "d1.get target table name:"+tgt_table+"."
# get primary key or unique key 
	flag_unique=0
	pris=""
	tgt_cursor.execute("select group_concat(column_name) as primary_key from information_schema.columns where table_schema='"+tgt_dbname+"' and table_name='"+tgt_table+"' and COLUMN_KEY='PRI' order by ORDINAL_POSITION;")
	tgt_table_pri = tgt_cursor.fetchall()
	if(len(tgt_table_pri)>0):
		pris=tgt_table_pri[0][0]
		flag_unique=1
		print "d2.get primary key:"+pris
	else:
		tgt_cursor.execute("select group_concat(column_name) as unique_key  from information_schema.STATISTICS where table_schema='"+tgt_dbname+"' and table_name='"+tgt_table+"' and NON_UNIQUE=0 and INDEX_NAME<>'PRIMARY' order by SEQ_IN_INDEX;")
		tgt_table_pri = tgt_cursor.fetchall()
		if(len(tgt_table_pri)>0):
			pris=tgt_table_pri[0][0]
			flag_unique=1
			print "d2.get primary key:"+pris
		else:
			pass
# check if there is lastmodifieddate or last_modified_date
	tgt_cursor.execute("select column_name from information_schema.columns where table_schema='"+tgt_dbname+"' and table_name='"+tgt_table+"' and column_name in ('lastmodifieddate','last_modified_date');")
	tgt_modifieds = tgt_cursor.fetchall()
	flag_last_modified=0
	if(len(tgt_modifieds)>0):
		flag_last_modified=1
		tgt_modified=tgt_modifieds[0][0]
		print "d3.get last modified column:"+tgt_modified
	else:
		print "d3.no last modified column"
# if there is last modified date , get max last modified date on tgt 
	if flag_last_modified == 1:
		tgt_cursor.execute("select ifnull(max("+tgt_modified+"),'0000-00-00') as max_modified from "+tgt_dbname+"."+tgt_table+";")	
		data_max_modifieds = tgt_cursor.fetchall()
		data_max_modified = data_max_modifieds[0][0]
# get count in source where the lastmodifieddate>=data_max_modified
		sql_get_count = "select count(*) from "+src_dbname+"."+tgt_table+" where "+tgt_modified+">='"+str(data_max_modified)+"';"
		print sql_get_count
		src_cursor.execute(sql_get_count)
		data_counts = src_cursor.fetchall()
		data_count_diff = data_counts[0][0]
		print "d4.the diff count is "+str(data_count_diff)
		num_count = data_count_diff/num_batch + 1 
# iterate num_count , get dump info , then insert into tmp database 
		for i in range(num_count):
			dump_state="/usr/bin/mysqldump --add-locks=FALSE --replace --skip-comments --single-transaction -uroot -t -h127.0.0.1 "+src_dbname+" "+tgt_table+" --where=\""+tgt_modified+">='"+data_max_modified+"' order by "+pris+" limit "+str(i*num_batch)+","+str(num_batch)+"\""
			print dump_state
			dump_status,dump_result_tmp = commands.getstatusoutput(dump_state)
			num_s=dump_result_tmp.find('DISABLE KEYS *')
			num_e=dump_result_tmp.find('ALTER TABLE',num_s)
			sql_insert=dump_result_tmp[num_s+17:num_e-10]
			print sql_insert
			tmp_cursor.execute(sql_insert)
			tmp_db.commit()
			sql_scrub_1="update t02 set id=id+1600;"
			tmp_cursor.execute(sql_scrub_1)
			tmp_db.commit()
# connect to target mysql , select tmp table data insert into target table ,then truncate tmp table data
			#tgt_cursor.execute("truncate tmp_t1."+tgt_table)
			tgt_cursor.execute("insert into "+tgt_dbname+"."+tgt_table+" select * from tmp_t1."+tgt_table)
			tgt_db.commit()
			tgt_cursor.execute("truncate tmp_t1."+tgt_table)
		
"""
# dump data , and extract only insert sql
dump_status,dump_result_tmp = commands.getstatusoutput('/usr/bin/mysqldump --add-locks=FALSE --replace --skip-comments --single-transaction -uroot -t -h127.0.0.1 t1 t01')
num_s=dump_result_tmp.find('DISABLE KEYS *')
num_e=dump_result_tmp.find('ALTER TABLE',num_s)
sql_insert=dump_result_tmp[num_s+17:num_e-10]

# connect to temp databae , insert data and scrub data
tmp_db = MySQLdb.connect("127.0.0.1","root","","tmp_t1",port=3306)
tmp_cursor = tmp_db.cursor()
tmp_cursor.execute(sql_insert)
tmp_db.commit()
sql_scrub_1="update t01 set id=id+160;"
sql_scrub_2="update t01 set id=id+160;"
tmp_cursor.execute(sql_scrub_1)
tmp_cursor.execute(sql_scrub_2)
tmp_db.commit()

# connect to target mysql , select tmp table data insert into target table ,then truncate tmp table data
tgt_cursor.execute("insert into t2.t01 select * from tmp_t1.t01")
tgt_db.commit()
tgt_cursor.execute("truncate tmp_t1.t01")
tgt_db.commit()

tmp_db.close()
tgt_db.close()
"""
tgt_db.close()

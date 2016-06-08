#! /usr/bin/python
#coding:utf-8

# load module
import MySQLdb
import sys,getopt

# help info 
def help_info():
        print '\n'+'*'*155
        print """
This script is used for comparing two databases' schema,including table's column and index.

Usage:
compare database:
compare_schema.py --src_host=xx --src_port=xx --src_user=xx --src_pwd=xx --src_db=xx --tgt_user=xx --tgt_host=xx --tgt-port=xx --tgt_pwd=xx --tgt_db=xx

compare table only:
compare_schema.py --src_host=xx --src_port=xx --src_user=xx --src_pwd=xx --src_db=xx --tgt_user=xx --tgt_host=xx --tgt-port=xx --tgt_pwd=xx --tgt_db=xx --src_table=xx --tgt_table=xx


OPTIONS:
   --src_host      IP address of source database's host
   --src_user      User of source database,should least have select privilege of source database
   --src_port      Port of source database,default 3306
   --src_pwd       Password of src_user
   --src_db        Name of source database
   --tgt_host      IP address of target database's host
   --tgt_port      Port of target database,default 3306
   --tgt_user      User of target database,should least have select privilege of target database,and create temporary table
                    privilege of mysql database
   --tgt_pwd       Password of tgt_user
   --tgt_db        Name of target database

Prerequisites:
   Need to install mysql module for python.
"""
        print '\n'+'*'*155

# get parameters
if sys.argv[1]=="--help":
	help_info()
	sys.exit(1)
try:
	opts , args=getopt.getopt(sys.argv[1:],"s:t:",
	["src_user=","src_host=","src_pwd=","src_port=","src_db=","src_table=",
 	"tgt_user=","tgt_host=","tgt_pwd=","tgt_port=","tgt_db=","tgt_table="]);
except:
	print "\n--error:bad parameter!\n"
	sys.exit(1)
conn_info={}
 # 2 means start from 2,cause there are two '-' before word
for key,value in opts:
	conn_info[key[2:]]=value
if len(sys.argv)==1 :
	print "please add parameter,the format is like: --src_host=127.0.0.1 --src_user=dbuser --src_pwd=1 --src_db=test --src_table=a --tgt_user=dbuser --tgt_host=127.0.0.1 --tgt_pwd=1 --tgt_db=test --tgt_table=a"
        sys.exit(1)
if len(args)!=0 :
	print "please add correct parameter,the format is like:--src_host=127.0.0.1 --src_user=dbuser --src_pwd=1 --src_db=test --src_table=a --tgt_user=dbuser --tgt_host=127.0.0.1 --tgt_pwd=1 --tgt_db=test --tgt_table=a"
	sys.exit(1)
# set default values
conn_info.setdefault('src_port',3306);
conn_info.setdefault('tgt_port',3306);
conn_info.setdefault('src_table','');
conn_info.setdefault('tgt_table','');

# connect test
import MySQLdb
# open db connections
try:
	src_db = MySQLdb.connect(conn_info['src_host'],conn_info['src_user'],conn_info['src_pwd'],'',conn_info['src_port'])
	src_cursor = src_db.cursor()
	src_cursor.execute("SELECT VERSION()")
	src_data = src_cursor.fetchone()
except:
	print "---error:can't conn source database!"
try:
        tgt_db = MySQLdb.connect(conn_info['tgt_host'],conn_info['tgt_user'],conn_info['tgt_pwd'],'',conn_info['tgt_port'])
        tgt_cursor = tgt_db.cursor()
        tgt_cursor.execute("SELECT VERSION()")
        tgt_data = tgt_cursor.fetchone()
except:
	print "---error:can't conn target database!"
# function to replace null value
def ifnull(var, val):
  if var is None:
    return val
  return var

# compare tables
def compare_tables(src_db,tgt_db):
	print "--start to compare tables:\n"
	src_cursor.execute("select SCHEMA_NAME from information_schema.SCHEMATA where SCHEMA_NAME='"+src_db+"';")
	src_data = src_cursor.fetchall()
	if len(src_data)==0:
		print "---error:source db does not exist!\n"
		sys.exit(1)
        src_cursor.execute("select table_name from information_schema.TABLES where table_schema='"+src_db+"'")
        src_data = src_cursor.fetchall()
        if len(src_data)==0:
                print "---error:there is no table in source db!\n"
                sys.exit(1)
	tgt_cursor.execute("select SCHEMA_NAME from information_schema.SCHEMATA where SCHEMA_NAME='"+tgt_db+"';")
	tgt_data = tgt_cursor.fetchall()
	if len(tgt_data)==0:
		print "---error:target db does not exist!\n"
		sys.exit(1)
	
	tgt_cursor.execute("create temporary table mysql.tmp_tablesa(table_name varchar(64) primary key)")
	tgt_cursor.execute("create temporary table mysql.tmp_tablesb(table_name varchar(64) primary key)")
	tgt_cursor.execute("insert into mysql.tmp_tablesb select table_name from information_schema.tables where table_schema='"+tgt_db+"';")
	for row in src_data:
		sql="insert into mysql.tmp_tablesa select '"+row[0]+"'"
		#print sql
		tgt_cursor.execute(sql)
	atob=btoa=1
	compare_atob="select table_name from mysql.tmp_tablesa where table_name not in (select table_name from mysql.tmp_tablesb)"
	tgt_cursor.execute(compare_atob)
	tgt_data=tgt_cursor.fetchall()
	if len(tgt_data)>0 :
		print "--source db has "+str(len(tgt_data))+" tables more than target db.You should execute following sql to add table:\n"
		for row in tgt_data:
			sql="show create table "+src_db+"."+row[0]
			#print sql
			src_cursor.execute(sql)
			src_data=src_cursor.fetchall()
			for row in src_data:
				print row[1]+";\n"
	else:
		atob=0
		#print "--source db has equal or less table than target db"
	compare_btoa="select table_name from mysql.tmp_tablesb where table_name not in (select table_name from mysql.tmp_tablesa)"
	tgt_cursor.execute(compare_btoa)
	tgt_data=tgt_cursor.fetchall()
	if len(tgt_data)>0 :
		print "--target db has "+str(len(tgt_data))+" tables more than source db.You should execute following sql to drop table:\n"	
		for row in tgt_data:
			print "drop table "+tgt_db+"."+row[0]+";\n"
	else:
		btoa=0
		#print "--target db has equal or less table than source db"
	if atob!=0 or btoa!=0:
		print "--source db and target db have different table,execute sql first to amend this first\n"
	if atob==0 and btoa==0:
		print "--source db and target has same table names,continue to compare columns:\n"
		sql="select table_name from information_schema.tables where table_schema='"+src_db+"'";
		src_cursor.execute(sql)
		src_data=src_cursor.fetchall()
		for row in src_data:
			compare_columns(src_db,row[0],tgt_db,row[0])
			
def compare_columns(src_db,src_table,tgt_db,tgt_table):
	print "--start to compare columns between "+src_db+"."+src_table+" and "+tgt_db+"."+tgt_table+":\n"
	src_cursor.execute("select * from information_schema.columns where table_schema='"+src_db+"' and table_name='"+src_table+"'");
	src_data = src_cursor.fetchall()
	if len(src_data)==0:
		print "---error:there is no source table "+src_db+"."+src_table+"!\n"
		sys.exit(1)
	tgt_cursor.execute("select * from information_schema.columns where table_schema='"+tgt_db+"' and table_name='"+tgt_table+"'");
	tgt_data = tgt_cursor.fetchall()
	if len(tgt_data)==0:
		print "---error:there is no target table "+tgt_db+"."+tgt_table+"!\n"
		sys.exit(1)
	tgt_cursor.execute("create temporary table mysql.tmp_columnsa like information_schema.columns")	
	tgt_cursor.execute("create temporary table mysql.tmp_columnsb like information_schema.columns")
	tgt_cursor.execute("insert into mysql.tmp_columnsb select * from information_schema.columns where table_schema='"+tgt_db+"' and table_name='"+tgt_table+"'");
	# insert into target tmp table 
	# 这里改成遍历数组的方式,而且需要判断是否为NULL,是NULL的话,左右不加单引号
	for row in src_data:
		sql="insert into mysql.tmp_columnsa values("
		for i in range(len(row)):
			if row[i] is None:
				item="NULL,"
			else:
				item="'"+str(row[i]).replace("'",r"\'")+"',"
			sql+=item
		sql=sql[:-1]
		sql=sql+");"
		#print sql
		tgt_cursor.execute(sql)
	atob=btoa=1
	compare_atob='''select a.COLUMN_NAME,a.COLUMN_TYPE,case a.IS_NULLABLE when 'YES' then '' when 'NO' then 'NOT NULL' end as c3,
	case ifnull(a.COLUMN_DEFAULT,'-') when '-' then '' else concat('DEFAULT \'',COLUMN_DEFAULT,'\'') end as c4,
	case a.COLUMN_KEY when 'PRI' then 'Primary key' else '' end as c5,
	a.EXTRA from 
	mysql.tmp_columnsa a left outer join mysql.tmp_columnsb b on  
	a.COLUMN_NAME=b.COLUMN_NAME and 
	ifnull(a.COLUMN_DEFAULT,0)=ifnull(b.COLUMN_DEFAULT,0) and
	ifnull(a.IS_NULLABLE,0)=ifnull(b.IS_NULLABLE,0) and
	ifnull(a.DATA_TYPE,0)=ifnull(b.DATA_TYPE,0) and
	ifnull(a.CHARACTER_MAXIMUM_LENGTH,0)=ifnull(b.CHARACTER_MAXIMUM_LENGTH,0) and
	ifnull(a.CHARACTER_OCTET_LENGTH,0)=ifnull(b.CHARACTER_OCTET_LENGTH,0) and
	ifnull(a.NUMERIC_PRECISION,0)=ifnull(b.NUMERIC_PRECISION,0) and
	ifnull(a.NUMERIC_SCALE,0)=ifnull(b.NUMERIC_SCALE,0) and
	ifnull(a.DATETIME_PRECISION,0)=ifnull(b.DATETIME_PRECISION,0) and
	ifnull(a.CHARACTER_SET_NAME,0)=ifnull(b.CHARACTER_SET_NAME,0) and
	ifnull(a.COLLATION_NAME,0)=ifnull(b.COLLATION_NAME,0) and
	ifnull(a.COLUMN_TYPE,0)=ifnull(b.COLUMN_TYPE,0) and
	ifnull(a.EXTRA,0)=ifnull(b.EXTRA,0) and
	ifnull(a.PRIVILEGES,0)=ifnull(b.PRIVILEGES,0) and
	ifnull(a.COLUMN_COMMENT,0)=ifnull(b.COLUMN_COMMENT,0) 
	where  b.COLUMN_NAME is NULL;'''
	tgt_cursor.execute(compare_atob);
	tgt_data = tgt_cursor.fetchall()
	if len(tgt_data)>0:
		print "--source table "+src_table+" has more columns than target "+tgt_table+",you can execute following statement to add different columns on target:\n"
		for row in tgt_data:
			tgt_cursor.execute("select COLUMN_NAME from mysql.tmp_columnsb where COLUMN_NAME='"+row[0]+"'");
			tgt_data_tmp = tgt_cursor.fetchall()
			if len(tgt_data_tmp)>0:
				sql="alter table "+tgt_table+" modify "+row[0]+" "+row[1]+" "+row[2]+" "+row[3]+" "+row[4]+" "+row[5]
				sql=sql.rstrip()
				sql=sql+";\n"
				print sql
			else:
				sql="alter table "+tgt_table+" add "+row[0]+" "+row[1]+" "+row[2]+" "+row[3]+" "+row[4]+" "+row[5]
				sql=sql.rstrip()
				sql=sql+";\n"
				print sql
	else:	
		atob=0
		#print "--source table "+src_table+" has equal or less column than target table "+tgt_table+"."
	compare_btoa='''select b.COLUMN_NAME,b.COLUMN_TYPE,b.COLUMN_DEFAULT from 
	mysql.tmp_columnsa a right outer join mysql.tmp_columnsb b on  
	a.COLUMN_NAME=b.COLUMN_NAME and 
	ifnull(a.COLUMN_DEFAULT,0)=ifnull(b.COLUMN_DEFAULT,0) and
	ifnull(a.IS_NULLABLE,0)=ifnull(b.IS_NULLABLE,0) and
	ifnull(a.DATA_TYPE,0)=ifnull(b.DATA_TYPE,0) and
	ifnull(a.CHARACTER_MAXIMUM_LENGTH,0)=ifnull(b.CHARACTER_MAXIMUM_LENGTH,0) and
	ifnull(a.CHARACTER_OCTET_LENGTH,0)=ifnull(b.CHARACTER_OCTET_LENGTH,0) and
	ifnull(a.NUMERIC_PRECISION,0)=ifnull(b.NUMERIC_PRECISION,0) and
	ifnull(a.NUMERIC_SCALE,0)=ifnull(b.NUMERIC_SCALE,0) and
	ifnull(a.DATETIME_PRECISION,0)=ifnull(b.DATETIME_PRECISION,0) and
	ifnull(a.CHARACTER_SET_NAME,0)=ifnull(b.CHARACTER_SET_NAME,0) and
	ifnull(a.COLLATION_NAME,0)=ifnull(b.COLLATION_NAME,0) and
	ifnull(a.COLUMN_TYPE,0)=ifnull(b.COLUMN_TYPE,0) and
	ifnull(a.EXTRA,0)=ifnull(b.EXTRA,0) and
	ifnull(a.PRIVILEGES,0)=ifnull(b.PRIVILEGES,0) and
	ifnull(a.COLUMN_COMMENT,0)=ifnull(b.COLUMN_COMMENT,0) 
	where a.COLUMN_NAME is NULL;'''
        tgt_cursor.execute(compare_btoa);
        tgt_data = tgt_cursor.fetchall()
        if len(tgt_data)>0:
                print "--target table "+tgt_table+" has more columns than "+src_table+".you can execute following statement to drop needless columns on target:\n"
                for row in tgt_data:
			tgt_cursor.execute("select COLUMN_NAME from mysql.tmp_columnsa where COLUMN_NAME='"+row[0]+"'");
			tgt_data_tmp = tgt_cursor.fetchall()
			if len(tgt_data_tmp)>0:
				pass
			else:
                        	sql="alter table "+tgt_table+" drop "+row[0]+";\n"
                        	print sql
	else:
		btoa=0
		#print "--target table "+tgt_table+" has equal or less column than source table "+src_table+".\n"
	if atob==0 and btoa==0:
		print "--source "+src_db+"."+src_table+"'s columns is totally same with target "+tgt_db+"."+tgt_table+"'s.\n"
		tgt_cursor.execute("drop table mysql.tmp_columnsa");
		tgt_cursor.execute("drop table mysql.tmp_columnsb");
		compare_indexes(src_db,src_table,tgt_db,tgt_table)
	if atob!=0 or btoa!=0:
                print "--source table and target table have different columns,execute sql first to amend this first\n"
		tgt_cursor.execute("drop table mysql.tmp_columnsa");
		tgt_cursor.execute("drop table mysql.tmp_columnsb");

#compare index
def compare_indexes(src_db,src_table,tgt_db,tgt_table):
	print "--start to compare indexes between "+src_db+"."+src_table+" and "+tgt_db+"."+tgt_table+":\n"
        src_cursor.execute("select table_name,index_name,case NON_UNIQUE when 1 then '' when 0 then 'unique' end isunique,group_concat(column_name order by SEQ_IN_INDEX asc) as columns from information_schema.STATISTICS where table_schema='"+src_db+"' and table_name='"+src_table+"' group by index_name")
        src_data = src_cursor.fetchall()
        tgt_cursor.execute("create temporary table mysql.tmp_indexa(table_name varchar(64),index_name varchar(64),isunique varchar(10),columns varchar(255))")
        tgt_cursor.execute("create temporary table mysql.tmp_indexb(table_name varchar(64),index_name varchar(64),isunique varchar(10),columns varchar(255))")
        tgt_cursor.execute("insert into mysql.tmp_indexb select table_name,index_name,case NON_UNIQUE when 1 then '' when 0 then 'unique' end isunique,group_concat(column_name order by SEQ_IN_INDEX asc) as columns from information_schema.STATISTICS where table_schema='"+tgt_db+"' and table_name='"+tgt_table+"' group by index_name")
        for row in src_data:
                sql="insert into mysql.tmp_indexa select '"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"';"
                #print sql
                tgt_cursor.execute(sql)
	btoa=atob=1
	compare_btoa="select * from mysql.tmp_indexb where concat(isunique,columns) not in (select concat(isunique,columns) from mysql.tmp_indexa);"
	tgt_cursor.execute(compare_btoa);
        tgt_data = tgt_cursor.fetchall()
        if len(tgt_data)>0:
		print "--target "+tgt_db+"."+tgt_table+" has more index than source "+src_db+"."+src_table+" , execute following ddl to drop index:\n"
		for row in tgt_data:
			if row[1]=='PRIMARY':
    				sql="alter table "+tgt_table+" drop primary key;"
			else:
     				sql="alter table "+tgt_table+" drop key "+row[1]+";"
			print sql+"\n"
	else:
		btoa=0
	compare_atob="select * from mysql.tmp_indexa where concat(isunique,columns) not in (select concat(isunique,columns) from mysql.tmp_indexb);"
	tgt_cursor.execute(compare_atob);
        tgt_data = tgt_cursor.fetchall()
        if len(tgt_data)>0:
		print "--source "+src_db+"."+src_table+" has more index than target "+tgt_db+"."+tgt_table+" , execute following ddl to add index:\n"
		for row in tgt_data:
			if row[1]=='PRIMARY':
    				#sql="alter table "+tgt_table+" add primary key("+row[3]+");"
				pass
			else:
    				sql="alter table "+tgt_table+" add "+row[2]+" key("+row[3]+");"
				print sql+"\n"
	else:
		atob=0
	if atob==0 and btoa==0:
        	print "--"+src_db+"."+src_table+"'s index is totally same with "+tgt_db+"."+tgt_table+"'s.\n"
        tgt_cursor.execute("drop table mysql.tmp_indexa");
        tgt_cursor.execute("drop table mysql.tmp_indexb");

# start compare
print "\n--start to compare:\n"
if conn_info['src_table']=='' and conn_info['tgt_table']=='' :
	compare_tables(conn_info['src_db'],conn_info['tgt_db'])
if conn_info['src_table']!='' and conn_info['tgt_table']!='' :
	compare_columns(conn_info['src_db'],conn_info['src_table'],conn_info['tgt_db'],conn_info['tgt_table'])	

src_db.close()
tgt_db.close()

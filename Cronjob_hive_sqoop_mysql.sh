#!/bin/bash

source ~/.bashrc

HOME_DIR=/home/probis/dsc/netwan_v2/job
#HIVE_SQL=${HOME_DIR}/netwan-drop-create-performance.sql

LOG_DIR=${HOME_DIR}/log
LOG_HIVE=${LOG_DIR}/hive-create.log
LOG_SQOOP=${LOG_DIR}/sqoop.log
LOG_NZ=${LOG_DIR}/nz.log

#EXEC_NZSQL="/usr/local/nz/bin/nzsql -host 10.62.x.x -u usr_probis -q"

tahun=`date +%Y`
bulan=`date +%m`
tanggal=`date +%d --date='1 days ago'`


# run Hive sql
hive -e "use dsc_bigdata;

drop table dsc_bigdata.netwan2_alarm;

create table dsc_bigdata.netwan2_alarm as 
select 
regexp_extract(log_alarm,'1.3.6.1.2.1.1.3.0 = (\\d+)')day,
regexp_extract(log_alarm,', ([\\d\\:\\.]+),')jam,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.0 = ([\\w\\[\\]\\(\\)\\s]+)')desc1,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.1 = ([\\d]+)')severity,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.2 = ([\\w\\[\\]\\(\\)\\s\\;\\-]+)')lok,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.3 = ([\\w\\-]+)')hostname,
from_unixtime(cast(regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.6 = ([\\d]+)') as bigint)) as time_epoch,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.7 = ([\\d\\.]+)')ip_address,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.8 = ([\\w\\s]+)')ip_address2,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.9 = ([\\w\\-]+)')brand,
regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.10 = ([\\w\\.]+)')ip_port,
from_unixtime(cast(regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.11 = ([\\d]+)') as bigint))time_eopch2,
from_unixtime(cast(regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.12 = ([\\d]+)') as bigint))time_eopch3
from probis_db_ext.alarm_stg where log_alarm like '%Variables%' and tahun='${tahun}' and bulan='${bulan}' and tanggal='${tanggal}' 
and regexp_extract(log_alarm,' 1.3.6.1.4.1.1279.1 = ([\\d]+)') is not null;

drop table dsc_bigdata.netwan2_sum_alarm;

create table netwan2_sum_alarm as select hostname,desc1,severity,lok,max(time_eopch2)time_epoch2,max(day)day,max(jam)jam,count(*)jml from netwan2_alarm where hostname!='' group by hostname,desc1,severity,lok;" > ${LOG_HIVE}


# sqoop
sqoop-export --driver com.mysql.jdbc.Driver --connect jdbc:mysql://odb1.dev.udata.id/netwan_v2 --direct --table netwan2_sum_alarm --username hamim --password hamim --batch --export-dir /user/hive/warehouse/dsc_bigdata.db/netwan2_sum_alarm --fields-terminated-by '\0x01'  --lines-terminated-by '\n' --input-null-string "\\\\N" --input-null-non-string "\\\\N" --outdir ${HOME_DIR}/out > ${LOG_SQOOP} 2>&1

mysql -h odb1.dev.udata.id -u xxxx -pxxxx -e "use netwan_v2;truncate netwan2_agg_alarm; INSERT INTO netwan2_agg_alarm SELECT hostname,desc1,severity,lok,MAX(time_epoch2)time_epoch2,MAX(DAY)DAY,MAX(jam)jam,SUM(jml)jml FROM netwan2_sum_alarm GROUP BY hostname,desc1,severity,lok;"

# TELEGRAM NOTIFICATION

TITLE='Netwan V2 Daily'
RESULT=`grep -i 'INFO mapreduce.ExportJobBase: Exported' $LOG_SQOOP`
ERROR=`grep -i 'ERROR ' $LOG_SQOOP`
ERROR2=`grep -i 'Error:' $LOG_HIVE`

echo "${RESULT}"\\n"${ERROR}" >> ${HOME_DIR}/log_sqoop.log

if [[ -z $RESULT ]] # empty
then
  MSG="${TITLE}\nERROR. Partition not created. Please check log file."
else
  MSG=${TITLE}\\n"${RESULT}"\\n"${ERROR}"\\n"${ERROR2}"
fi

RDEV=xxxx@r.dev.udata.id
echo -e $MSG | ssh $RDEV "cat > tg1.txt"
ssh $RDEV "telegram-cli -W -e 'send_text NotifNetwan tg1.txt'; sleep 2; rm -f tg1.txt"

exit 0


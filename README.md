# ClouderaFlinkSQLForPartners
ClouderaFlinkSQLForPartners / CSA 1.2


# Queries

# Max, Average, Min Temperature per Location

select CAST(`location` as STRING) `location`, max(temp_f) as max_temp_f, avg(temp_f) as avg_temp_f, min(temp_f) as min_temp_f from weather group by location;

# Max/Min/Avg/Count per NJ
select CAST(`location` as STRING) `location`, max(temp_f) as max_temp_f, avg(temp_f) as avg_temp_f, min(temp_f) as min_temp_f,
       COUNT(temp_f) as numOfRecords
from weather 
WHERE `location` is not null and `location` <> 'null' and trim(`location`) <> '' and `location` like '%NJ'
group by location;

# Max/Min/Avg/Count NJ and NY

select CAST(`location` as STRING) `location`, max(temp_f) as max_temp_f, floor(avg(temp_f)) as avg_temp_f, min(temp_f) as min_temp_f,
       COUNT(temp_f) as numOfRecords
from weather 
WHERE `location` is not null and `location` <> 'null' and trim(`location`) <> '' and (`location` like '%NJ' or `location` like '%NY')
group by location
having avg(temp_f) < 50;

# Against Kudu

select * from kudu.default_database.`impala::default.envirosensors`;
 
# 
select symbol, 
       CAST(from_unixtime(floor(ts/1000)) AS TIMESTAMP(3)) as event_time,
       AVG(CAST(`high` as DOUBLE)) as avgHigh
from stocks
WHERE symbol is not null
GROUP BY symbol, ts

          
# Stock Events
 
CREATE TABLE stockEvents (
symbol    STRING,
uuid STRING,
`ts`    BIGINT,
`dt`    BIGINT,
`datetime`    STRING,
`open`    STRING,
`close`    STRING,
`high`    STRING,
`volume`    STRING,
`low`    STRING,
event_time AS CAST(from_unixtime(floor(`ts`/1000)) AS TIMESTAMP(3)),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
'connector.type'      = 'kafka',
'connector.version'   = 'universal',
'connector.topic'     = 'stocks',
'connector.startup-mode' = 'earliest-offset',
'connector.properties.bootstrap.servers' = 'edge2ai-1.dim.local:9092',
'format.type' = 'registry',
'format.registry.properties.schema.registry.url' = 'http://ec2-18-233-171-141.compute-1.amazonaws.com:7788/api/v1'
);
 

# Stock Tumbling Window

select symbol,
TUMBLE_START(event_time, INTERVAL '1' MINUTE) as tumbleStart,
TUMBLE_END(event_time, INTERVAL '1' MINUTE) as tumbleEnd,
AVG(CAST(`high` as DOUBLE)) as avgHigh
FROM stockEvents
WHERE symbol is not null
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), symbol;  


SELECT * FROM (
  SELECT * ,
  ROW_NUMBER() OVER (
    PARTITION BY window_start
    ORDER BY num_stocks desc
  ) AS rownum
  FROM (
    SELECT TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start, symbol, COUNT(*) AS num_stocks
    FROM stockEvents
    GROUP BY symbol, TUMBLE(event_time, INTERVAL '10' MINUTE)
  )
)
WHERE rownum <=3;


# References

* https://docs.cloudera.com/csa/1.2.0/flink-sql-table-api/topics/csa-kafka-registry-avro.html

* https://docs.cloudera.com/csa/1.2.0/release-notes/topics/csa-supported-sql.html

* https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/queries.html


#####

# In progress

SELECT * FROM (
  SELECT * ,
  ROW_NUMBER() OVER (
    PARTITION BY window_start
    ORDER BY num_transactions desc
  ) AS rownum
  FROM (
    SELECT TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start, itemId, COUNT(*) AS num_transactions
    FROM ItemTransactions
    GROUP BY itemId, TUMBLE(event_time, INTERVAL '10' MINUTE)
  )
)
WHERE rownum <=3;

select fromTimestamp(`datetime`)
from stocks

select symbol, CURRENT_TIME, `high`, TIMESTAMP `datetime` as ts
from stocks;

select symbol,
TUMBLE_START(CURRENT_TIME, INTERVAL '1' MINUTE) as tumbleStart,
TUMBLE_END(CURRENT_TIME, INTERVAL '1' MINUTE) as tumbleEnd,
AVG(CAST(`high` as DOUBLE)) as avgHigh
FROM stocks
WHERE symbol is not null
GROUP BY TUMBLE(CURRENT_TIME, INTERVAL '1' MINUTE), symbol;

CREATE TABLE pos (
   tstx BIGINT,
   idtx BIGINT,
   idstore INT,
   idproduct INT,
   quantity INT,
   timetx AS CAST(from_unixtime(floor(tstx/1000)) AS TIMESTAMP(3)),
   WATERMARK FOR timetx AS timetx - INTERVAL '10' SECOND
) WITH (
   'connector.type' = 'kafka',
   'connector.version' = 'universal',
   'connector.topic' = 'pos',
   'connector.startup-mode' = 'latest-offset',
   'connector.properties.bootstrap.servers' = 'kafka-url:9092',
   'connector.properties.group.id' = 'FlinkSQLPOS',
   'format.type' = 'json'
);

CREATE TABLE ItemTransactions (
transactionId    BIGINT,
`timestamp`    BIGINT,
itemId    STRING,
quantity INT,
event_time AS CAST(from_unixtime(floor(`timestamp`/1000)) AS TIMESTAMP(3)),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
'connector.type'      = 'kafka',
'connector.version'   = 'universal',
'connector.topic'     = 'transaction.log.1',
'connector.startup-mode' = 'earliest-offset',
'connector.properties.bootstrap.servers' = '<broker_address>',
'format.type' = 'json'
);

SELECT queryId, q.event_time as query_time, t.itemId, sum(t.quantity) AS recent_transactions
FROM ItemTransactions AS t, Queries AS q
WHERE t.itemId = q.itemId AND 
t.event_time BETWEEN q.event_time - INTERVAL '1' MINUTE 
AND q.event_time
GROUP BY t.itemId, q.event_time, q.queryId;

SELECT
  HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)) as hour_of_day,
  COUNT(*) as buy_cnt
FROM
  user_behavior
WHERE
  behavior = 'buy'
GROUP BY
  TUMBLE(ts, INTERVAL '1' HOUR)
  
  

SELECT *
FROM registry.default_database.scada as s
JOIN kudu.default_database.`impala::default.envirosensors`
FOR SYSTEM_TIME AS OF s.systemtime AS e
ON s.uuid = e.uuid


SELECT s.uuid, s.systemtime, s.temperaturef, e.uuid, e.adjtempf
FROM registry.default_database.scada s
NATURAL JOIN kudu.default_database.`impala::default.envirosensors` e


SELECT s.uuid, s.systemtime, s.temperaturef, e.uuid, e.adjtempf
FROM registry.default_database.scada s
NATURAL JOIN kudu.default_database.`impala::default.envirosensors` e
where s.uuid = e.uuid


SELECT 	scada.uuid, scada.systemtime, scada.temperaturef, scada.pressure, scada.humidity,scada.lux,scada.proximity, scada.oxidising, scada.reducing, scada.nh3,scada.gasko,scada.amplitude100, 
  scada.amplitude500, scada.amplitude1000, scada.lownoise, scada.midnoise, scada.highnoise, scada.amps, scada.cpu, scada.memory, scada.ipaddress, scada.host, scada.host_name, scada.macaddress, scada.endtime, scada.runtime, scada.starttime, scada.cpu_temp, 
  scada.diskusage, scada.id, scada.temperature,scada.adjtemp, scada.adjtempf, energy.`current`, energy.voltage ,energy.`power`,energy.`total`,energy.fanstatus,
  energy.swver, energy.hwver, energy.deviceId, energy.hwId, energy.fwId, energy.oemId, energy.`alias`, energy.devname, energy.iconhash, energy.`feature`, energy.activemode, energy.relaystate, energy.updating, energy.rssi, energy.ledoff, energy.latitude, energy.longitude, 
  energy.ontime, energy.`day`, energy.`index`, energy.`zonestr`, energy.tzstr, energy.dstoffset, energy.host, energy.currentconsumption, energy.devicetime, energy.ledon, energy.`end`, energy.`te`, energy.cpu 
FROM energy FULL JOIN scada ON energy.systemtime = scada.systemtime

create view stockEventsTumbling
as select symbol,
TUMBLE_START(event_time, INTERVAL '1' MINUTE) as tumbleStart,
TUMBLE_END(event_time, INTERVAL '1' MINUTE) as tumbleEnd,
AVG(CAST(`high` as DOUBLE)) as avgHigh
FROM stockEvents
WHERE symbol is not null
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), symbol;  


#

# notes

* https://docs.cloudera.com/csa/1.2.0/flink-sql-table-api/topics/csa-create-statements.html

* http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Using-logicalType-in-the-Avro-table-format-td34803.html

* https://towardsdatascience.com/event-driven-supply-chain-for-crisis-with-flinksql-be80cb3ad4f9

* https://community.cloudera.com/t5/Support-Questions/NiFi-processor-Convert-string-datetime-format-to-long-unix/td-p/226940

* https://github.com/cloudera/flink-tutorials/tree/master/flink-sql-tutorial

* https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/queries.html

* https://github.com/simonellistonball/flink-precisely-demo

* https://github.com/BrooksIan/Flink2Kafka

* https://github.com/cloudera/flink-tutorials/tree/master/flink-sql-tutorial

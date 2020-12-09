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

select CAST(`location` as STRING) `location`, max(temp_f) as max_temp_f, avg(temp_f) as avg_temp_f, min(temp_f) as min_temp_f,
       COUNT(temp_f) as numOfRecords
from weather 
WHERE `location` is not null and `location` <> 'null' and trim(`location`) <> '' and (`location` like '%NJ' or `location` like '%NY')
group by location
having avg(temp_f) < 50;


= "INSERT INTO sinkTopic "
                + "SELECT sensor, "
                + "TUMBLE_START(ts, INTERVAL '1' MINUTE) as tumbleStart, "
                + "TUMBLE_END(ts, INTERVAL '1' MINUTE) as tumbleEnd, "
                + "AVG(temp) AS avgTemp "
                + "FROM sourceTopic "
                + "WHERE sensor IS NOT null "
                + "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), sensor";
                
# References

https://docs.cloudera.com/csa/1.2.0/release-notes/topics/csa-supported-sql.html

https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/queries.html


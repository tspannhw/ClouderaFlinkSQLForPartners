# ClouderaFlinkSQLForPartners
ClouderaFlinkSQLForPartners / CSA 1.2


# Queries

# Max, Average, Min Temperature per Location

select location, max(temp_f) as max_temp_f, avg(temp_f) as avg_temp_f, min(temp_f) as min_temp_f from weather group by location;



# References

https://docs.cloudera.com/csa/1.2.0/release-notes/topics/csa-supported-sql.html


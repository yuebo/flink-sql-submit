DROP TABLE IF EXISTS flights;
CREATE TABLE flights (
    FlightNum  STRING,
    DestCountry STRING
) WITH (
  'connector' = 'elasticsearch-source',
  'url' = 'http://localhost:9200',
  'query' = '{"match_all":{}}',
  'format'='json',
  'index'='kibana_sample_data_flights',
  'mode'='searchAfter'
);


CREATE INLINE CLASS test.Udf3 AS
package test
public class Udf3 extends org.apache.flink.table.functions.ScalarFunction{
    def String eval(String test){
        return "<"+test+">"
    }
}
END;
DROP FUNCTION IF EXISTS test;
CREATE FUNCTION test AS 'test.Udf3';


DROP TABLE IF EXISTS flights_print;
CREATE TABLE flights_print (
    FlightNum  STRING,
    DestCountry STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO flights_print(FlightNum,DestCountry)
SELECT FlightNum,test(DestCountry) FROM flights;

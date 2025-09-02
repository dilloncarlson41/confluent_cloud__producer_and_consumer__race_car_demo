-- create an alert thresholds table/topic
CREATE TABLE driver_in_loop_alert_thresholds (
  alert_name STRING NOT NULL PRIMARY KEY NOT ENFORCED,
  max_value INT,
  min_value INT
) DISTRIBUTED BY HASH(alert_name);


-- insert threshold values per metric
INSERT INTO driver_in_loop_alert_thresholds
VALUES('speed_mph', 150, 100),
      ('tire_pressure', 60, 35);   


-- create an alert table/topic
CREATE TABLE driver_in_loop_alerts (
  race_car_id STRING NOT NULL PRIMARY KEY NOT ENFORCED, 
  alert_desc STRING,
  alert_value INT,
  alert_timestamp TIMESTAMP
) DISTRIBUTED BY HASH(race_car_id);


-- query to stream alerts to an alert topic when the tire pressure is too low
INSERT INTO driver_in_loop_alerts(
  race_car_id,
  alert_desc,
  alert_value
  )
SELECT dilt.race_car_id as race_car_id
    ,'tire_pressure_too_low' as alert_desc
    , dilt.tire_pressure as alert_value 
FROM driver_in_loop_telemetry dilt
LEFT JOIN driver_in_loop_alert_thresholds dilat
    ON dilat.alert_name = 'tire_pressure'
WHERE dilt.tire_pressure < dilat.min_value;


-- query to stream alerts to an alert topic when the driver is going really fast
INSERT INTO driver_in_loop_alerts(
  race_car_id,
  alert_desc,
  alert_value
  )
SELECT dilt.race_car_id as race_car_id
    ,'driver_is_going_really_fast' as alert_desc
    , dilt.speed_mph as alert_value 
FROM driver_in_loop_telemetry dilt
LEFT JOIN driver_in_loop_alert_thresholds dilat
    ON dilat.alert_name = 'speed_mph'
WHERE dilt.speed_mph > dilat.max_value;
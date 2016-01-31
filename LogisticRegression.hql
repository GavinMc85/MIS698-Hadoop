--create table for full train.csv file

CREATE EXTERNAL TABLE IF NOT EXISTS train (srch_id int, 
	date_time String, 
	site_id int, 
	visitor_location_country_id int, 
	visitor_hist_starrating float, 
	visitor_hist_adr_usd float,
	prop_country_id int, 
	prop_id int, 
	prop_starrating int, 
	prop_review_score float, 
	prop_brand_bool int, 
	prop_location_score1 float, 
	prop_location_score2 float, 
	prop_log_historical_price float, 
	position int, 
	price_usd float, 
	promotional_flag int, 
	srch_destination_id int, 
	srch_length_of_stay int, 
	srch_booking_window int, 
	srch_adult_count int, 
	srch_children_count int, 
	srch_room_count int, 
	srch_saturday_night_bool int, 
	srch_query_affinity_score float, 
	orig_destination_distance float, 
	random_bool int, 
	comp1_rate int, 
	comp1_inv int, 
	comp1_rate_percent_diff float, 
	comp2_rate int, 
	comp2_inv int, 
	comp2_rate_percent_diff float, 
	comp3_rate int, 
	comp3_inv int, 
	comp3_rate_percent_diff float, 
	comp4_rate int, 
	comp4_inv int, 
	comp4_rate_percent_diff float, 
	comp5_rate int, 
	comp5_inv int, 
	comp5_rate_percent_diff float, 
	comp6_rate int, 
	comp6_inv int, 
	comp6_rate_percent_diff float, 
	comp7_rate int, 
	comp7_inv int, 
	comp7_rate_percent_diff float, 
	comp8_rate int, 
	comp8_inv int, 
	comp8_rate_percent_diff float, 
	click_bool int,
	gross_booking_usd float, 
	booking_bool int) row format delimited fields terminated by ',' stored as textfile
	Location '/user/cloudera/MIS698';

--import the csv file into the table

LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/train.csv' OVERWRITE INTO TABLE train;

--copy into hdfs

INSERT OVERWIRTE DIRECTORY '/user/cloudera' SELECT * from trainfeatures;


--find correlations

SELECT 
corr(srch_id, booking_bool) AS srch_id_corr,
corr(site_id, booking_bool) AS site_id_corr,
corr(visitor_location_country_id, booking_bool) AS location_id_corr,
corr(visitor_hist_starrating, booking_bool) AS starrating_corr,
corr(visitor_hist_adr_usd, booking_bool) AS visitor_hist_adr_usd_corr,
corr(prop_country_id, booking_bool) AS prop_country_id_corr,
corr(prop_id, booking_bool) AS prop_id_corr,
corr(prop_starrating, booking_bool) AS prop_starrating_corr,
corr(prop_review_score, booking_bool) AS prop_review_score_corr,
corr(prop_brand_bool, booking_bool) AS brand_bool_corr,
corr(prop_location_score1, booking_bool) AS location_score1_corr,
corr(prop_location_score2, booking_bool) AS prop_location_score2_corr,
corr(prop_log_historical_price, booking_bool) AS log_historical_price_corr,
corr(price_usd, booking_bool) AS price_usd_corr,
corr(promotional_flag, booking_bool) AS promotion_flag_corr,
corr(srch_destination_id, booking_bool) AS destination_id_corr,
corr(srch_length_of_stay, booking_bool) AS length_of_stay_corr,
corr(srch_booking_window, booking_bool) AS booking_window_corr,
corr(srch_adult_count, booking_bool) AS adult_count_corr,
corr(srch_children_count, booking_bool) AS children_count_corr,
corr(srch_room_count, booking_bool) AS room_count_corr,
corr(srch_saturday_night_bool, booking_bool) AS saturday_night_corr,
corr(srch_query_affinity_score, booking_bool) AS affinity_score_corr,
corr(orig_destination_distance, booking_bool) AS dest_distance_corr,
corr(comp1_rate, booking_bool) AS comp1_rate_corr,
corr(comp1_inv, booking_bool) AS comp1_inv_corr,
corr(comp1_rate_percent_diff, booking_bool) AS comp1_rate_diff_corr,
corr(comp2_rate, booking_bool) AS comp2_rate_corr,
corr(comp2_inv, booking_bool) AS comp2_inv_corr,
corr(comp2_rate_percent_diff, booking_bool) AS comp2_rate_diff_corr,
corr(comp3_rate, booking_bool) AS comp3_rate_corr,
corr(comp3_inv, booking_bool) AS comp3_inv_corr,
corr(comp3_rate_percent_diff, booking_bool) AS comp3_rate_diff_corr,
corr(comp4_rate, booking_bool) AS comp4_rate_corr,
corr(comp4_inv, booking_bool) AS comp4_inv_corr,
corr(comp4_rate_percent_diff, booking_bool) AS comp4_rate_diff_corr,
corr(comp5_rate, booking_bool) AS comp5_rate_corr,
corr(comp5_inv, booking_bool) AS comp5_inv_corr,
corr(comp5_rate_percent_diff, booking_bool) AS comp5_rate_diff_corr,
corr(comp6_rate, booking_bool) AS comp6_rate_corr,
corr(comp6_inv, booking_bool) AS comp6_inv_corr,
corr(comp6_rate_percent_diff, booking_bool) AS comp6_rate_diff_corr,
corr(comp7_rate, booking_bool) AS comp7_rate_corr,
corr(comp7_inv, booking_bool) AS comp7_inv_corr,
corr(comp7_rate_percent_diff, booking_bool) AS comp7_rate_diff_corr,
corr(comp8_rate, booking_bool) AS comp8_rate_corr,
corr(comp8_inv, booking_bool) AS comp8_inv_corr,
corr(comp8_rate_percent_diff, booking_bool) AS comp8_rate_diff_corr
FROM Train
WHERE random_bool = 0;

--after examining correlations, trim down features; create view with only selected columns

CREATE VIEW trainfeatures AS
SELECT 
booking_bool,
srch_booking_window,
srch_children_count,
srch_length_of_stay,
comp3_inv,
srch_room_count,
comp1_rate,
comp2_rate,
comp3_rate,
comp4_rate,
comp5_rate,
comp6_rate,
comp7_rate,
comp8_rate,
prop_starrating,
prop_review_score,
srch_query_affinity_score,
promotional_flag,
prop_location_score2
FROM train
WHERE random_bool=0;





--need to convert to compressed row storage format. We'll need a python script for that...
--export smaller dataset to csv file to convert

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/staging' SELECT * FROM trainfeatures;

--need to concatenate files using bash: $ cat /home/cloudera/staging/* > trainfeatures.csv
--run python script csv2libsvm.py; run using: 
-- $ python /home/cloudera/staging/trainfeatures.csv /home/cloudera/staging/trainlibsvm.csv 1 1
-- still more preparation! place conv.awk in folder with trainlibsvm.csv and run the following from bash:
-- $ awk -f conv.awk trainlibsvm.csv | sed -e "s/+1/1/" | sed -e "s/-1/0/" > traincrf.csv

CREATE EXTERNAL TABLE expedia (
  rowid int,
  label float,
  features ARRAY<STRING> )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ",";
  
  
LOAD DATA LOCAL INPATH '/home/cloudera/staging/traincrf.csv' OVERWRITE INTO TABLE expedia;

--check number of rows
SELECT count(rowid) FROM expedia;

--6977877

-- need to initialize hivemall; jar file and hive file located in /tmp

add jar /tmp/hivemall-with-dependencies.jar;
source /tmp/define-all.hive;

set hivevar:rand_seed=31;

CREATE TABLE expedia_shuffled
AS
SELECT rand(${rand_seed}) as rnd, * from expedia;

--80% for training

CREATE TABLE expediatrain as
SELECT * from expedia_shuffled
ORDER BY rnd DESC
limit 5582301;

--20% for testing
CREATE TABLE expediatest as
SELECT * from expedia_shuffled
ORDER BY rnd ASC
limit 1395576;

--explode the datasets

CREATE TABLE expediatrain_exploded as
SELECT
rowid,
label,
cast(split(feature,":")[0] as int) feature,
cast(split(feature,":")[1] as float) as value
FROM expediatrain LATERAL VIEW explode(addBias(features)) t AS feature;

CREATE TABLE expediatest_exploded AS
SELECT
rowid,
label,
cast(split(feature,":")[0] as int) as feature,
cast(split(feature,":")[1] as float) as value
FROM expediatest LATERAL VIEW explode(addBias(features)) t as feature;


--train our model

select count(1) from expediatest;
set hivevar:num_test_instances=1395576;

CREATE TABLE expedia_model1
as
SELECT
 cast(feature as int) as feature,
 avg(weight) as weight
FROM
 (SELECT
	logress(addBias(features),label) as (feature,weight)
  FROM
	expediatrain
 ) t
group by feature;

--prediction

CREATE or REPLACE VIEW expedia_predict1
as
SELECT 
 t.rowid,
 sigmoid(sum(m.weight * t.value)) as prob,
 CAST((case when sigmoid(sum(m.weight * t.value)) >= 0.5 then 1.0 else 0.0 end) as FLOAT) as label
FROM
 expediatest_exploded t LEFT OUTER JOIN
 expedia_model1 m ON (t.feature = m.feature)
GROUP BY
 t.rowid;

--evaluation

CREATE OR REPLACE VIEW expedia_submit1 as
SELECT
 t.label as actual,
 pd.label as predicted,
 pd.prob as probability
FROM expediatest t JOIN expedia_predict1 pd
 ON (t.rowid = pd.rowid)

SELECT count(1) / ${num_test_instances} from expedia_submit1
where actual == predicted;

-- 96% accuracy

	

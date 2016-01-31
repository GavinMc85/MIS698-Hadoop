--initiate hivemall

add jar /tmp/hivemall-with-dependencies.jar;
source /tmp/define-all.hive;

--create dataset

CREATE TABLE train_subset AS
SELECT
 srch_id,
 prop_id,
 CAST((CASE WHEN prop_location_score2 is NULL then "0.13083132" else prop_location_score2 END) as float) as prop_location_score2,
 CAST((CASE WHEN prop_review_score is NULL then "3.8" else prop_review_score END) as float) as prop_review_score,
 promotional_flag,
 prop_starrating
FROM train
WHERE prop_id IS NOT NULL;

--set variables for Z-score

set hivevar:mu_prop_loc=0.13083132;
set hivevar:std_prop_loc= 0.1384;
set hivevar:mu_prop_review=3.8;
set hivevar:std_prop_review= 1;
set hivevar:mu_promo_flag= 0.2222;
set hivevar:std_promo_flag= 0.4157379;
set hivevar:mu_prop_starrating= 3.186;
set hivevar:std_prop_starrating= 1.045;

--create table with z score ratings

CREATE TABLE train_z AS SELECT srch_id, prop_id, zscore(prop_location_score2, ${mu_prop_loc}, ${std_prop_loc}) as prop_location_score2, zscore(prop_review_score, ${mu_prop_review}, ${std_prop_review}) as prop_review_score, zscore(promotional_flag, ${mu_promo_flag}, ${std_promo_flag}) as promotional_flag, zscore(prop_starrating, ${mu_prop_starrating}, ${std_prop_starrating}) as prop_starrating FROM train_subset;

--create combined rating

SET hivevar:seed = 31;
CREATE TABLE expedia_shuffled AS
SELECT
 rand(${seed}) as rnd,
 srch_id,
 prop_id,
 ((4*prop_location_score2)+(2*promotional_flag)+prop_review_score+prop_starrating) as rating
FROM train_z;

--gives rating score between -11.7 and 31.3
--80% for training

CREATE TABLE exptraining
as
SELECT * from expedia_shuffled
order by rnd DESC limit 1857875;

--20% for testing

CREATE TABLE exptesting
as
SELECT * from expedia_shuffled
order by rnd ASC limit 465668;

--find average score

SELECT avg(rating) from exptraining;

--set variables for training algorithm

set hivevar:mu=-0.0692;
set hivevar:factor=10;
set hivevar:iters=50;

--training

create table rec_model
as
SELECT
 idx,
 array_avg(u_rank) as Pu,
 array_avg(m_rank) as Qi,
 avg(u_bias) as Bu,
 avg(m_bias) as Bi
FROM (
 SELECT
   train_mf_sgd(srch_id, prop_id, rating, "-factor ${factor} -mu ${mu} -iter ${iters}") as (idx, u_rank, m_rank, u_bias, m_bias)
 FROM
  exptraining) t
GROUP BY idx;


--Predict
select
  t2.actual,
  mf_predict(t2.Pu, p2.Qi, t2.Bu, p2.Bi, ${mu}) as predicted
from (
  select
    t1.srch_id, 
    t1.prop_id,
    t1.rating as actual,
    p1.Pu,
    p1.Bu
  from
    exptesting t1 LEFT OUTER JOIN rec_model p1
    ON (t1.srch_id = p1.idx) 
) t2 
LEFT OUTER JOIN rec_model p2
ON (t2.prop_id = p2.idx);

--evaluate using MAE and RMSE

select
  mae(predicted, actual) as mae,
  rmse(predicted, actual) as rmse
from (
  select
    t2.actual,
    mf_predict(t2.Pu, p2.Qi, t2.Bu, p2.Bi, ${mu}) as predicted
  from (
    select
      t1.srch_id, 
      t1.prop_id,
      t1.rating as actual,
      p1.Pu,
      p1.Bu
    from
      exptesting t1 LEFT OUTER JOIN rec_model p1
      ON (t1.srch_id = p1.idx) 
  ) t2 
  LEFT OUTER JOIN rec_model p2
  ON (t2.prop_id = p2.idx)
) t;

--hotel recommendation; selects top k hotels user has not seen

set hivevar:srch_id=1;
set hivevar:topk=5;

select
  t1.prop_id, 
  mf_predict(t2.Pu, t1.Qi, t2.Bu, t1.Bi, ${mu}) as predicted
from (
  select
    idx prop_id,
    Qi, 
    Bi
  from
    rec_model p
  where
    p.idx NOT IN 
      (select prop_id from exptraining where srch_id=${srch_id})
) t1 CROSS JOIN (
  select
    Pu,
    Bu
  from 
    rec_model
  where
    idx = ${srch_id}
) t2
order by
  predicted DESC
limit ${topk};



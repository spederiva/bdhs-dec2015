raw_data = LOAD '/user/eyalb/ml-latest-small/ratings.csv' USING PigStorage(',')
    AS (userId:int, movieId:int, rating:double, ts:long);

filtered_data = FILTER raw_data BY userId > 0 AND movieId > 0;

STORE filtered_data INTO '/user/eyalb/ratings-fixed';

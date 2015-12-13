CRAETE TABLE ratings (
	userId INT,
	movieId INT,
	rating DOUBLE,
	ts BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/eyalb/ratings-fixed';
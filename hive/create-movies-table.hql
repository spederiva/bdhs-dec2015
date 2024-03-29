CREATE EXTERNAL TABLE IF NOT EXISTS movies (
  movieId INT,
  title STRING,
  year INT,
  genres ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/eyalb/movies-fixed';

SELECT iq.movieId, iq.title, iq.genre, iq.avg_rating, iq.cnt_rating
FROM (
	SELECT agenres.gg AS genre, MAX(results.avg_rating) AS avg_rating
	FROM movies AS md
	LEFT OUTER JOIN 
	(
		SELECT DISTINCT groups.g AS gg FROM (
			SELECT EXPLODE(genres) AS g FROM movies
		) AS groups
	) AS agenres
	JOIN (
		SELECT movieId, AVG(rating) AS avg_rating, COUNT(rating) AS cnt_rating
		FROM ratings
		GROUP BY movieId
	) AS results
	ON results.movieId = md.movieId
	WHERE ARRAY_CONTAINS(md.genres, agenres.gg)
	AND results.cnt_rating >= 20
	GROUP BY agenres.gg
) AS max_query
INNER JOIN (
	SELECT md.movieId, md.title, agenres.gg AS genre, results.avg_rating, results.cnt_rating
	FROM movies AS md
	LEFT OUTER JOIN 
	(
		SELECT DISTINCT groups.g AS gg FROM (
			SELECT EXPLODE(genres) AS g FROM movies
		) AS groups
	) AS agenres
	JOIN (
		SELECT movieId, AVG(rating) AS avg_rating, COUNT(rating) AS cnt_rating
		FROM ratings
		GROUP BY movieId
	) AS results
	ON results.movieId = md.movieId
	WHERE ARRAY_CONTAINS(md.genres, agenres.gg)
	AND results.cnt_rating >= 20
) AS iq
ON iq.avg_rating = max_query.avg_rating AND iq.genre = max_query.genre;


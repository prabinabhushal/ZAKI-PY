SELECT * FROM music.music_track;

-- Task 1: Genre Popularity Aggregation
-- Objective: Rank tracks by play count within genres and aggregate popular tracks.

-- Task Description: Without Function

-- Calculate a popularity metric: play_count ÷ 100,000, rounded to 2 decimals. Use 0 for null play_count.
-- Categorize tracks: ‘Hit’ if metric ≥ 15, ‘Known’ if ≥ 10, else ‘Underrated’.
-- Use a window function to rank tracks by play_count within each genre (highest = rank 1).
-- Use STRING_AGG to list track names (alphabetically) with rating ≥ 4.5 per genre, comma-separated, or ‘None’ if none.
-- Show: track name, genre, popularity metric, category, genre rank, high-rated tracks.
-- Order by genre, genre rank.
-- Expected Output:


-- track_name	    genre	  popularity_metric  	track_category	genre_rank	      high_rated_tracks
-- Blinding Lights	Pop	      25.00	                  Hit	            1	        Billie Jean, Blinding Lights
-- ...	...	...	...	...	...


WITH cte_popularity AS(
	SELECT
	track_name , genre , rating , play_count,
	COALESCE(ROUND(play_count / 100000, 2) , 0) AS popularity_metric
	FROM music.music_track
),  
cte_category AS(
	SELECT track_name , genre , rating , popularity_metric,
	 CASE 
        WHEN popularity_metric >= 15 THEN 'Hit'
		WHEN popularity_metric >= 10 THEN 'Known'
        ELSE 'Underrated'
    END AS track_category,
	DENSE_RANK() OVER (PARTITION BY genre ORDER BY play_count DESC) as genre_rank
	FROM cte_popularity
),
cte_ranking AS(
	SELECT genre,
	COALESCE(STRING_AGG(track_name, ' , ') FILTER (WHERE rating >= 4.5) , 'None') AS high_rated_tracks
	FROM cte_category
	GROUP BY genre
)
SELECT c.track_name , c.genre , c.popularity_metric, c.track_category, c.genre_rank, r.high_rated_tracks
FROM cte_category c
JOIN cte_ranking r ON c.genre = r.genre
ORDER BY genre, genre_rank;



-- 2. Task Description:

-- Calculate rating deviation: absolute difference between track rating and artist’s average rating, rounded to 2 decimals. Use 0 for null rating.
-- Categorize tracks: ‘Consistent’ if deviation ≤ 0.2, ‘Variable’ if ≤ 0.5, else ‘Diverse’.
-- Use a window function to count tracks per artist.
-- Use STRING_AGG to list track names (alphabetically) with play_count ≥ 1,000,000 per artist, comma-separated, or ‘None’ if none.
-- Show: track name, artist, rating deviation, consistency category, artist track count, high-play tracks.
-- Order by artist, rating deviation.
-- Expected Output:


-- track_name	   artist	rating_deviation	consistency_category	artist_track_count	high_play_tracks
-- Lose Yourself	Eminem	   0.10	                Consistent	              2	             Lose Yourself
-- ...	...	...	...	...	...

WITH cte_rating AS(
	SELECT track_id, track_name, play_count, genre , rating, artist,
	COALESCE(ROUND(ABS(rating - avg(rating) OVER (PARTITION BY artist)),2), 0) as rating_deviation
	FROM music.music_track
),
cte_category AS(
	SELECT track_name, play_count, genre , rating , rating_deviation , artist , 
	CASE 
        WHEN rating_deviation <= 0.2 THEN 'Consistent'
		WHEN rating_deviation <= 0.5 THEN 'Variable'
        ELSE 'Diverse'
    END AS consistency_category,
	count(track_id) OVER (PARTITION BY artist) as artist_track_count
	FROM cte_rating
),
cte_ranking AS(
	SELECT artist,
	COALESCE(STRING_AGG(track_name, ' , ') FILTER (WHERE play_count >= 1000000), 'None') AS high_play_tracks
	FROM cte_category
	GROUP BY artist
)
SELECT c.track_name , c.artist , c.rating_deviation, c.consistency_category, c.artist_track_count, r.high_play_tracks
FROM cte_category c
JOIN cte_ranking r ON c.artist = r.artist
ORDER BY artist, rating_deviation;




-- 3: Recent Track Performance
-- Objective: Rank recent tracks by rating and aggregate short tracks by genre.

-- Task Description:

-- For tracks released after 2000, calculate a performance score: rating + (play_count ÷ 500,000), rounded to 2 decimals. Use 0 for null rating or play_count.
-- Categorize tracks: ‘Standout’ if score ≥ 8, ‘Notable’ if ≥ 6, else ‘Average’.
-- Use a window function to rank tracks by performance score within genre (highest = rank 1).
-- Use STRING_AGG to list track names (alphabetically) with duration_seconds ≤ 250 per genre, comma-separated, or ‘None’ if none.
-- Show: track name, genre, performance score, performance category, genre rank, short tracks.
-- Order by performance score (descending).
-- Expected Output:


-- track_name	    genre	performance_score	performance_category	genre_rank	short_tracks
-- Blinding Lights	Pop	    9.40	            Standout	            1	        Blinding Lights, Shape of You
-- ...	...	...	...	...	...


WITH cte_score AS(
	SELECT track_name, genre, artist, rating, duration_seconds,
	ROUND(COALESCE(rating,0) + COALESCE((play_count / 500000),0) ,2) AS performance_score
	FROM music.music_track
	WHERE EXTRACT(YEAR FROM release_date) > 2000
),
cte_category AS(
	SELECT track_name, genre, artist, rating, performance_score,duration_seconds,
		CASE 
        WHEN performance_score >= 8 THEN 'Standout'
		WHEN performance_score >= 6 THEN 'Notable'
        ELSE 'Average'
    END AS performance_category,
	DENSE_RANK() OVER (PARTITION BY genre ORDER BY performance_score DESC) as genre_rank
	FROM cte_score
),
cte_short AS(
	SELECT genre,
	COALESCE(STRING_AGG(track_name, ' , ') FILTER (WHERE duration_seconds <= 250 ) ,'None') as short_tracks
	FROM cte_category
	GROUP BY genre
)
SELECT c.track_name , c.genre, c.performance_score, c.performance_category, c.genre_rank, s.short_tracks
FROM cte_category c
JOIN cte_short s ON c.genre = s.genre
ORDER BY performance_score DESC;


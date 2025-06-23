select *from series ;
select * from episode;

-- 1. Objective: Create a PostgreSQL function to compute an episode’s impact score, then use it to analyze series performance, highlighting 
-- top episodes and genre trends.

-- Task Description:

-- Use the series and episode tables.
-- Create a function calculate_impact_score with parameters:
-- rating : Episode rating.
-- air_date : Episode air date.
-- Return a NUMERIC score (rounded to 2 decimals): rating * 5 + months_since_air / 12, where months_since_air is months from air_date to current date.
-- Use COALESCE to return 0 if rating is null.

CREATE OR REPLACE FUNCTION series.calculate_impact_score(rating NUMERIC, air_date DATE)
RETURNS NUMERIC AS
$$
BEGIN
    RETURN ROUND(COALESCE(rating, 0)*5 + 
	    ((EXTRACT(YEAR FROM AGE(CURRENT_DATE, air_date)) * 12) + EXTRACT(MONTH FROM AGE(CURRENT_DATE, air_date))) / 12,2);
END;
$$
LANGUAGE plpgsql;


-- categorize episodes: ‘High Impact’ if score ≥ 45, ‘Moderate’ if ≥ 40, else ‘Low’.
-- string agg where rating >8.5 of episodes  as high_rated_episodes	
-- rank on the basis of impact score


-- series_name	episode_title	impact_score	episode_category	rank	high_rated_episodes	           avg_series_rating
-- Breaking Bad  	Pilot	      	46.33		   High Impact		1	     Pilot, Seven Thirty-Seven       	8.6

WITH cte_rated AS(
	SELECT e.series_id , 
	STRING_AGG(CASE WHEN rating > 8.5 THEN episode_title
	END,
	',') as high_rated_episodes	
	FROM episode e
	GROUP BY e.series_id
),
cte_impact AS(
	SELECT series.calculate_impact_score (rating, air_date) as impact_score, series_id, episode_title, rating
	FROM episode
)

SELECT series_name,episode_title,impact_score,
 (CASE 
	WHEN impact_score>= 45 THEN 'High Impact'
	WHEN impact_score>= 40 THEN 'Moderate'
	ELSE 'Low'
  END) as episode_category , 
 DENSE_RANK() OVER (ORDER BY impact_score DESC) AS rank,
 high_rated_episodes,
 ROUND(avg(rating) OVER (PARTITION BY i.series_id),2) as avg_series_rating
FROM series s
JOIN cte_rated r ON s.series_id = r.series_id
JOIN cte_impact i ON s.series_id = i.series_id;




-- 2.Create a PostgreSQL function to evaluate episode longevity, then use it to analyze series episode trends and genre popularity.

-- Task Description:
-- Use the series and episode tables.
-- Create a function calculate_longevity_score in the public schema with parameters:
-- rating : Episode rating.
-- season_number : Season number.
-- Return a NUMERIC score (rounded to 2 decimals): rating * 3 + season_number * 2. Use COALESCE to return 0 if rating is null.

CREATE OR REPLACE FUNCTION series.calculate_longevity_score(rating NUMERIC, season_number INT)
RETURNS NUMERIC
AS $$
	BEGIN
	RETURN ROUND(COALESCE(rating, 0)*3 + season_number * 2 , 2);
	END;
$$
LANGUAGE plpgsql;

-- In the main query:
-- Use the function to compute each episode’s longevity score.
-- Use CASE to label episodes: ‘Long-Lasting’ if score ≥ 30, ‘Moderate’ if ≥ 25, else ‘Short-Lived’.
-- Use a window function to count episodes per series.
-- Use STRING_AGG to list genres of series with episodes aired after 2015 (alphabetically), or ‘None’ if none.
-- Display: series name, episode title, longevity score, episode label, episode count, recent genres, total seasons per series.
-- Order by series name, longevity score (descending).

-- Expected Output:

-- series_name	episode_title	longevity_score	episode_label	episode_count	recent_genres	total_seasons
-- Breaking Bad	No Más	        31.50	        Long-Lasting	3		Action, Sci-Fi	3


WITH cte_long AS(
	SELECT series.calculate_longevity_score(rating,season_number) AS longevity_score,
	episode_title, 
	series_id,
	season_number,
	count(episode_number) OVER (PARTITION BY series_id) as episode_count
	FROM episode
),
cte_lasting AS(
	SELECT longevity_score, episode_title, series_id, episode_count,season_number,
	CASE
		WHEN longevity_score >= 30 THEN 'Long_Lasting'
		WHEN longevity_score >= 25 THEN 'Moderate'
		ELSE 'Short_Lived'
	END as episode_label
	FROM cte_long
),
cte_agg AS(
SELECT 
    s.series_id,
    STRING_AGG(
        DISTINCT CASE 
            WHEN EXTRACT(YEAR FROM e.air_date) > 2015 THEN s.genre 
			ELSE 'None'
        END, 
		','
    ) AS recent_genres
FROM series s
JOIN episode e ON s.series_id = e.series_id
GROUP BY s.series_id
)
SELECT series_name ,episode_title, longevity_score, episode_label, episode_count, recent_genres,
max(season_number) OVER (PARTITION BY l.series_id) as total_seasons
FROM series s
JOIN cte_lasting l ON s.series_id = l.series_id
JOIN cte_agg a ON s.series_id = a.series_id
ORDER BY series_name, longevity_score DESC;




-- 3.Create a PostgreSQL function to assess episode rating consistency, then use it to analyze series performance and episode trends.

-- Task Description:
-- Use the series and episode tables.
-- Create a function calculate_rating_consistency in the public schema with parameters:
-- rating : Episode rating.
-- series_avg_rating : Average rating of the series.
-- Return a NUMERIC absolute score : (rating - series_avg_rating) * 10. Use COALESCE to return 0 if rating or series_avg_rating is null.


CREATE OR REPLACE FUNCTION series.calculate_rating_consistency (rating NUMERIC, series_avg_rating NUMERIC)
RETURNS NUMERIC
AS $$
	BEGIN
	IF rating IS NULL OR series_avg_rating IS NULL THEN
    RETURN 0;
	END IF;
	RETURN ROUND(ABS((rating -series_avg_rating)* 10), 2);
	END;
$$
LANGUAGE plpgsql;


-- In the main query:
-- Use the function to compute each episode’s rating consistency score .
-- Use CASE to categorize episodes: ‘Consistent’ if score ≤ 5, ‘Variable’ if ≤ 15, else ‘Highly Variable’.
-- Use a window function to rank episodes by rating within each season (descending).
-- Use STRING_AGG to list episode titles (alphabetically) aired in the last 10 years (since June 1, 2015), or ‘None’ if none.
-- Display: series name, episode title, consistency score, consistency category, season rank, recent episodes, max series rating.
-- Order by series name, consistency score.

-- Expected Output:

-- series_name	episode_title	consistency_score	consistency_category	season_rank   	recent_episodes		max_series_rating
-- Breaking Bad	Seven 		Thirty-Seven	            	1.00			Consistent	        1		        	8.7



WITH cte_agg AS(
	SELECT AVG(rating) OVER (PARTITION BY series_id) as series_avg_rating,
	series_id,rating,episode_title
	FROM episode
),
cte_consist AS(
	SELECT series_id,
	rating,
	episode_title,
	series.calculate_rating_consistency(rating, series_avg_rating) AS consistency_score,
	DENSE_RANK() OVER (PARTITION BY series_id ORDER BY rating DESC) AS season_rank
	FROM cte_agg
),
cte_score AS(
	SELECT series_id,rating,episode_title,consistency_score,season_rank,
	CASE WHEN consistency_score <= 5 THEN 'Consistent'
		 WHEN consistency_score <= 15 THEN 'Variable'
		 ELSE 'Highly_Variable'
	END AS consistency_category
	FROM cte_consist
),
cte_recent AS(
	SELECT series_id,
	STRING_AGG(
        DISTINCT CASE 
            WHEN air_date > '2015-06-01' THEN episode_title
			ELSE 'None'
        END, 
		','
    ) AS recent_episodes
	FROM episode
	GROUP BY series_id
)
SELECT s.series_name,sc.episode_title,sc.consistency_score,sc.consistency_category,sc.season_rank,r.recent_episodes,
	   max(rating) OVER (PARTITION BY sc.series_id) as max_series_rating
FROM series s
JOIN cte_score sc ON s.series_id = sc.series_id
JOIN cte_recent r ON s.series_id = r.series_id
ORDER BY s.series_name, sc.consistency_score DESC;
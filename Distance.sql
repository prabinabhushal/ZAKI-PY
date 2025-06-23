CREATE INDEX pd ON pr_table(provider_group_id);
CREATE INDEX npd ON nr_table(provider_group_id);
CREATE INDEX pr_geom ON pr_table USING GIST (geom);

--1. Write a query to find providers in pr_table within 5 kilometers of a given point (e.g., latitude 40.7128, longitude -74.0060) 
-- and join with nr_table to include their negotiated_rate and billing_code. (00100).. Order by distance ascending.
WITH rate AS(
SELECT * 
FROM nr_table
WHERE billing_code = '00100'
)
SELECT *,
ST_Distance(ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography, geom) AS distance
FROM pr_table p 
JOIN rate n ON p.provider_group_id = n.provider_group_id
WHERE ST_DWithin(geom,ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,5000)
ORDER BY distance;


CREATE INDEX prv_tax ON pr_table USING GIN (taxonomy_codes);
CREATE INDEX tax_list ON taxonomy.billing_taxonomy USING GIN (taxonomy_list);

--2. Write a query to find provider with billing code(99214) within 20 miles of (-96.79698789999999,32.7766642) applying 
--specialized_filter.
WITH rate AS(
SELECT * 
FROM nr_table
WHERE billing_code = '99214'
)
SELECT *,
ST_Distance(ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography, geom) AS distance
FROM pr_table p 
JOIN rate n ON p.provider_group_id = n.provider_group_id
JOIN taxonomy.billing_taxonomy t on t.taxonomy_list && p.taxonomy_codes
WHERE ST_DWithin(ST_SetSRID(ST_MakePoint(-96.79698789999999, 32.7766642), 4326)::geography, geom,1609.34*20)
limit 15;


SELECT *,ST_Distance(ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography, p.geom) as distance
FROM pr_table p 
JOIN nr_table n ON p.provider_group_id = n.provider_group_id
JOIN taxonomy.billing_taxonomy t on t.taxonomy_list && p.taxonomy_codes
WHERE n.billing_code = '99214' AND 
ST_DWithin(ST_SetSRID(ST_MakePoint(-96.79698789999999, 32.7766642), 4326)::geography, p.geom,32186)
limit 15;



--3. Write a query to find providers in pr_table whose associated billing_codes in nr_table have a billing_description in billing_taxonomy containing 
-- a specific keyword (e.g., 'surgery'). Return provider_full_name, prv_city, billing_code, and billing_description.
WITH billing AS(
SELECT provider_full_name, prv_city,billing_code
FROM pr_table p 
JOIN nr_table n ON p.provider_group_id = n.provider_group_id
)
SELECT provider_full_name, prv_city, b.billing_code, billing_description
FROM billing b
JOIN taxonomy.billing_taxonomy t on b.billing_code = t.billing_code
WHERE t.billing_description ILIKE '%surgery%';



--4. Write a query to find providers in pr_table with a specific prv_type_code (1) whose maximum negotiated_rate in nr_table exceeds a threshold (200) 
-- for billing_code (99214). Apply specialized filter
EXPLAIN ANALYZE WITH rate AS(
SELECT provider_group_id
FROM nr_table
WHERE billing_code = '99214'
GROUP BY provider_group_id
HAVING max(negotiated_rate) > 200
)
SELECT *
FROM pr_table p 
JOIN rate r ON p.provider_group_id = r.provider_group_id
JOIN nr_table n ON p.provider_group_id = n.provider_group_id AND billing_code = '99214'
JOIN taxonomy.billing_taxonomy t on t.taxonomy_list && p.taxonomy_codes
WHERE prv_type_code = 1
LIMIT 15;


-- 5.Find provider records for billing_code '99214'  with specialized filter, and the service_code array contains at least one service_code .
-- Rank providers based on their highest negotiated_rate for each unique npi and tin combination, ensuring providers with the same npi and
-- tin receive the same rank. Return all columns from nr_table, plus provider_full_name, npi, tin, prv_city, negotiated rate , 
-- distance to the reference point (latitude 40.7128, longitude -74.0060) within 100 kilometers . Only include records with ranks between 1 and 10,

CREATE INDEX index_billing_code ON nr_table (billing_code);

#MODIFIED

WITH cte_pr AS (
    SELECT p.*
	FROM pr_table p
	JOIN taxonomy.billing_taxonomy t ON t.taxonomy_list && p.taxonomy_codes
	WHERE ST_DWithin(p.geom::geography, ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,100000)
),
cte_nr AS (
SELECT n.* 
FROM nr_table n
 WHERE n.billing_code = '99214'AND array_length(n.service_code, 1) > 0 
),
nrpr AS(
 SELECT provider_full_name, npi, tin, prv_city ,n.*,
 MAX(negotiated_rate) OVER (PARTITION BY npi, tin) AS max_rate
 FROM cte_nr n
 JOIN cte_pr p ON p.provider_group_id = n.provider_group_id
 ),
final_rank AS (
    SELECT nrpr.*,
	DENSE_RANK() OVER (ORDER BY max_rate DESC) AS max_rank
    FROM nrpr
)
SELECT * 
FROM final_rank
WHERE max_rank BETWEEN 1 AND 10;

#
WITH ranked_data AS (
    SELECT n.*, p.provider_full_name, p.npi, p.tin, p.prv_city,
    ST_Distance(ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,p.geom::geography)/1000 AS distance,
    MAX(n.negotiated_rate) OVER (PARTITION BY p.npi, p.tin) AS max_rate
    FROM nr_table n
    JOIN pr_table p ON p.provider_group_id = n.provider_group_id
    JOIN taxonomy.billing_taxonomy t ON t.taxonomy_list && p.taxonomy_codes
    WHERE n.billing_code = '99214'AND array_length(n.service_code, 1) > 0 AND 
	 ST_DWithin(p.geom::geography, ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,100000)
),
final_rank AS (
    SELECT *,DENSE_RANK() OVER (ORDER BY max_rate DESC) AS max_rank
    FROM ranked_data
)
SELECT * 
FROM final_rank
WHERE max_rank BETWEEN 1 AND 10
LIMIT 5;


-- 6. Find provider records for billing_code_description like MRI  with specialized filter and the billing_code_modifier array contains at least one 
-- modifier starting with '5' (e.g., '59'). Rank providers based on the average negotiated_rate for each unique npi and tin combination, ensuring providers 
-- with the same npi and tin receive the same rank. (latitude 40.7128, longitude -74.0060) within 50 miles. Only include records with ranks between 2 and 20,
--ordered by rank.


#MODIFIED 
WITH cte_pr AS (
    SELECT p.*,billing_description
	FROM pr_table p
	JOIN taxonomy.billing_taxonomy t ON t.taxonomy_list && p.taxonomy_codes
	WHERE t.billing_description ILIKE '%MRI%' AND 
	ST_DWithin(p.geom::geography, ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,1609.34*50)
),
cte_nr AS (
SELECT n.*
FROM nr_table n
 WHERE EXISTS (
            SELECT 1 FROM unnest(n.billing_code_modifier) AS mod WHERE mod LIKE '5%')
),
nrpr AS(
 SELECT provider_full_name, npi, tin, prv_city ,n.*,billing_description ,
 AVG(negotiated_rate) OVER (PARTITION BY npi, tin) AS avg_rate
 FROM cte_nr n
 JOIN cte_pr p ON p.provider_group_id = n.provider_group_id
 ),
final_rank AS (
    SELECT nrpr.*,
	DENSE_RANK() OVER (ORDER BY avg_rate DESC) AS avg_rank
    FROM nrpr
)
SELECT * 
FROM final_rank
WHERE avg_rank BETWEEN 2 AND 20
ORDER BY avg_rank;

#
WITH ranked_data AS (
    SELECT *,
	ST_Distance(ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,p.geom)as distance,
	AVG(n.negotiated_rate) OVER (PARTITION BY npi, tin) AS avg_rate
    FROM nr_table n
    JOIN pr_table p ON n.provider_group_id = p.provider_group_id
    JOIN taxonomy.billing_taxonomy t ON t.taxonomy_list && p.taxonomy_codes
    WHERE billing_description ILIKE '%MRI%'
    AND EXISTS (
            SELECT 1 FROM unnest(n.billing_code_modifier) AS mod WHERE mod LIKE '5%')
        AND ST_DWithin(geom,ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography,1609.34*50)),
		
ranked_2 as(
SELECT *, 
DENSE_RANK() OVER (ORDER BY avg_rate DESC) AS rank
FROM ranked_data )

SELECT * 
FROM ranked_2
WHERE rank BETWEEN 2 AND 20
ORDER BY rank;


































-- graf 1: Počet hodnotení podľa vekových skupín--
SELECT 
    u.age_group, 
    COUNT(f.fact_rating) AS num_ratings
FROM fact_ratings f
JOIN dim_users u ON f.user_id = u.user_id
GROUP BY u.age_group
ORDER BY num_ratings DESC;

-- graf 2: Počet hodnotení podľa pohlavia--
SELECT 
    u.gender, 
    COUNT(f.fact_rating) AS num_ratings
FROM fact_ratings f
JOIN dim_users u ON f.user_id = u.user_id
GROUP BY u.gender
ORDER BY num_ratings DESC;

-- graf 3: Obľúbenosť filmov podľa ročných období--
SELECT 
    CASE 
        WHEN d.month IN (12, 1, 2) THEN 'Zima'
        WHEN d.month IN (3, 4, 5) THEN 'Jar'
        WHEN d.month IN (6, 7, 8) THEN 'Leto'
        WHEN d.month IN (9, 10, 11) THEN 'Jeseň'
    END AS season,
    COUNT(f.fact_rating) AS num_ratings
FROM fact_ratings f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY season
ORDER BY num_ratings DESC;

-- graf 4: Obľúbenosť filmov u mužov vs žien podľa žánrov--
SELECT 
    g.genre_name,
    u.gender,
    COUNT(f.fact_rating) AS num_ratings
FROM fact_ratings f
JOIN dim_users u ON f.user_id = u.user_id
JOIN dim_genres g ON f.genre_id = g.genre_id
GROUP BY g.genre_name, u.gender
ORDER BY g.genre_name, num_ratings DESC;

-- graf 5: 10 filmov s najlepším hodnotím - od najvyššieho hodnotenia
SELECT 
    g.genre_name, 
    COUNT(f.fact_rating) AS num_ratings
FROM fact_ratings f
JOIN dim_genres g ON f.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY num_ratings DESC
LIMIT 10;
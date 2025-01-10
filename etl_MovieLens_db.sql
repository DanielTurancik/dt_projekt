CREATE DATABASE RAT_MOVIELENS;
USE DATABASE RAT_MOVIELENS;

CREATE OR REPLACE STAGE my_stage;


CREATE OR REPLACE TABLE users_staging (
    id INT,
    age INT,
    gender STRING,
    occupation_id INT,
    zip_code STRING
);

CREATE OR REPLACE TABLE age_groups_staging (
    id INT,
    name STRING
);


CREATE OR REPLACE TABLE movie_genre_relationship_staging (
    id INT,
    movie_id INT,
    genre_id INT
);

CREATE OR REPLACE TABLE movie_genres_staging (
    id INT,
    name STRING
);


CREATE OR REPLACE TABLE movies_staging (
    id INT,
    title STRING,
    release_year INT
);

CREATE OR REPLACE TABLE occupations_staging (
    id INT,
    name STRING
);

CREATE OR REPLACE TABLE ratings_staging (
    id INT,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at TIMESTAMP
);

CREATE OR REPLACE TABLE tags_staging (
    id INT,
    user_id INT,
    movie_id INT,
    tags STRING,
    created_at TIMESTAMP
);



COPY INTO age_groups_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movie_genres_staging
FROM @my_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO movie_genre_relationship_staging
FROM @my_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @my_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @my_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

COPY INTO users_staging
FROM @my_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);



CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
    id AS user_id,
    gender,
    occupation_id AS occupation,
    age,
    CASE
        WHEN age BETWEEN 0 AND 12 THEN 'Child'
        WHEN age BETWEEN 13 AND 19 THEN 'Teen'
        WHEN age BETWEEN 20 AND 35 THEN 'Young Adult'
        WHEN age BETWEEN 36 AND 55 THEN 'Adult'
        ELSE 'Senior'
    END AS age_group,
    zip_code
FROM users_staging;


CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    id AS genre_id,
    name AS genre_name
FROM movie_genres_staging;

CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT
    id AS movie_id,
    title,
    CAST(release_year AS STRING) AS release_year
FROM movies_staging;

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    CAST(EXTRACT(HOUR FROM rated_at) AS INT) AS time_id,
    CAST(rated_at AS TIME) AS timestamp_time,
    EXTRACT(HOUR FROM rated_at) AS hour,
    CASE
        WHEN EXTRACT(HOUR FROM rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm
FROM ratings_staging;

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    CAST(EXTRACT(DAY FROM rated_at) AS INT) AS date_id,
    CAST(rated_at AS DATE) AS timestamp_date,
    EXTRACT(DAY FROM rated_at) AS day,
    EXTRACT(MONTH FROM rated_at) AS month,
    EXTRACT(YEAR FROM rated_at) AS year,
    EXTRACT(WEEK FROM rated_at) AS week,
    CASE
        WHEN EXTRACT(MONTH FROM rated_at) BETWEEN 1 AND 3 THEN 1
        WHEN EXTRACT(MONTH FROM rated_at) BETWEEN 4 AND 6 THEN 2
        WHEN EXTRACT(MONTH FROM rated_at) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS quarter
FROM ratings_staging;

CREATE OR REPLACE TABLE dim_tags AS
SELECT DISTINCT
    id AS tags_id,
    tags,
    created_at
FROM tags_staging;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT DISTINCT
    r.id AS fact_rating,
    r.rating,
    r.user_id,
    r.movie_id,
    t.time_id,
    d.date_id,
    tg.id AS tags_id,
    mgr.genre_id
FROM ratings_staging r
LEFT JOIN dim_time t ON EXTRACT(HOUR FROM r.rated_at) = t.time_id
LEFT JOIN dim_date d ON CAST(r.rated_at AS DATE) = d.timestamp_date
LEFT JOIN tags_staging tg ON r.movie_id = tg.movie_id AND r.user_id = tg.user_id
LEFT JOIN movie_genre_relationship_staging mgr ON r.movie_id = mgr.movie_id;


DROP TABLE IF EXISTS age_groups_staging;
DROP TABLE IF EXISTS movie_genres_staging;
DROP TABLE IF EXISTS movie_genre_relationship_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;



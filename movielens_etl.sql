CREATE DATABASE movielens_shark;
CREATE SCHEMA etl_staging;
CREATE OR REPLACE STAGE my_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE ratings_staging (
    id INT,
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp STRING
);

CREATE OR REPLACE TABLE movies_staging (
    movieId INT,
    title STRING,
    release_year INT 
);

CREATE OR REPLACE TABLE genres_movies_staging (
    id INT,
    movieId INT,
    genreId INT 
);

CREATE OR REPLACE TABLE genres_staging (
    genreId INT,
    name STRING
);

CREATE OR REPLACE TABLE users_staging (
    userId INT,
    age INT,
    gender STRING,
    occupation STRING,
    zipCode STRING
);

CREATE OR REPLACE TABLE occupations_staging (
    occupationId INT,
    name STRING
);

CREATE OR REPLACE TABLE age_group_staging (
    agegroupId INT,
    name STRING
);

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @my_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @my_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @my_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @my_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO age_group_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT * FROM ratings_staging;
SELECT * FROM movies_staging;
SELECT * FROM genres_movies_staging;
SELECT * FROM genres_staging;
SELECT * FROM users_staging;
SELECT * FROM occupations_staging;
SELECT * FROM age_group_staging;

CREATE OR REPLACE TABLE dim_movies AS
SELECT
    DISTINCT movieId,
    title
FROM movies_staging;

CREATE OR REPLACE TABLE bridge_genres_movies AS
SELECT
    DISTINCT id,
    movieId,
    genreId
FROM genres_movies_staging;

CREATE OR REPLACE TABLE dim_genres AS
SELECT
    DISTINCT genreId,
    name
FROM genres_staging;
    
CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
    userId,
    a.name AS agegroup,
    gender,
    o.name AS occupation,
    zipCode
FROM users_staging u
JOIN occupations_staging o ON u.occupation = o.occupationid
JOIN age_group_staging a ON u.age = a.agegroupid; 

CREATE OR REPLACE TABLE dim_date AS
SELECT
    DISTINCT
    TO_DATE(TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS date,
    DATE_PART('year', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS day
FROM ratings_staging;

SELECT * FROM dim_movies;
SELECT * FROM bridge_genres_movies;
SELECT * FROM dim_genres;
SELECT * FROM dim_users;
SELECT * FROM dim_date;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    r.userId,
    r.movieId,
    r.rating,
    d.date AS dateId
FROM ratings_staging r
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date;

SELECT * FROM fact_ratings;

DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging; 

CREATE SCHEMA analysis;

CREATE OR REPLACE VIEW analysis.rating_count_stats AS
SELECT 
    m.title,
    COUNT(rating) AS rating_count,
    ROUND(AVG(rating), 2) AS avg_rating
FROM etl_staging.fact_ratings f
JOIN etl_staging.dim_movies m ON f.movieId = m.movieId
GROUP BY f.movieId, m.title;

SELECT * FROM analysis.rating_count_stats;

CREATE OR REPLACE VIEW analysis.user_rating_stats AS
SELECT
    u.gender,
    u.occupation,
    u.agegroup,
    rating
FROM 
    etl_staging.fact_ratings f
JOIN 
    etl_staging.dim_users u
ON f.userid = u.userid;

SELECT * FROM analysis.user_rating_stats;

CREATE OR REPLACE VIEW analysis.genre_rating_stats AS
SELECT 
    g.name,
    COUNT(f.rating) AS rating_count,
    ROUND(AVG(f.rating), 2) AS average_rating
FROM 
    etl_staging.fact_ratings f
JOIN 
    etl_staging.bridge_genres_movies b
ON 
    f.movieid = b.movieid
JOIN
    etl_staging.dim_genres g
ON 
    b.genreid = g.genreid
GROUP BY 
    g.name;

SELECT * FROM analysis.genre_rating_stats;

CREATE OR REPLACE VIEW analysis.date_rating_stats AS
SELECT
    d.year,
    d.month,
    d.day,
    COUNT(rating) AS rating_count,
    ROUND(AVG(rating), 2) AS average_rating
FROM
    etl_staging.fact_ratings f
JOIN 
     etl_staging.dim_date d
ON 
    f.dateid = d.date
GROUP BY
    d.year, d.month, d.day
ORDER BY
    d.year, d.month, d.day ASC;

SELECT * FROM analysis.date_rating_stats;
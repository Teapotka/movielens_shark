# ETL proces datasetu MovieLens

Tento repozitar obsahuje implementovanie ETL procesu pre analyzu dat z MovieLens datasetu. Proces zahrna kroky na extrahovanie, transformovanie a nacitanie dat do dimenzionalneho modelu v Snowflake. Tento model podporuje vizualizaciu a analyzu filmov, uzivatelov a ich hodnoteni.

## 1. Uvod a popis zdrojovych dat

Cielom tejto prace bolo uskutocnit analyzu dat ktore sa tykaju filmov a ich zanrov, hodnoteni, userov a ich demografickych udajov. Tato analyza umoznuje identifikovat chronologiu mnozstva hodnoteni filmov od roku 2000 do 2003 vratane a taktiez pomier hodnoteni (kvantativne a kvalikativne) za den, popularnost konretnych zanrov, zavislost hodnoteni (kvantativne a kvalikativne) od sektorami prace userov, ich vekovej ci gendernej prislusnoti.  

Zdrojove Data:
- `ratings.csv`: Hodnotenia filmov uzivatelmi.
- `movies.csv`: Informacie o filmoch.
- `genres_movies.csv`: Mapping relacie N:M medzi filmami a zanermi
- `genres.csv`: Zanre filmov.
- `users.csv`: Demograficke udaje o uzivateloch.
- `occupations.csv`: Zamestnania.
- `age_group.csv`: Vekove skupiny.

### 1.1 Datova architektura

ERD diagram MovieLens

| <img src="./MovieLens_ERD.png"/> |
|:-:|
|*Obrazok 1 Entitno-relacna schema MovieLens*|

## 2. Dimenzionalny model

Bol navrhnty hviezdicovy model (star schema), kde centralnou tabulkou faktou sa stala `fact_ratings`, na ktoru sa pripajaju dalsie dimenzie:

- `dim_date` - datumy hodnoteni (den, mesiac, rok)
- `dim_movies` - nazvy filmov
- `dim_users` - demograficke udaje userov: vekova kategoria, pohlavie, lokalita, zamestanie

Kvoli relacie N:M medzi entitami `movies` a `genres` bolo rozhodnute vyuzit `bridge` pre spravne podalsie mapovanie

- `dim_genres` - nazvy zanrov

Struktura schemy je znazornena nizsie

| <img src="./star_scheme.png"/> |
|:-:|
|*Obrazok 2 Star schema MovieLens*|

## 3. ETL proces v Snowflake

ETL proces pozostaval z troch hlavnych faz: extrahovanie (Extract), transformacia (Transform) a nacitanie (Load). Tento proces bol implementovany v Snowflake s cielom pripravit zdrojove data zo staging vrstvy do dimenzionalneho modelu, ktory by uz potom mohol sa analyzovat a vizualizovat.

### 3.1 Extract

`.csv` subory boli najprv nahrate pomocou interneho stage `my_stage`, co bolo realizovane prikazom:

```sql
CREATE OR REPLACE STAGE my_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

Nasledujucim krokom bolo nahravanie obsahu kazdeho `.csv` suboru do staging tabulky. pre kazdu tabulku sa vyuzivali podobne prikazy:

1. Vytvorenie

```sql
CREATE OR REPLACE TABLE ratings_staging (
    id INT,
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp STRING
);
```

2. Importovanie dat

```sql
COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

3. Overovanie spravnosti operacii

```sql
SELECT * FROM ratings_staging;
```

### 3.2 Transform

V tejto faze sa zabezpecovalo vycistenie, transformovanie a obohatenie dat zo staging tabuliek. Hlavnym cielom sa nechavalo pripravovanie dimenzii a faktovej tabulky na jednoduchu a efektivnu analizu.

Prvym krokom bolo vytvorenie dimenzii:
- `dim_users` - bolo rozhodnute realizovat `join` tabuliek `occupations_staging` a `age_group_staging` v tuto dimenziu pre denormalizaciu. Vo vysledku `dim_users` obsahovala popis vekovych kategorii (napr. "25-34") a pracovneho sektoru (napr. "Lawyer").<br/>
***Typ dimenzie SCD1 (Slowly Changing Dimensions - Overwrite Old Value)***<br/>
Moze sa aktualizovat informacie o usere bez historickeho ukladania zmien

```sql
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
```

- `dim_movies` ako aj `dim_genres` - obsahuju iba nazvy. pri transformacie `movies_staging` bolo vynechany stlpec `release_year` kvoli nezameriavani analyzy na data, ktore on poskytuje.<br/>
***Typ dimezii SCD0 (Slowly Changing Dimensions - Retain Original Value)***<br/>
nazvy zanrov a filmy casovo nemenia

```sql
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    DISTINCT movieId,
    title
FROM movies_staging;

CREATE OR REPLACE TABLE dim_genres AS
SELECT
    DISTINCT genreId,
    name
FROM genres_staging;
```

- `dim_date` - bola navrhnuta tak ze jej id sluzi presny datum (rok-mesiac-den) a obsahuje ich aj zvlast ako typ INT. je strukturovana pre vhodnu casovu analyzu. 
***Typ dimezie SCD0 (Slowly Changing Dimensions - Retain Original Value)***
Datumy po ich ulozeni uz sa nemenia

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    DISTINCT
    TO_DATE(TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS date,
    DATE_PART('year', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS day
FROM ratings_staging;
```

- `fact_rating`- obsahuje metriky(rating) a cudzie kluce na suvisiace dimenzie (movieId, userId, dateId)


```sql
CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    r.userId,
    r.movieId,
    r.rating,
    d.date AS dateId
FROM ratings_staging r
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date;
```

### 3.3 Load

Po vytvoreni dimenzii a faktovej tabulky data a nahravani do nich dat, bolo rozhodnute odstranit staging tabulky pre optimalizaciu vyuzitia uloziska.

```sql
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging; 
```
Vo vysledku vdaka ETL processu sa podarilo uskutocnit rychlu a effektivnu manipulaciu dat z `.csv` pre realizaciu zadefinovaneho multidimenzionalneho modelu typu star.
Taktiez pre buducnu analyzu boli vytvorene View vo scheme `analysis`:

- `rating_count_stats`: predstavuje sebou zlucenie s `dim_movies` pre ziskavanie premier (kvalitativ) a pocet (kvantativ) hodnoteni pre kazdy film: 

```sql
CREATE OR REPLACE VIEW analysis.rating_count_stats AS
SELECT 
    m.title,
    COUNT(rating) AS rating_count,
    ROUND(AVG(rating), 2) AS avg_rating
FROM etl_staging.fact_ratings f
JOIN etl_staging.dim_movies m ON f.movieId = m.movieId
GROUP BY f.movieId, m.title;
```

- `user_rating_stats` - predstavuje sebou zlucenie s `dim_users` pre ziskavanie demogracikych parametrov na analyzu zavislosti hodnoteni od nich

```sql
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
```

- `genre_rating_stats` - predstavuje sebou zlucenie `N:M` vztahu tabuliek `dim_movies` a `dim_genres` pre ziskavanie relacie medzi zanrami a hodnoteniami (ich kvantativ a kvalikativ) pre nasledujucu analyzu

```sql
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
```

- `date_rating_stats` - predstavuje sebou zlucenie s `dim_date` pre ziskavanie chronologie hodnoceni a ich kvalitativ a kvantativ

```sql
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
```

## 4. Vizualizacia dat

Bolo navrhnute `10 vizualizacii`

### 1. Pocet hodnoteni pre kazdy film

| <img src="./visualizations/analysis_1.png"/> |
|:-:|
|*Obrazok 3 pocet hodnoteni pre kazdy film*|

Umoznuje zobrazovania vsetkeho poctu filmov a pre kazdy jeho kvantativ hodnoteni. Takto mozeme sa dozviet ze najviac hodnoteni ma film `American Beauty (1999)` ***(az 3428)*** a najmens - `Wooden Man's Bride, The (Wu Kui) (1994)` ***(iba 1)***. Taktiez ihlovaty tvar grafu na zaciatku sviedci o tom ze rozdiel v poctach hodnoteni medzi popularnymi filmami je velky ale zaroven hladky tvar grafu na konci ma opacny vyznam - rozdiel medzi mens hodnotenymi filmami maly.

```sql
SELECT title, rating_count FROM analysis.rating_count_stats;
```

### 2. Premier hodnoteni pre kazdy film

| <img src="./visualizations/analysis_2.png"/> |
|:-:|
|*Obrazok 4 premier hodnoteni pre kazdy film*|

Umoznuje zobrazovania vsetkeho poctu filmov a pre kazdy jeho premier hodnoteni (kvalitativ). Takto mozeme sa dozviet ze najvyssie hodnotenie ma film `Baby, The (1973)` ***(az 5)*** a najnizsie - `Wirey Spindell (1999)` ***(iba 1)***. Taktiez ihlovaty tvar grafu na koncach i hladky v strede sviedci o tom ze je malo filmov z velmy vysokym ci naopak velmi nizkym hodnotenim ale vacsina filmov maju upokojive hodnotenie

```sql
SELECT title, rating_count FROM analysis.rating_count_stats;
```

### 3. Chronoligia hodnoteni

| <img src="./visualizations/analysis_3.png"/> |
|:-:|
|*Obrazok 5 Chronoligia hodnoteni*|

Umoznuje zobrazovanie vsetkeho poctu hodnoteni na casovej osi. Takto mozeme sa dozviet ze do stredy novebra 2000 usery hodnodnotili filmy dost aktivne az do anomalneho poctu hodnoteni v 20.11.2000 - 61754 hodnoteni. po tom aktivnost userov zacala klesat. Predpokladom takej tendencie moze byt zaciatocna popularita platformy, aktivna a upesna promocia filmov 20.11.2000 a nasledovne spad popularity tejto platformy.

```sql
SELECT TO_DATE(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'))) AS rating_date, rating_count FROM date_rating_stats;
```

### 4-7. Cislove vysledky

| <img src="./visualizations/analysis_4_7.png"/> |
|:-:|
|*Obrazok 6 Cislove vysledky*|

4. Hodnota premierneho hodnotenia za den sa rovna `3.54` co je upokojivym

```sql
SELECT AVG(average_rating) FROM date_rating_stats;
```

5. Hodnota premierneho poctu hodnoteni za den sa rovna `965` co sviedci o dostatnej aktivite za cely cas

```sql
SELECT ROUND(AVG(rating_count)) FROM date_rating_stats;
```

6. a 7. sviedci o tom ze premierne hodnotenie userov gendernej kategorie typu pan je o trochu mensie ako userov gendernej kategorie typu pani

```sql
SELECT ROUND(AVG(rating),2) FROM user_rating_stats WHERE gender LIKE 'M';

SELECT ROUND(AVG(rating),2) FROM user_rating_stats WHERE gender LIKE 'F';
```

### 8-10. zanre, vek a zamestnania ku hodnoteniam

| <img src="./visualizations/analysis_8_10.png"/> |
|:-:|
|*Obrazok 7 zanre, vek a zamestnania ku hodnoteniam*|

8. Z tohto grafu da sa odvodit ze najpopularnejsi zaner je `Comedy`, ale najvacsie priemerne hodnotenie ma `Drama`.

```sql
SELECT name, rating_count, average_rating FROM genre_rating_stats ORDER BY rating_count DESC LIMIT 10;
```

9. Z tohto grafu da sa odvodit ze najviac hodnoteni su od vekovej kategorie userov `25-34`, ale najvacsie priemerne hodnotenie sa ziskava od kategorie `56+`.

```sql
SELECT agegroup, COUNT(rating) AS rating_count, ROUND(AVG(rating), 2) AS average_rating FROM user_rating_stats GROUP BY agegroup;
```

10. Z tohto grafu da sa odvodit ze najviac hodnoteni su od userov, ktore sa nachadzaju v pracovnem sektore `Educator`, ale najvacsie priemerne hodnotenie sa ziskava od userov, ktore sa nachadzaju v pracovnem sektore `Marketing`.

```sql
SELECT occupation, ROUND(AVG(rating), 2) AS average_rating, COUNT(rating) AS rating_count FROM user_rating_stats GROUP BY occupation ORDER BY rating_count DESC LIMIT 10;
```

-- DATA CLEANING AND EXPLORATION FOR THE MOST STREAMED SONGS ON SPOTIFY FOR 2024 --
SELECT *
FROM most_streamed_songs_2024;

-- Create a copy of the raw data (purpose: to have raw data available if needed) --
CREATE TABLE most_streamed_songs_2024_copy
LIKE most_streamed_songs_2024;

SELECT *
FROM most_streamed_songs_2024_copy; -- check new table created

INSERT most_streamed_songs_2024_copy
SELECT *
FROM most_streamed_songs_2024; -- insert data from original table to new table


-- STEP 1: REMOVE ANY EXISTING DUPLICATES --------------------------------------


-- row number to match against columns, partition against all columns
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Track, `Album Name`, Artist, 
`Release Date`, ISRC, `All Time Rank`, `Spotify Streams`, `Spotify Playlist Count`,
`Spotify Playlist Reach`, `Spotify Popularity`, `YouTube Views`, `YouTube Likes`,
`TikTok Posts`, `TikTok Likes`, `TikTok Views`, `YouTube Playlist Reach`, 
`Apple Music Playlist Count`, `AirPlay Spins`, `SiriusXM Spins`, `Deezer Playlist Count`,
`Deezer Playlist Reach`, `Amazon Playlist Count`, `Pandora Streams`, `Pandora Track Stations`,
`Soundcloud Streams`, `Shazam Counts`, `TIDAL Popularity`, `Explicit Track`) as row_num
FROM most_streamed_songs_2024_copy;

-- filter if row_num is 2+ (indicates duplicates)
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Track, `Album Name`, Artist, 
`Release Date`, ISRC, `All Time Rank`, `Spotify Streams`, `Spotify Playlist Count`,
`Spotify Playlist Reach`, `Spotify Popularity`, `YouTube Views`, `YouTube Likes`,
`TikTok Posts`, `TikTok Likes`, `TikTok Views`, `YouTube Playlist Reach`, 
`Apple Music Playlist Count`, `AirPlay Spins`, `SiriusXM Spins`, `Deezer Playlist Count`,
`Deezer Playlist Reach`, `Amazon Playlist Count`, `Pandora Streams`, `Pandora Track Stations`,
`Soundcloud Streams`, `Shazam Counts`, `TIDAL Popularity`, `Explicit Track`) as row_num
FROM most_streamed_songs_2024_copy
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- File has no duplicates


-- STEP 2: STANDARDIZE THE DATA -------------------------------------------


SELECT *
FROM most_streamed_songs_2024_copy;

-- Remove white space from texts such as Track, Album Name, Artist
SELECT Track, TRIM(Track), `Album Name`, TRIM(`Album Name`), Artist, TRIM(Artist)
FROM most_streamed_songs_2024_copy;
-- Update the changes to the file
UPDATE most_streamed_songs_2024_copy
SET Track = TRIM(Track), `Album Name` = TRIM(`Album Name`), Artist = TRIM(Artist);

-- Check for redundancies in Artist
-- LOWER to standardize cases, not updated to keep the same format for presentation
SELECT LOWER(Artist) AS standard_artist,
       COUNT(*) AS total_count,
       COUNT(DISTINCT Artist) AS distinct_case_variants
FROM most_streamed_songs_2024_copy
GROUP BY LOWER(Artist)
HAVING COUNT(DISTINCT Artist) > 1;
-- no duplicates found

SELECT LOWER(`Album Name`) AS standard_album,
       COUNT(*) AS total_count,
       COUNT(DISTINCT `Album Name`) AS distinct_case_album
FROM most_streamed_songs_2024_copy
GROUP BY LOWER(`Album Name`)
HAVING COUNT(DISTINCT `Album Name`) > 1;
-- no duplicates found

-- Remove commas from columns with data type intended to be integers and replace blanks with NULL to convert data type text to INT/BIGINT
UPDATE most_streamed_songs_2024_copy
SET
`Spotify Streams` = REPLACE(NULLIF(`Spotify Streams`, ''), ',', ''),
`Spotify Playlist Count` = REPLACE(NULLIF(`Spotify Playlist Count`, ''), ',', ''),
`Spotify Playlist Reach` = REPLACE(NULLIF(`Spotify Playlist Reach`, ''), ',', ''),
`Spotify Popularity` = REPLACE(NULLIF(`Spotify Popularity`, ''), ',', ''),
`YouTube Views` = REPLACE(NULLIF(`YouTube Views`, ''), ',', ''),
`YouTube Likes` = REPLACE(NULLIF(`YouTube Likes`, ''), ',', ''),
`TikTok Posts` = REPLACE(NULLIF(`TikTok Posts`, ''), ',', ''),
`TikTok Likes` = REPLACE(NULLIF(`TikTok Likes`, ''), ',', ''),
`TikTok Views` = REPLACE(NULLIF(`TikTok Views`, ''), ',', ''),
`YouTube Playlist Reach` = REPLACE(NULLIF(`YouTube Playlist Reach`, ''), ',', ''),
`Apple Music Playlist Count` = REPLACE(NULLIF(`Apple Music Playlist Count`, ''), ',', ''),
`AirPlay Spins` = REPLACE(NULLIF(`AirPlay Spins`, ''), ',', ''),
`SiriusXM Spins` = REPLACE(NULLIF(`SiriusXM Spins`, ''), ',', ''),
`Deezer Playlist Count` = REPLACE(NULLIF(`Deezer Playlist Count`, ''), ',', ''),
`Deezer Playlist Reach` = REPLACE(NULLIF(`Deezer Playlist Reach`, ''), ',', ''),
`Amazon Playlist Count` = REPLACE(NULLIF(`Amazon Playlist Count`, ''), ',', ''),
`Pandora Streams` = REPLACE(NULLIF(`Pandora Streams`, ''), ',', ''),
`Pandora Track Stations` = REPLACE(NULLIF(`Pandora Track Stations`, ''), ',', ''),
`Soundcloud Streams` = REPLACE(NULLIF(`Soundcloud Streams`, ''), ',', ''),
`Shazam Counts` = REPLACE(NULLIF(`Shazam Counts`, ''), ',', ''),
`Tidal Popularity` = REPLACE(NULLIF(`Tidal Popularity`, ''), ',', ''),
`Explicit Track` = REPLACE(NULLIF(`Explicit Track`, ''), ',', '');

SELECT *
FROM most_streamed_songs_2024_copy;

-- Ensure correct data type for columns
-- BIGINT is used over INT if column contains values out of range for INT
ALTER TABLE most_streamed_songs_2024_copy
MODIFY COLUMN `All Time Rank` INT, 
MODIFY COLUMN `Track Score` DECIMAL(4, 1),
MODIFY COLUMN `Spotify Streams` BIGINT,
MODIFY COLUMN `Spotify Playlist Count` BIGINT,
MODIFY COLUMN `Spotify Playlist Reach` BIGINT, 
MODIFY COLUMN `Spotify Popularity` INT, 
MODIFY COLUMN `YouTube Views` BIGINT, 
MODIFY COLUMN `YouTube Likes` BIGINT,
MODIFY COLUMN `TikTok Posts` BIGINT, 
MODIFY COLUMN `TikTok Likes` BIGINT, 
MODIFY COLUMN `TikTok Views` BIGINT, 
MODIFY COLUMN `YouTube Playlist Reach` BIGINT, 
MODIFY COLUMN `Apple Music Playlist Count` BIGINT, 
MODIFY COLUMN `AirPlay Spins` INT, 
MODIFY COLUMN `SiriusXM Spins` INT, 
MODIFY COLUMN `Deezer Playlist Count` INT,
MODIFY COLUMN `Deezer Playlist Reach` BIGINT, 
MODIFY COLUMN `Amazon Playlist Count` INT, 
MODIFY COLUMN `Pandora Streams` BIGINT, 
MODIFY COLUMN `Pandora Track Stations` INT,
MODIFY COLUMN `Soundcloud Streams` BIGINT, 
MODIFY COLUMN `Shazam Counts` BIGINT, 
MODIFY COLUMN `TIDAL Popularity` INT, 
MODIFY COLUMN `Explicit Track` INT;

-- Set date column as date
SELECT `Release Date`,
STR_TO_DATE(`Release Date`, '%m/%d/%Y') -- makes the string into a date
FROM most_streamed_songs_2024_copy;

UPDATE most_streamed_songs_2024_copy
SET `Release Date` = STR_TO_DATE(`Release Date`, '%m/%d/%Y');

ALTER TABLE most_streamed_songs_2024_copy
MODIFY COLUMN `Release Date` DATE;


-- STEP 3: NULL Values
-- change null values to 0 in int/bigint data type columns for analysis

UPDATE most_streamed_songs_2024_copy
SET 
    `Spotify Streams` = COALESCE(`Spotify Streams`, 0),
    `Spotify Playlist Count` = COALESCE(`Spotify Playlist Count`, 0),
    `Spotify Playlist Reach` = COALESCE(`Spotify Playlist Reach`, 0),
    `Spotify Popularity` = COALESCE(`Spotify Popularity`, 0),
    `YouTube Views` = COALESCE(`YouTube Views`, 0),
    `YouTube Likes` = COALESCE(`YouTube Likes`, 0),
    `TikTok Posts` = COALESCE(`TikTok Posts`, 0),
    `TikTok Likes` = COALESCE(`TikTok Likes`, 0),
    `TikTok Views` = COALESCE(`TikTok Views`, 0),
    `YouTube Playlist Reach` = COALESCE(`YouTube Playlist Reach`, 0),
    `Apple Music Playlist Count` = COALESCE(`Apple Music Playlist Count`, 0),
	`AirPlay Spins` = COALESCE(`AirPlay Spins`, 0),
	`SiriusXM Spins` = COALESCE(`SiriusXM Spins`, 0),
	`Deezer Playlist Count` = COALESCE(`Deezer Playlist Count`, 0),
    `Deezer Playlist Reach` = COALESCE(`Deezer Playlist Reach`, 0),
    `Amazon Playlist Count` = COALESCE(`Amazon Playlist Count`, 0),
    `Pandora Streams` = COALESCE(`Pandora Streams`, 0),
    `Pandora Track Stations` = COALESCE(`Pandora Track Stations`, 0),
    `Soundcloud Streams` = COALESCE(`Soundcloud Streams`, 0),
    `Shazam Counts` = COALESCE(`Shazam Counts`, 0),
    `Explicit Track` = COALESCE(`Explicit Track`, 0);


-- STEP 4: REMOVE COLUMNS OR ROWS NOT NEEDED --------------------------------------------


SELECT *
FROM most_streamed_songs_2024_copy;

-- TIDAL Popularity column has an overwhelming amount of NULL values
SELECT `TIDAL Popularity`, COUNT(*) as null_count
FROM most_streamed_songs_2024_copy
WHERE `TIDAL Popularity` IS NULL
GROUP BY `TIDAL Popularity`;

-- Deleting columns - TIDAL has no data so column will be dropped
ALTER TABLE most_streamed_songs_2024_copy
DROP COLUMN `TIDAL Popularity`;


-- EXPLORATORY DATA ANALYSIS --------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM most_streamed_songs_2024_copy;


-- 1) Which top 10 artists had the most songs make it on the 'most streamed songs of 2024'?
SELECT Artist, COUNT(Track) as track_cnt
FROM most_streamed_songs_2024_copy
GROUP BY Artist
ORDER BY track_cnt DESC
LIMIT 10;

-- 2) Which albums had the most songs be on the 'most streamed for 2024'?
SELECT `Album Name`, Artist, COUNT(`Album Name`) as cnt
FROM most_streamed_songs_2024_copy
GROUP BY `Album Name`, Artist
ORDER BY cnt DESC
LIMIT 10;

-- 3) What were the top 5 songs by spotify streams in 2024?
SELECT Track, Artist, FORMAT(`Spotify Streams`, 0) as spotify_streams
FROM most_streamed_songs_2024_copy
ORDER BY `Spotify Streams` DESC
LIMIT 5;

-- 4) What are the top 5 highest streaming tracks across all music-specific streaming platforms?
ALTER TABLE most_streamed_songs_2024_copy
ADD COLUMN total_streams BIGINT;

UPDATE most_streamed_songs_2024_copy
SET
	total_streams = (`Spotify Streams` + `YouTube Views` + 
    `AirPlay Spins` + `SiriusXM Spins` +
    `Pandora Streams` + `Soundcloud Streams`);

SELECT Track, Artist, FORMAT(total_streams, 0) as total_music_streams
FROM most_streamed_songs_2024_copy
ORDER BY total_streams DESC
LIMIT 5;

-- 5) Which 5 songs had the most YouTube Views?
SELECT Track, Artist, FORMAT(`YouTube Views`,0) as youtube_views
FROM most_streamed_songs_2024_copy
ORDER BY `YouTube Views` DESC
LIMIT 5;

-- 6) Which tracks have appeared the most in playlist across all music-specific platforms?
SELECT Track, Artist,
	FORMAT(SUM(`Spotify Playlist Count`),0) as sum_spotify_playlist,
    FORMAT(SUM(`Apple Music Playlist Count`),0) as sum_apple_playlist,
    FORMAT(SUM(`Deezer Playlist Count`),0) as sum_deezer_playlist,
    FORMAT(SUM(`Amazon Playlist Count`),0) as sum_amazon_playlist,
    FORMAT(SUM(`Pandora Track Stations`),0) as sum_pandora_stations
FROM most_streamed_songs_2024_copy
GROUP BY Track, Artist, `Spotify Playlist Count`
ORDER BY `Spotify Playlist Count` DESC
LIMIT 5;

-- 7) What high-streaming Artists have platform-specific popularity to spotify? Assuming >60% defines platform-specific
-- ordering by spotify streams to show the highest streaming artists being considered platform-specific to spotify
SELECT Artist,
	FORMAT(SUM(`Spotify Streams`),0) as sum_spotify_streams,
    FORMAT(SUM(`YouTube Views`),0) as sum_youtube_views,
    FORMAT(SUM(`AirPlay Spins`),0) as sum_airplay_spins,
    FORMAT(SUM(`SiriusXM Spins`),0) as sum_siriusxm_spins,
    FORMAT(SUM(`Pandora Streams`),0) as sum_pandora_streams,
    FORMAT(SUM(`Soundcloud Streams`),0) as sum_soundcloud_streams,
	ROUND(
	SUM(`Spotify Streams`) * 100.0 / 
	NULLIF(
		SUM(`Spotify Streams`) +
		SUM(`YouTube Views`) +
        SUM(`AirPlay Spins`) +
        SUM(`SiriusXM Spins`) +
		SUM(`Pandora Streams`) +
		SUM(`Soundcloud Streams`),
        0), 2
    ) AS spotify_stream_percent
FROM most_streamed_songs_2024_copy
GROUP BY Artist
HAVING spotify_stream_percent >=60
ORDER BY SUM(`Spotify Streams`) DESC;
-- Data can be used to see if there are any competitive artists that a platform such as
-- spotify could incentivize to bring more streamers to their platform instead
-- either close numbers with spotify or dominating spotify

-- 8) What high-streaming artists have almost platform-majority streaming with spotify? Assuming almost platform-majority = 45-60% streams with spotify
SELECT Artist,
	FORMAT(SUM(`Spotify Streams`),0) as sum_spotify_streams,
    FORMAT(SUM(`YouTube Views`),0) as sum_youtube_views,
    FORMAT(SUM(`AirPlay Spins`),0) as sum_airplay_spins,
    FORMAT(SUM(`SiriusXM Spins`),0) as sum_siriusxm_spins,
    FORMAT(SUM(`Pandora Streams`),0) as sum_pandora_streams,
    FORMAT(SUM(`Soundcloud Streams`),0) as sum_soundcloud_streams,
	ROUND(
	SUM(`Spotify Streams`) * 100.0 / 
	NULLIF(
		SUM(`Spotify Streams`) +
		SUM(`YouTube Views`) +
        SUM(`AirPlay Spins`) +
        SUM(`SiriusXM Spins`) +
		SUM(`Pandora Streams`) +
		SUM(`Soundcloud Streams`),
        0), 2
    ) AS spotify_stream_percent
FROM most_streamed_songs_2024_copy
GROUP BY Artist
HAVING spotify_stream_percent BETWEEN 45 AND 60
ORDER BY SUM(`Spotify Streams`) DESC;
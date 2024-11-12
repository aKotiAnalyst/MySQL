/* Data Cleaning
Here I use a data set from kaggle.com, just for research and training purpose.
world_layoffs.csv
 */

SELECT *
FROM world_layoffs.layoffs_raw;

/*
 1. Remove Duplicates
 2. Standardize the Data
 3. Remove Null or blank values
 4. Delete unnecessary columns 
 */

-- 1. Removing Duplicates

-- Creating a staging table as not to offset the original table
CREATE Table layoffs_staging
LIKE layoffs_raw;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs_raw;

-- Identifying Duplicates; unique entries will have row_num = 1, while row_num >=2 it means there is an issue
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, 'date', 
			stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Duplicates in a CTE, using the above 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, 'date', 
			stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Checking some of the duplicate entrys from our CTE
SELECT * 
FROM layoffs_staging
WHERE company = 'Alerzo';

-- a new Table where we can delete duplicates based on `row_num`
CREATE TABLE `layoffs_staging_unique` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging_unique;

INSERT INTO layoffs_staging_unique
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, 'date', 
			stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Checking for duplicates
SELECT * 
FROM layoffs_staging_unique
WHERE row_num > 1;

-- and deleting them
DELETE 
FROM layoffs_staging_unique
WHERE row_num > 1;

-- Checking that our new table has only unique entries
SELECT * 
FROM layoffs_staging_unique;



-- 2. Standardizing the Data

-- Checking for white space in company names
SELECT company, TRIM(company)
FROM layoffs_staging_unique;

-- and removing them
UPDATE layoffs_staging_unique
SET company = TRIM(company);

-- Checking industry entries for other type-os
SELECT DISTINCT industry
FROM layoffs_staging_unique
ORDER BY 1;

-- deepdive into multiple spellings for 'Crypto'
SELECT *
FROM layoffs_staging_unique
WHERE industry LIKE 'Crypto%';

-- Standarlizing it
UPDATE layoffs_staging_unique
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging_unique
ORDER BY 1;

-- Next issue is blanks as industry names from the above
SELECT *
FROM layoffs_staging_unique
WHERE industry LIKE '';

-- fixing issues with industry name as blanks ''
UPDATE layoffs_staging_unique
SET industry = 'Other'
WHERE industry LIKE '';

SELECT DISTINCT industry
FROM layoffs_staging_unique
ORDER BY 1;

-- Next issues is Null for industry names
SELECT *
FROM layoffs_staging_unique
WHERE industry is null;

-- fixing it
UPDATE layoffs_staging_unique
SET industry = 'Other'
WHERE industry is null;

SELECT DISTINCT industry
FROM layoffs_staging_unique
ORDER BY 1;

-- Looking at locations
SELECT DISTINCT location
FROM layoffs_staging_unique
ORDER BY 1;

-- Looking at location
SELECT DISTINCT location
FROM layoffs_staging_unique
ORDER BY 1;

-- Looking at country
SELECT DISTINCT country
FROM layoffs_staging_unique
ORDER BY 1;

-- issue with spelling for US
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_unique
ORDER BY 1;

-- applying the fix
UPDATE layoffs_staging_unique
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Updating the date from text to a date format
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging_unique;

UPDATE layoffs_staging_unique
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging_unique;

ALTER TABLE layoffs_staging_unique
MODIFY COLUMN `date` DATE;

DESCRIBE layoffs_staging_unique `date`;



-- 3. Remove irelavant Null 

SELECT *
FROM layoffs_staging_unique
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging_unique
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- 4. Delete unnecessary columns 
ALTER TABLE layoffs_staging_unique
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging_unique;
-- this looks cleaner now






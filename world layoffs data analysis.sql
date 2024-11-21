/* Data Analysis & Exploring 
Here I use the data that was previously cleaned. 
Orignal world_layoffs.csv from kaggle.com, just for research and training purposes.
world_layoffs.csv
 */
 
 SELECT *
 FROM layoffs_staging_unique;
 
 SELECT MIN(`date`), MAX(`date`)
 FROM layoffs_staging_unique;
 -- the data represents the covid period
 
 SELECT MAX(total_laid_off), MAX(percentage_laid_off)
 FROM layoffs_staging_unique;
 
 -- looking at compamies that fired 100% of their staff
 SELECT *
 FROM layoffs_staging_unique
 WHERE percentage_laid_off = 1
 ORDER BY total_laid_off DESC;
 
 
 -- companies with the biggest valuation to go under
 SELECT *
 FROM layoffs_staging_unique
 WHERE percentage_laid_off = 1
 ORDER BY funds_raised_millions DESC;
 
 -- companies with the highest number of emp that were let go
 SELECT company, SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company
 ORDER BY 2 DESC;
 
 -- the industries that were hit the hardest 
 SELECT industry, SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY industry
 ORDER BY 2 DESC;
 
 -- hardest hit countries
 SELECT country, SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY country
 ORDER BY 2 DESC;
 
 -- days with the highest number of firings
 SELECT `date`, SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY `date`
 ORDER BY 2 DESC;
 
 -- laid off by year
 SELECT YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY YEAR(`date`)
 ORDER BY 1 DESC;
 
 -- by month
 SELECT SUBSTRING(`date`, 6,2) AS `Month`, SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY `Month`;
 
  -- by year & month
 SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
 FROM layoffs_staging_unique
 WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
 GROUP BY `Month`
 ORDER BY 1 ASC;
 
   -- by year & month
 SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
 FROM layoffs_staging_unique
 WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
 GROUP BY `Month`
 ORDER BY 2 DESC;
 

-- using a CTE to roll data based on month
WITH Rolling_Total AS
(
 SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS sum_laid_off
 FROM layoffs_staging_unique
 WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
 GROUP BY `Month`
 ORDER BY 1 ASC
)
SELECT `Month`, sum_laid_off, 
		SUM(sum_laid_off) OVER (ORDER BY `Month`) AS rolling_total
FROM Rolling_Total;

 -- companies layoffs  by year
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company, YEAR(`date`)
 ORDER BY company ASC;
 
 -- companies with most layoffs by year
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company, YEAR(`date`)
 ORDER BY 3 DESC;
 
 -- using a CTE to rank company layoffs
 WITH Company_Year (company, years, total_laid_off) AS
 (
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company, YEAR(`date`)
 )
 SELECT *, 
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
 FROM Company_Year
 WHERE years IS NOT NULL;
 
 WITH Company_Year (company, years, total_laid_off) AS
 (
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company, YEAR(`date`)
 )
 SELECT *, 
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
 FROM Company_Year
 WHERE years IS NOT NULL
 ORDER BY ranking ASC;
 
 -- top 6
 WITH Company_Year (company, years, total_laid_off) AS
 (
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY company, YEAR(`date`)
 ), 
 Company_Year_Rank AS
 (
 SELECT *, 
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
 FROM Company_Year
 WHERE years IS NOT NULL
 )
 SELECT *
 FROM Company_Year_Rank
 WHERE ranking <= 6;
 
 -- now also ranking the industries based on years and layoffs (top 6)
 WITH Industry_Year (industry, years, total_laid_off) AS
 (
 SELECT industry, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging_unique
 GROUP BY industry, YEAR(`date`)
 ), 
 Industry_Year_Rank AS
 (
 SELECT *, 
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
 FROM Industry_Year
 WHERE years IS NOT NULL
 )
 SELECT *
 FROM Industry_Year_Rank
 WHERE ranking <= 6;
 
 
 
 
 
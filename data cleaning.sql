create table layoffs_staging
like layoffs;

insert layoffs_staging
select*
from layoffs;

select * from layoffs_staging
where company='casper';

select *,
row_number()over(
partition by company,total_laid_off,percentage_laid_off,`date`, country,funds_raised_millions,location ,industry,stage) as row_num
 from layoffs_staging
;
with duplicate_cte as 
(       
select *,
row_number()over(
partition by company,total_laid_off,percentage_laid_off,`date`, country,funds_raised_millions,location ,industry,stage) as row_num
 from layoffs_staging
 )

select* 
from duplicate_cte 
where row_num>1
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



select * from
layoffs_staging2 ;

INSERT INTO layoffs_staging2 (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num)
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
       ROW_NUMBER() OVER (PARTITION BY company, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions, location, industry, stage) AS row_num
FROM layoffs_staging;





select * from
layoffs_staging2 
where row_num>1;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;


# standardizing data 
SELECT * 
FROM world_layoffs.layoffs_staging2;


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SET SQL_SAFE_UPDATES = 1;



SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_staging2 SET industry = NULL WHERE industry = ''; 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');
SET SQL_SAFE_UPDATES = 1;


SELECT *
FROM world_layoffs.layoffs_staging2;


SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);
SET SQL_SAFE_UPDATES = 1;

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;



SELECT *
FROM layoffs_staging2;


UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') WHERE `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$';

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off ='none'
AND percentage_laid_off = 'none';


SELECT * 
FROM world_layoffs.layoffs_staging2;

DELETE FROM layoffs_staging2 WHERE total_laid_off = 'none' AND percentage_laid_off = 'none';

SELECT * 
FROM world_layoffs.layoffs_staging2;

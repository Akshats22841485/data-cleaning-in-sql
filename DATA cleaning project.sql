-- Cleaning Dataset

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte1 AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte1
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num>1;

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;


-- Standardizing Data

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country='United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- OR we can do the following for fixing the problem of . at the end of country name

SELECT DISTINCT country,TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country=TRIM(TRAILING '.' FROM country);

-- CHANGING date from text to date format
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

-- CHANGING the data type of date in layoffs_staging2 table from text to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;





SELECT CONCAT(FIRST_NAME,LAST_NAME),WORKER_TITLE,AVG(SALARY) OVER(PARTITION BY WORKER_TITLE)
FROM worker
JOIN title
	ON worker.WORKER_ID = title.WORKER_REF_ID;
    
SELECT CONCAT(FIRST_NAME,' ',LAST_NAME),WORKER_TITLE,SALARY,
SUM(SALARY) OVER(PARTITION BY WORKER_TITLE ORDER BY WORKER_ID) SUM_SALARY
FROM worker
JOIN title
	ON worker.WORKER_ID = title.WORKER_REF_ID;

SELECT WORKER_ID,CONCAT(FIRST_NAME,' ',LAST_NAME),WORKER_TITLE,SALARY,
ROW_NUMBER() OVER(PARTITION BY WORKER_TITLE ORDER BY SALARY DESC) AS row_num,
RANK() OVER(PARTITION BY WORKER_TITLE ORDER BY SALARY DESC) AS rank_num,
DENSE_RANK() OVER(PARTITION BY WORKER_TITLE ORDER BY SALARY DESC) AS dense_rank_num
FROM worker
JOIN title
	ON worker.WORKER_ID = title.WORKER_REF_ID;
    
    
-- CTE examples
WITH CTE_example as(
SELECT CONCAT(FIRST_NAME,LAST_NAME),WORKER_TITLE,SALARY
FROM worker
JOIN title
	ON worker.WORKER_ID = title.WORKER_REF_ID
),
CTE_example2 AS(
Select JOINING_DATE,SALARY FROM worker_clone
WHERE SALARY > 100000
)
SELECT * FROM CTE_example
JOIN CTE_example2 
	ON CTE_example.SALARY=CTE_example2.SALARY;
    
-- Handling NULL values
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry='';

SELECT * 
FROM layoffs_staging2
WHERE company="Bally's Interactive";

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2;


SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;
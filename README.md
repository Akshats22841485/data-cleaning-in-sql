This is the data cleaning project for the layoffs table using SQL language 
The code given was used to convert the un processed data in the layoffs file to processed data in product table file
The duplicate data is first cleared 
Then the data sheet is standerdized in which the data present was checked if there were and company,industry,country or location names which were same but they spell differently for some reason.
Then we changed the date format which was originally in text form to DATE format.
Then NULL values were handeled by selecting the NULL values of industry of same company and changing them
Then we deleted all rows with total_laid_off and percentage_laid_off ==NULL

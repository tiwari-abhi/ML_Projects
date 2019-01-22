# ADS_FALL2018
This repository contains projects for the Advanced Data Science class in Fall 2018.

**1. Data Visualization** <br />
The purpose of this assignment was to get us familiarized with the different visualizations libraries of Python like Matplotlib, Plotly, Cufflinks, etc. It also introduced the idea of using different types of plots for conveying specific types of information.

For this assignment, we worked on the Gun Violence in USA dataset that is available on https://www.kaggle.com/jameslko/gun-violence-data

The dataset was rich with time-series information and we used a lot of trend charts and line charts to plot out intresting findings.


**2. EDGAR Dataset Assigment** <br />
The EDGAR dataset is a very popular and readily available on the US government's public site at https://www.sec.gov/edgar.shtml

The assigment was divided into the following 2 parts :

Part 1 : Web Scraping
The idea behind this part of the assignment was to develop a Python script which receives a company-specific CIK and DAC number, which would then automatically scrape the SEC's web site and generate a user-friendly Excel report of all the 10Q tables. 

Part 2 : Missing Value Analysis
After scraping the data from Part 1 and generating the reports, in this part of the assignment we were asked to additionally perform missing value analysis and also generate summary reports of the missing data for each company's report.

***Highlights:*** <br />
- The final reports from both the parts would be pushed into a user's Amazon S3 storage, where they would be available for analysis
- The entire pipeline for the project was Dockerized to operate on any and every machine.

**3. Evaluation of Energy Consumption in Commercial Households from IOT Sensors** <br />
The data for this experiment is available at https://github.com/LuisM78/Appliances-energy-prediction-data

The data contains records of different appliances and their consumption in a particular household. The purpose of this assignment was to predict the energy consumption by employing feature engineering and exploratory analysis.

We had to develop a pipeline which would clean the data, handle missing values, generate custom features, try out different prediction algorithms and finally generate a report with the best possible algorithm for the job.

***Highlights:*** <br />
- We made use of automated libraries for feature selection like Boruta-Py, TPOT, Feature Tools, TSFresh

**4. Midterm Project : Freddie Mac Single Family Loan Data** <br />
This dataset contains 26 million fixed rate mortgages between Jan 1999 to Sept 2017 which can help us build more accurate credit performance models. It is publically available at http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.html

We had to develop a pipeline for this project which involved scraping the data from the website for the given time frame, this script would also carry out exploratory analysis and finally give us results.

We applied the best algorithm from the pipeline to alternative scenarios to verify their validity.

***Highlights:*** <br />
- We employed AutoML technologies like H2O and TPOT to land with results that were validated by our manual investigations
- We used feature selection strategies like Forward, Backward and Recursive Feature selection

**5. Final Project : Bike Sharing Prediction on Weather Data**<br />
For our final project, we took part in a Kaggle competetion to predict the hourly bike rental demand based on weather data from the city of Washington,DC in the USA. The data for this project is available at https://www.kaggle.com/c/bike-sharing-demand

We also developed a simple front end Flask Application which can be used by the marketing team to see the demand and developed pricing according to the demand. This application was hosted on the AWS cloud using its Elastic Beanstalk service.

***Highlights:*** <br />
- We employed GridSearchCV to get the best hyperparameters for our algorithm
- We used AutoML technologies like BigML and H2O AI to come up with best models for prediction
- We deployed the application on Azure cloud as well

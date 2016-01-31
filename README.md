# MIS698-Hadoop-Project
Expedia Recommendation using Hive

For this project, we used the hivemall library of user defined functions to create recommendations for the 
Expedia kaggle competition (https://www.kaggle.com/c/expedia-personalized-sort)

Data can be accessed from google drive:
https://drive.google.com/file/d/0B7nflSRh-DU_VTlOVzdNSVIzckU/view?usp=sharing
https://drive.google.com/file/d/0B7nflSRh-DU_RG1pWUtVOG1CM1E/view?usp=sharing


We first attempted to use logistic regression for classification, which by default predicted all bookings as false.

We then used matrix factorization to generate a list of hotels most likely to be booked by a visitor on Expedia.
# product_classification_and_etl

This repository offers product classification task and data pipeline (ETL -> Extract, Transform, Load).
#   
Underneath, there is a description of the purpose of each file.
#
1. db_connection.py

In this file, definitions for tasks such as establishing a database connection and adding entries to log files are provided. While it is not obligatory to execute this file, the functions were imported as modules in the specified files underneath.
#
2. _product_classifier.ipynb

The provided Jupyter notebook imports data from a cloud database containing information on product descriptions, brands, categories, and sub-categories. Using this data, the notebook tackles the problem of product description classification through classification algorithms. This process automates the categorization of products based on their descriptions, which is useful for managing large numbers of products in e-commerce websites. By classifying products, it becomes easier to manage inventory, improve marketing strategies, and enhance the customer experience. The notebook preprocesses the data by removing unnecessary characters, translating to the local language, and removing categories with a small number of products. It then extracts features using vectorization, trains a machine learning model, and saves the model as a pickle object (prod_cat_ml V20-12-2022.pkl). Once saved, the model can predict the label or class of a product and store it in a database for further use.
#
3. _ETL_pipeline.py

TThe Python code is linked to a database that has an ETL table called Taxonomy, used to store extracted and transformed data. The script checks for the existence of the Taxonomy table and fetches data from Invoicelines and Transaction tables filtered by the maximum id value of the Taxonomy table, which ensures incremental ETL. If the table doesn't exist, the Taxonomy table is created and data is selected from other tables for label prediction. 

The saved classification model (pickle object) is loaded to predict the label of newly added products, and the predicted labels with additional features are stored in a database for future use.






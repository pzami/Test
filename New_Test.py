#!/usr/bin/env python
# coding: utf-8

# # Introduction
# This kernel has been created by the [Information Systems Lab](http://islab.uom.gr) to introduce students of the [University of Macedonia](http://www.uom.gr/index.php?tmima=2&categorymenu=2), Greece to Machine Learning & Data Science.
# 
# ## The Instacart competition
# Instacart is an American company that operates as a same-day grocery delivery service. Customers select groceries through a web application from various retailers and delivered by a personal shopper. Instacart's service is mainly provided through a smartphone app, available on iOS and Android platforms, apart from its website.
# 
# In 2017 Instacart organised a Kaggle competition and provided to the community a sample dataset of over 3 million grocery orders from more than 200,000 Instacart users. The orders include 32 million basket items and 50,000 unique products. The objective of the competition was participants to **predict which previously purchased products will be in a user’s next order**.
# 
# ## Objective
# The objective of this Kernel is to introduce students to predictive business analytics with Python through the Instacart case. 
# 
# By the time you finish this example, you will be able to:
# * Describe the steps of creating a predictive analytics model
# * Use Python and Pandas package to manipulate data
# * Use Python and Pandas package to create, combine, and delete DataFrames
# * Use Logistic Regression to create a predictive model
# * Apply the predictive model in order to make a prediction
# * Create a submission file for the competition of Instacart
# 
# ## Problem definition
# The data that Instacart opened up include orders of 200,000 Instacart users with each user having between 4 and 100 orders. Instacart indicates each order in the data as prior, train or test. Prior orders describe the **past behaviour** of a user while train and test orders regard the **future behaviour that we need to predict**. 
# 
# As a result, we want to predict which previously purchased products (prior orders) will be in a user’s next order (train and test orders). 
# 
# For the train orders Instacart reveals the results (i.e., the ordered products) while for the test orders we do not have this piece of information. Moreover, the future order of each user can be either train or test meaning that each user will be either a train or a test user. 
# 
# The setting of the Instacart problem is described in the figure below (orders with yellow color denotes future orders of a user). 
# 
# <img src="https://i.imgur.com/S0Miw3m.png" width="350">
# 
# Each user has purchased various products during their prior orders. Moreover, for each user we know the order_id of their future order. The goal is to predict which of these products will be in a user's future order. 
# 
# This is a **classification problem** because we need to predict whether each pair of user and product is a reorder or not. This is indicated by the value of the reordered variable, i.e. reordered=1 or reordered=0 (see figure below). 
# 
# <img src="https://i.imgur.com/SxK2gsR.png" width="350">
# 
# As a result we need to come up and calculate various **predictor variables (X)** that will describe the characteristics of a product and the behaviour of a user regarding one or multiple products. We will do so by analysing the prior orders of the dataset. We will then use the train users to create a predictive model and the test users to make our actual prediction. As a result we create a table as the following one and we train an algorithm based on predictor variables (X) and response variable (Y).
# 
# <img src="https://i.imgur.com/Yb1CKAF.png" width="600">
# 
# ## Method
# Our method includes the following steps:
# 1. <b>Import and reshape data</b>: This step includes loading CSV files into pandas DataFrames, tranform character variables to categorical variables, and create a supportive table.
# 2. <b>Create predictor variables</b>: This step includes identifying and calculating predictor variables (aka features) from the initial datasets provided by Instacart. 
# 3. <b>Create train and test DataFrames</b>: In this step we create two distinct pandas DataFrames that will be used in the creation and the use of the predictive model.
# 4. <b>Create predictive model (fit)</b>: In this step we employ Logistic Regression to create the predictive model through the train dataset.
# 5. <b>Apply predictive model (predict)</b>: This step includes applying the model to predict the 'reordered' variable for the test dataset.
# 6. <b>Create submission file</b>: In this final step we create the submission file with our predictions for Instacart's competition.
# 7. <b>Get F1 score</b>: In this step we submit the produced and file and get the F1 score describing the accuracy of our prediction model.

# # 1. Import and Reshape Data 
# First we load the necessary Python packages and then we import the CSV files that were provided by Instacart.
# 
# ## 1.1 Import the required packages
# The garbage collector (package gc), attempts to reclaim garbage, or memory occupied by objects (e.g., DataFrames) that are no longer in use by Python ([ref1](https://www.techopedia.com/definition/1083/garbage-collection-gc-general-programming), [ref2](https://en.wikipedia.org/wiki/Garbage_collection_(computer_science)). This package will eliminate our risk to exceed the 16GB threshold of available RAM that Kaggle offers.
# 
# The **"as"** reserved word is to define an alias to the package. The alias help us to call easier a package in our code.

# In[1]:


from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi({"username":"pzamichos","key":"11d1dd53800b96878b228af465dad635"})
api.authenticate()
files = api.competition_download_files("Instacart-Market-Basket-Analysis")
import zipfile
with zipfile.ZipFile('Instacart-Market-Basket-Analysis.zip', 'r') as zip_ref:
    zip_ref.extractall('instacart')


# In[14]:


import os
working_directory = '.'
os.chdir(working_directory)
for file in os.listdir(working_directory):   # get the list of files
    if zipfile.is_zipfile(file): # if it is a zipfile, extract it
        with zipfile.ZipFile(file) as item: # treat the file as a zip
           item.extractall()  # extract it in the working directory


# In[16]:


# For data manipulation
import pandas as pd              

# Garbage Collector to free up memory
import gc                         
gc.enable()                       # Activate 


# ## 1.2 Load data from the CSV files
# Instacart provides 6 CSV files, which we have to load into Python. Towards this end, we use the .read_csv() function, which is included in the Pandas package. Reading in data with the .read_csv( ) function returns a DataFrame.

# In[17]:


orders = pd.read_csv('orders.csv')
order_products_train = pd.read_csv('order_products__train.csv')
order_products_prior = pd.read_csv('order_products__prior.csv')
products = pd.read_csv('products.csv')
aisles = pd.read_csv('aisles.csv')
departments = pd.read_csv('departments.csv')


# This step results in the following DataFrames:
# * <b>orders</b>: This table includes all orders, namely prior, train, and test. It has single primary key (<b>order_id</b>).
# * <b>order_products_train</b>: This table includes training orders. It has a composite primary key (<b>order_id and product_id</b>) and indicates whether a product in an order is a reorder or not (through the reordered variable).
# * <b>order_products_prior </b>: This table includes prior orders. It has a composite primary key (<b>order_id and product_id</b>) and indicates whether a product in an order is a reorder or not (through the reordered variable).
# * <b>products</b>: This table includes all products. It has a single primary key (<b>product_id</b>)
# * <b>aisles</b>: This table includes all aisles. It has a single primary key (<b>aisle_id</b>)
# * <b>departments</b>: This table includes all departments. It has a single primary key (<b>department_id</b>)

# If you want to reduce the execution time of this Kernel you can use the following piece of code by uncomment it. This will trim the orders DataFrame and will keep a 10% random sample of the users. You can use this for experimentation.

# In[11]:


'''
#### Remove triple quotes to trim your dataset and experiment with your data
### COMMANDS FOR CODING TESTING - Get 10% of users 
orders = orders.loc[orders.user_id.isin(orders.user_id.drop_duplicates().sample(frac=0.1, random_state=25))] 
'''


# We now use the .head( ) method in order to visualise the first 10 rows of these tables. Click the Output button below to see the tables.

# In[18]:


orders.head()


# In[13]:


order_products_train.head()


# In[14]:


order_products_prior.head()


# In[15]:


products.head()


# In[16]:


aisles.head()


# In[17]:


departments.head()


# ## 1.3 Reshape data
# We transform the data in order to facilitate their further analysis. First, we convert character variables into categories so we can use them in the creation of the model. In Python, a categorical variable is called category and has a fixed number of different values.

# In[18]:


# We convert character variables into category. 
# In Python, a categorical variable is called category and has a fixed number of different values
aisles['aisle'] = aisles['aisle'].astype('category')
departments['department'] = departments['department'].astype('category')
orders['eval_set'] = orders['eval_set'].astype('category')
products['product_name'] = products['product_name'].astype('category')


# ## 1.4 Create a DataFrame with the orders and the products that have been purchased on prior orders (op)
# We create a new DataFrame, named <b>op</b> which combines (merges) the DataFrames <b>orders</b> and <b>order_products_prior</b>. Bear in mind that <b>order_products_prior</b> DataFrame includes only prior orders, so the new DataFrame <b>op</b>  will contain only these observations as well. Towards this end, we use pandas' merge function with how='inner' argument, which returns records that have matching values in both DataFrames. <img src="https://i.imgur.com/zEK7FpY.jpg" width="400">

# In[19]:


#Merge the orders DF with order_products_prior by their order_id, keep only these rows with order_id that they are appear on both DFs
op = orders.merge(order_products_prior, on='order_id', how='inner')
op.head()


# The table contains for all the customers **(user_id)**: <br>
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ➡︎ the orders **(order_id)** that they have placed accompanied with: <br>
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ➡︎ the products **(product_id)** that have been bought in each order

# # 2. Create Predictor Variables
# We are now ready to identify and calculate predictor variables based on the provided data. We can create various types of predictors such as:
# * <b>User predictors</b> describing the behavior of a user e.g. total number of orders of a user.
# * <b>Product predictors</b> describing characteristics of a product e.g. total number of times a product has been purchased.
# * <b>User & product predictors</b> describing the behavior of a user towards a specific product e.g. total times a user ordered a specific product.

# ## 2.1 Create user predictors
# 
# 
# ### 2.1.1 Number of orders per customer 📚📝
# We calculate the total number of placed orders per customer. We create a **user** DataFrame to store the results.

# In[20]:


## First approach in one step:
# Create distinct groups for each user, identify the highest order number in each group, save the new column to a DataFrame
user = op.groupby('user_id')['order_number'].max().to_frame('user_t_orders') #
user.head()

## Second approach in two steps: 
#1. Save the result as DataFrame with Double brackets --> [[ ]] 
#user = op.groupby('user_id')[['order_number']].max()
#2. Rename the label of the column
#user.columns = ['user_t_orders']
#user.head()


# In[21]:


# Reset the index of the DF so to bring user_id from index to column (pre-requisite for step 2.4)
user = user.reset_index()
user.head()


# ## 2.2 Create product predictors
#  
# ### 2.2.1 Number of purchases for each product 📚📝
# We calculate the total number of purchases for each product (from all customers). We create a **prd** DataFrame to store the results.

# In[22]:


# Create distinct groups for each product, count the orders, save the result for each product to a new DataFrame  
prd = op.groupby('product_id')['order_id'].count().to_frame('prd_t_purchases') #
prd.head()


# In[23]:


# Reset the index of the DF so to bring product_id rom index to column (pre-requisite for step 2.4)
prd = prd.reset_index()
prd.head()


# ## 2.3 Create user-product predictors
# 
# 
# ### 2.3.1 How many times a user bought a product 📚📝
# We create different groups that contain all the rows for each combination of user and product. Then with the aggregation function .count( ) we get how many times each user bought a product. We save the results on new **uxp** DataFrame.

# In[24]:


# Create distinct groups for each combination of user and product, count orders, save the result for each user X product to a new DataFrame 
uxp = op.groupby(['user_id', 'product_id'])['order_id'].count().to_frame('uxp_t_bought') #
uxp.head()


# In[25]:


# Reset the index of the DF so to bring user_id & product_id rom indices to columns (pre-requisite for step 2.4)
uxp = uxp.reset_index()
uxp.head()


# ## 2.4 Merge all features
# We now merge the DataFrames with the three types of predictors that we have created (i.e., for the users, the products and the combinations of users and products).
# 
# We will start from the **uxp** DataFrame and we will add the user and prd DataFrames. We do so because we want our final DataFrame (which will be called **data**) to have the following structure: 
# 
# <img style="float: left;" src="https://i.imgur.com/mI5BbFE.jpg" >
# 
# 
# 
# 
# 

# ### 2.4.1 Merge uxp with user DataFrame
# Here we select to perform a left join of uxp with user DataFrame based on matching key "user_id"
# 
# <img src="https://i.imgur.com/WlI84Ud.jpg" width="400">
# 
# Left join, ensures that the new DataFrame will have:
# - all the observations of the uxp (combination of user and products) DataFrame 
# - all the **matching** observations of user DataFrame with uxp based on matching key **"user_id"**
# 
# The new DataFrame as we have already mentioned, will be called **data**.

# In[26]:


#Merge uxp features with the user features
#Store the results on a new DataFrame
data = uxp.merge(user, on='user_id', how='left')
data.head()


# ### 2.4.1 Merge data with prd DataFrame 📚📝
# In this step we continue with our new DataFrame **data** and we perform a left join with prd DataFrame. The matching key here is the "product_id".
# <img src="https://i.imgur.com/Iak6nIz.jpg" width="400">
# 
# Left join, ensures that the new DataFrame will have:
# - all the observations of the data (features of userXproducts and users) DataFrame 
# - all the **matching** observations of prd DataFrame with data based on matching key **"product_id"**

# In[27]:


#Merge uxp & user features (the new DataFrame) with prd features
data = data.merge(prd, on='product_id', how='left') #
data.head()


# ### 2.4.2 Delete previous DataFrames

# The information from the DataFrames that we have created to store our features (op, user, prd, uxp) is now stored on **data**. 
# 
# As we won't use them anymore, we now delete them.

# In[28]:


del op, user, prd, uxp
gc.collect()


# # 3. Create train and test DataFrames
# ## 3.1 Include information about the last order of each user
# 
# The **data** DataFrame that we have created on the previous chapter (2.4) should include two more columns which define the type of user (train or test) and the order_id of the future order.
# This information can be found on the initial orders DataFrame which was provided by Instacart: 
# 
# <img style="float: left;" src="https://i.imgur.com/jbatzRY.jpg" >
# 
# 
# Towards this end:
# 1. We select the **orders** DataFrame to keep only the future orders (labeled as "train" & "test). 
# 2. Keep only the columns of our desire ['eval_set', 'order_id'] <span style="color:red">**AND** </span> 'user_id' as is the matching key with our **data** DataFrame
# 2. Merge **data** DataFrame with the information for the future order of each customer using as matching key the 'user_id'

# To filter and select the columns of our desire on orders (the 2 first steps) there are numerous approaches:

# In[29]:


## First approach:
# In two steps keep only the future orders from all customers: train & test 
orders_future = orders[((orders.eval_set=='train') | (orders.eval_set=='test'))]
orders_future = orders_future[ ['user_id', 'eval_set', 'order_id'] ]
orders_future.head(10)

## Second approach (if you want to test it you have to re-run the notebook):
# In one step keep only the future orders from all customers: train & test 
#orders_future = orders.loc[((orders.eval_set=='train') | (orders.eval_set=='test')), ['user_id', 'eval_set', 'order_id'] ]
#orders_future.head(10)

## Third approach (if you want to test it you have to re-run the notebook):
# In one step exclude all the prior orders so to deal with the future orders from all customers
#orders_future = orders.loc[orders.eval_set!='prior', ['user_id', 'eval_set', 'order_id'] ]
#orders_future.head(10)


# Now to fulfill step 3, we merge on **data** DataFrame the information for the last order of each customer. The matching key here is the user_id and we select a left join as we want to keep all the observations from **data** DataFrame.
# 
# <img src="https://i.imgur.com/m3pNVDW.jpg" width="400">

# In[30]:


# bring the info of the future orders to data DF
data = data.merge(orders_future, on='user_id', how='left')
data.head(10)


# ## 3.2 Prepare the train DataFrame 📚📝
# In order to prepare the train Dataset, which will be used to create our prediction model, we need to include also the response (Y) and thus have the following structure:
# 
# <img style="float: left;" src="https://i.imgur.com/PDu2vfR.jpg" >
# 
# Towards this end:
# 1. We keep only the customers who are labelled as "train" from the competition
# 2. For these customers we get from order_products_train the products that they have bought, in order to create the response variable (reordered:1 or 0)
# 3. We make all the required manipulations on that dataset and we remove the columns that are not predictors
# 
# So now we filter the **data** DataFrame so to keep only the train users:

# In[31]:


#Keep only the customers who we know what they bought in their future order
data_train = data[data.eval_set=='train'] #
data_train.head()


# For these customers we get from order_products_train the products that they have bought. The matching keys are here two: the "product_id" & "order_id". A left join keeps all the observations from data_train DataFrame
# 
# <img src="https://i.imgur.com/kndys9d.jpg" width="400">

# In[32]:


#Get from order_products_train all the products that the train users bought bought in their future order
data_train = data_train.merge(order_products_train[['product_id','order_id', 'reordered']], on=['product_id','order_id'], how='left' )
data_train.head(15)


# On the last columm (reordered) you can find out our response (y). 
# There are combinations of User X Product which they were reordered (1) on last order where other were not (NaN value).
# 
# Now we manipulate the data_train DataFrame, to bring it into a structure for Machine Learning (X1,X2,....,Xn, y):
# - Fill NaN values with value zero (regards reordered rows without value = 1)
# - Set as index the column(s) that describe uniquely each row (in our case "user_id" & "product_id")
# - Remove columns which are not predictors (in our case: 'eval_set','order_id')

# In[33]:


#Where the previous merge, left a NaN value on reordered column means that the customers they haven't bought the product. We change the value on them to 0.
data_train['reordered'] = data_train['reordered'].fillna(0)
data_train.head(15)


# In[34]:


#We set user_id and product_id as the index of the DF
data_train = data_train.set_index(['user_id', 'product_id'])
data_train.head(15)


# In[35]:


#We remove all non-predictor variables
data_train = data_train.drop(['eval_set', 'order_id'], axis=1)
data_train.head(15)


# ## 3.3 Prepare the test DataFrame 📚📝
# The test DataFrame must have the same structure as the train DataFrame, excluding the "reordered" column (as it is the label that we want to predict).
# <img style="float: left;" src="https://i.imgur.com/lLJ7wpA.jpg" >
# 
#  To achieve this we:
# - Keep only the customers who are labelled as test
# - Set as index the column(s) that uniquely describe each row (in our case "user_id" & "product_id")
# - Remove the columns that are predictors (in our case:'eval_set', 'order_id')

# In[36]:


#Keep only the future orders from customers who are labelled as test
data_test = data[data.eval_set=='test'] #
data_test.head()


# In[37]:


#We set user_id and product_id as the index of the DF
data_test = data_test.set_index(['user_id', 'product_id']) #
data_test.head()


# In[38]:


#We remove all non-predictor variables
data_test = data_test.drop(['eval_set','order_id'], axis=1)
#Check if the data_test DF, has the same number of columns as the data_train DF, excluding the response variable
data_test.head()


# # 4. Create predictive model (fit)
# The Machine Learning model that we are going to create is based on the Logistic Regression Algorithm.
# 
# From Scikit-learn package we import the LogisticRegression estimator.
# 
# To create the predictive model we:
# 1. We create a DataFrame with all the predictors, named **X_train** and a Series with the response, named **y_train**
# 2. We initiata a Logistic regression model with a specific random_state (so we can reproduce if we want to our model).
# 3. Finally we train our model with the X_train and y_train data.

# In[39]:


########################
#IMPORT REQUIRED PACKAGES
#######################
from sklearn.linear_model import LogisticRegression
#from sklearn.model_selection import train_test_split #validate algorithm
#from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  #validate algorithm

#####################
#CREATE X_train, y_train
#####################
X_train, y_train = data_train.drop('reordered', axis=1), data_train.reordered
#X_train, X_val, y_train, y_val = train_test_split(data_train.drop('reordered', axis=1), data_train.reordered, test_size=0.8, random_state=42)
  #validate algorithm

#####################
# INITIATE AND TRAIN MODEL
#####################
log = LogisticRegression(random_state=42)
model = log.fit(X_train, y_train)

#####################
# SCORE MODEL
#####################
#log.score(X_val, y_val) # validate algorithm

#####################
## DELETE TEMPORARY OBJECTS 
#####################
del [X_train, y_train]
# del [X_val, y_val] # remove validate algorithm objects
gc.collect()

### TO REMOVE THE FUTURE WARNING THAT YOU SEE BELOW: Add the argument solver='lbfgs' to LogisticRegression( ) function


# # 5. Apply predictive model (predict)
# The model that we have created is stored in the **model** object.
# At this step we predict the values for the test data and we store them in a new column in the same DataFrame.

# In[40]:


# Predict values for test data with our model from chapter 5 - the results are saved as a Python array
test_pred = model.predict(data_test).astype(int)
test_pred[0:20] #display the first 20 predictions of the numpy array


# In[41]:


## OR set a custom threshold (in this problem, 0.21 yields the best prediction)
test_pred = (model.predict_proba(data_test)[:,1] >= 0.21).astype(int)
test_pred[0:20] #display the first 20 predictions of the numpy array


# In[42]:


#Save the prediction in a new column in the data_test DF
data_test['prediction'] = test_pred
data_test.head()


# In[43]:


#Reset the index
final = data_test.reset_index()
#Keep only the required columns to create our submission file (Chapter 6)
final = final[['product_id', 'user_id', 'prediction']]

gc.collect()
final.head()


# # 6. Creation of Submission File
# To submit our prediction to Instacart competition we have to get for each user_id (test users) their last order_id. The final submission file should have the test order numbers and the products that we predict that are going to be bought.
# 
# To create this file we retrieve from orders DataFrame all the test orders with their matching user_id:

# In[44]:


orders_test = orders.loc[orders.eval_set=='test',("user_id", "order_id") ]
orders_test.head()


# We merge it with our predictions (from chapter 5) using a left join:
# <img src="https://i.imgur.com/KJubu0v.jpg" width="400">

# In[45]:


final = final.merge(orders_test, on='user_id', how='left')
final.head()


# And we move on with two final manipulations:
# - remove any undesired column (in our case user_id)
# - set product_id column as integer (mandatory action to proceed to the next step)

# In[46]:


#remove user_id column
final = final.drop('user_id', axis=1)
#convert product_id as integer
final['product_id'] = final.product_id.astype(int)

#Remove all unnecessary objects
del orders
del orders_test
gc.collect()

final.head()


# In this step we initiate an empty dictionary. In this dictionary we will place as index the order_id and as values all the products that the order will have. If none product will be purchased, we have explicitly to place the string "None". All this syntax follows the requirements of the competition for the submission file.

# In[47]:


d = dict()
for row in final.itertuples():
    if row.prediction== 1:
        try:
            d[row.order_id] += ' ' + str(row.product_id)
        except:
            d[row.order_id] = str(row.product_id)

for order in final.order_id:
    if order not in d:
        d[order] = 'None'
        
gc.collect()

#We now check how the dictionary were populated (open hidden output)
d


# Now we convert the dictionary to a DataFrame and prepare it to extact it into a .csv file

# In[48]:


#Convert the dictionary into a DataFrame
sub = pd.DataFrame.from_dict(d, orient='index')

#Reset index
sub.reset_index(inplace=True)
#Set column names
sub.columns = ['order_id', 'products']

sub.head()


# **The submission file should have 75.000 predictions to be submitted in the competition**

# In[49]:


#Check if sub file has 75000 predictions
sub.shape[0]


# The DataFrame can now be converted to .csv file. Pandas can export a DataFrame to a .csv file with the .to_csv( ) function.

# In[50]:


sub.to_csv('sub.csv', index=False)
api.competition_submit('sub.csv','','Instacart-Market-Basket-Analysis')


# # 7. Get F1 Score

# Before you are ready to submit your prediction to the competion, **ensure that**:
# - **You have used all of the offered data and not the 10% that was defined as an optional step on section 1.2**
# 
# To submit your prediction and get the F1 score you have to:
# 1. Commit this notebook and wait for the results 
# 2. Go to view mode (where you see your notebook but you can't edit it)
# 3. Click on the data section from your left panel
# 4. Find the sub.csv (on outputs), below the section with the data from Instacart
# 5. Click on "Submit to competition" button
# 
# Regarding step 1:
# >This step might take long. If it exceeds 20-30 minutes it would be wise to check your code again. Kaggle won't inform you during commit if the notebook has:
# - syntax errors
# - if it exceeds 16 GB RAM
# - if it takes an algorirthms too much to train or predict
# 
# >Any new commit:
# - can't take more than 9 hours
# - doesn't stop if it exceeds the 16 GB RAM - you will just receive an error of unsuccesful commit after 9 hours

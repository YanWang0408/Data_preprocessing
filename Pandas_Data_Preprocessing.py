# Importing the libraries
import numpy as np
import pandas as pd

# Importing the dataset
dataset = pd.read_csv('Data.csv')
dataset.head()

# Missing Values
#dataset.isnull().any() #check for null values
#dataset.dropna(inplace = True) #drop null values
#dataset["bmi"].fillna((dataset["bmi"].mean()), inplace=True) #medium() or mode()
from sklearn.preprocessing import Imputer
imputer = Imputer(missing_values = "NaN", strategy = "mean", axis = 0) 
imputer = imputer.fit(X[:,1:3]) #[row selection, column selection]
dataset[:, 1:3] = imputer.transform(dataset[:, 1:3])

# Encoding categorical data
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder = LabelEncoder()
dataset[:, 3] = labelencoder.fit_transform(dataset[:, 3])
onehotencoder = OneHotEncoder(categorical_features = [3])
dataset = onehotencoder.fit_transform(dataset).toarray()

dataset_dummy = pd.get_dummies(dataset["region"])
newdata = pd.concat([dataset, dataset_dummy], axis=1)
newdata.drop(["region"], axis=1)

# Splitting the data into independent and dependent variable
X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, 3].values

# Splitting the dataset into the Training set and Test set
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

# Feature Scaling
from sklearn.preprocessing import StandardScaler
sc_X = StandardScaler()
X_train = sc_X.fit_transform(X_train)
X_test = sc_X.transform(X_test)
sc_y = StandardScaler()
y_train = sc_y.fit_transform(y_train)


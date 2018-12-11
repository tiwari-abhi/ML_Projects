import pandas as pd
import numpy as np
import warnings
import boto
import boto3
import os
import sys
import time
import datetime

from boto.s3.key import Key
from datetime import datetime as dt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import r2_score, mean_squared_log_error
from sklearn.neural_network import MLPRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR

global scaler
scaler = MinMaxScaler(feature_range=(0, 1))

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore")

def get_data():
    train = pd.read_csv('https://s3.us-east-2.amazonaws.com/final-project-dataset/train.csv')
    test = pd.read_csv('https://s3.us-east-2.amazonaws.com/final-project-dataset/test.csv')
    return train, test

def preprocessing_data(train, test):
    global categoricalFeatureNames, numericalFeatureNames

    # Combining Test and Train Frames
    data = train.append(test)
    data.reset_index(inplace=True)
    data.drop('index', inplace=True, axis=1)

    # Deriving Time Series Columns from datetime field
    data["date"] = data.datetime.apply(lambda x: x.split()[0])
    data["hour"] = data.datetime.apply(lambda x: x.split()[1].split(":")[0]).astype("int")
    data["year"] = data.datetime.apply(lambda x: x.split()[0].split("-")[0])
    data["weekday"] = data.date.apply(lambda dateString: dt.strptime(dateString, "%Y-%m-%d").weekday())
    data["month"] = data.date.apply(lambda dateString: dt.strptime(dateString, "%Y-%m-%d").month)

    # Predicting Missing Wind Values using RF Regressor
    dataWind0 = data[data["windspeed"] == 0]
    dataWindNot0 = data[data["windspeed"] != 0]
    rfModel_wind = RandomForestRegressor()
    windColumns = ["season", "weather", "humidity", "month", "temp", "year", "atemp"]
    rfModel_wind.fit(dataWindNot0[windColumns], dataWindNot0["windspeed"])
    wind0Values = rfModel_wind.predict(X=dataWind0[windColumns])
    dataWind0["windspeed"] = wind0Values
    data = dataWindNot0.append(dataWind0)
    data.reset_index(inplace=True)
    data.drop('index', inplace=True, axis=1)

    # Designating Categorical Features from numeric columns
    categoricalFeatureNames = ["season", "holiday", "workingday", "weather", "weekday", "month", "year", "hour"]
    numericalFeatureNames = ["atemp", "humidity", "windspeed"]
    dropFeatures = ["casual", "datetime", "date", "registered", "temp"]

    # for var in categoricalFeatureNames:
    #     data[var] = data[var].astype("category")

    dataTrain = data[pd.notnull(data['count'])].sort_values(by=["datetime"])
    dataTest = data[~pd.notnull(data['count'])].sort_values(by=["datetime"])

    dataTrain = dataTrain.drop(dropFeatures, axis=1)
    dataTest = dataTest.drop(dropFeatures, axis=1)
    dataTest = dataTest.drop('count', axis=1)

    return dataTrain, dataTest

error_metric1 = pd.DataFrame({'Training RMSLE': [],'Training R^2': [],'Testing RMSLE':[], 'Testing R^2':[]})
error_metric2 = pd.DataFrame({'Training RMSLE': [],'Training R^2': [],'Testing RMSLE':[], 'Testing R^2':[]})


def scaled_model_stats(model, model_name, X_train, Y_train, X_test, Y_test):
    global error_metric1

    ytr = np.array(Y_train).reshape(len(Y_train), 1)
    yte = np.array(Y_test).reshape(len(Y_test), 1)

    Y_train_scaled = scaler.fit_transform(ytr).ravel()
    Y_test_scaled = scaler.fit_transform(yte).ravel()

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.fit_transform(X_test)

    train_data_predictions = model.predict(X_train_scaled)
    test_data_predictions = model.predict(X_test_scaled)

    trdp = np.array(train_data_predictions).reshape(len(train_data_predictions), 1)
    trdp = scaler.fit_transform(trdp).ravel()

    tedp = np.array(test_data_predictions).reshape(len(test_data_predictions), 1)
    tedp = scaler.fit_transform(tedp).ravel()

    # RMSLE
    model_rmsle_train = np.sqrt(mean_squared_log_error(Y_train_scaled, trdp))
    model_rmsle_test = np.sqrt(mean_squared_log_error(Y_test_scaled, tedp))

    # R-Squared
    model_r2_train = r2_score(Y_train_scaled, trdp)
    model_r2_test = r2_score(Y_test_scaled, tedp)

    df_local = pd.DataFrame({'Model': [model_name],
                             'Training RMSLE': [model_rmsle_train],
                             'Training R^2': [model_r2_train],
                             'Testing RMSLE':[model_rmsle_test],
                             'Testing R^2' : [model_r2_test]})

    error_metric1 = pd.concat([error_metric1, df_local], sort=True)

def model_stats(model, model_name, X_train, Y_train, X_test, Y_test):
    global error_metric2
    train_data_predictions = model.predict(X_train)
    test_data_predictions = model.predict(X_test)

    # RMSLE
    model_rmsle_train = np.sqrt(mean_squared_log_error(Y_train, train_data_predictions))
    model_rmsle_test = np.sqrt(mean_squared_log_error(Y_test, test_data_predictions))

    # R-Squared
    model_r2_train = r2_score(Y_train, train_data_predictions)
    model_r2_test = r2_score(Y_test, test_data_predictions)

    df_local = pd.DataFrame({'Model': [model_name],
                             'Training RMSLE': [model_rmsle_train],
                             'Training R^2': [model_r2_train],
                             'Testing RMSLE':[model_rmsle_test],
                             'Testing R^2' : [model_r2_test]})

    error_metric2 = pd.concat([error_metric2, df_local], sort=True)


def upload_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, inputLocation, filepath):
    try:
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        print("Connected to S3")
    except:
        print("Amazon keys are invalid!!")
        exit()

    loc = ''

    if inputLocation == 'APNortheast':
        loc = boto.s3.connection.Location.APNortheast
    elif inputLocation == 'APSoutheast':
        loc = boto.s3.connection.Location.APSoutheast
    elif inputLocation == 'APSoutheast2':
        loc = boto.s3.connection.Location.APSoutheast2
    elif inputLocation == 'CNNorth1':
        loc = boto.s3.connection.Location.CNNorth1
    elif inputLocation == 'EUCentral1':
        loc = boto.s3.connection.Location.EUCentral1
    elif inputLocation == 'EU':
        loc = boto.s3.connection.Location.EU
    elif inputLocation == 'SAEast':
        loc = boto.s3.connection.Location.SAEast
    elif inputLocation == 'USWest':
        loc = boto.s3.connection.Location.USWest
    elif inputLocation == 'USWest2':
        loc = boto.s3.connection.Location.USWest2

    try:
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts)
        bucket_name = 'finalproject' + str(st).replace(" ", "").replace("-", "").replace(":", "").replace(".", "")
        bucket = conn.create_bucket(bucket_name, location=loc)

        print("bucket created")
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        print('s3 client created')
        s3.upload_file(filepath, bucket_name, file_name)

        print("File successfully uploaded to S3", file_name, bucket)

    except:
        print("Amazon keys are invalid!!")
        exit()


def main():
    global df_train, df_test, X_train, X_test, Y_train, processed_train, processed_test, file_name

    args = sys.argv[1:]
    inputlocation = ''
    AWS_ACCESS_KEY = ''
    AWS_SECRET_KEY = ''
    count = 0

    if len(args) == 0:
        print('No Command Line Parameters Passed!')
        exit(0)

    for arg in args:
        if count == 0:
            inputlocation = str(arg)
        elif count == 1:
            AWS_ACCESS_KEY = str(arg)
        elif count == 2:
            AWS_SECRET_KEY = str(arg)
        count += 1

    df_train, df_test = get_data()

    processed_train, processed_test = preprocessing_data(df_train, df_test)

    X = processed_train.drop('count', axis=1)
    Y = processed_train['count']

    #Since data is timeseries hence we MUST keep the shuffle parameter as FALSE !!
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.30, shuffle=False)

    print('Trying out MLP Regression...')
    # Neural Network Model
    mlpr = MLPRegressor()
    mlpr.fit(X_train,Y_train)
    scaled_model_stats(mlpr,'Neural Network',X_train,Y_train,X_test,Y_test)
    print('MLP Regression...Done')
    print('\n')

    print('Trying out Linear Regression...')
    # Linear Regression Model
    lr = LinearRegression()
    lr.fit(X_train, Y_train)
    scaled_model_stats(lr, 'Linear Regression', X_train, Y_train, X_test, Y_test)
    print('Linear Regression...Done')
    print('\n')

    print('Trying out Lasso Regression...')
    # Lasso Regressor Model
    lass = Lasso()
    lass.fit(X_train, Y_train)
    scaled_model_stats(lass, 'Lasso Regression', X_train, Y_train, X_test, Y_test)
    print('Lasso Regression...Done')
    print('\n')

    print('Trying out Ridge Regression...')
    # Ridge Regression Model
    ridge = Ridge()
    ridge.fit(X_train, Y_train)
    scaled_model_stats(ridge, 'Ridge Regression', X_train, Y_train, X_test, Y_test)
    print('Ridge Regression...Done')
    print('\n')

    print('Trying out Gradient Boosting Regression...')
    # Gradient Boosting Regressor Model
    gb = GradientBoostingRegressor()
    gb.fit(X_train, Y_train)
    scaled_model_stats(gb, 'Gradient Boosting Regressor', X_train, Y_train, X_test, Y_test)
    print('Gradient Boosting Regression...Done')
    print('\n')

    print('Trying out Support Vector Regression...')
    # Support Vector Regressor Model
    svr = SVR()
    svr.fit(X_train, Y_train)
    model_stats(svr, 'Support Vector Regressor', X_train, Y_train, X_test, Y_test)
    print('Support Vector Regression...Done')
    print('\n')

    print('Trying out Random Forrest Regression...')
    # Random Forrest Model
    rf = RandomForestRegressor()
    rf.fit(X_train, Y_train)
    model_stats(rf, 'Random Forrest Regressor', X_train, Y_train, X_test, Y_test)
    print('Random Forrest Regression...Done')
    print('\n')

    file_name = 'Evaluation of Models.csv'
    final_df = pd.concat([error_metric1,error_metric2],sort=True)
    final_df.reset_index().drop('index', axis=1).to_csv(file_name)

    upload_to_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY, inputlocation, os.getcwd() + '/' + file_name)

if __name__ == '__main__':
    main()
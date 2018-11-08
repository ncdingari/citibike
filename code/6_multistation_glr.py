#!/usr/bin/env python
# coding: utf-8

# # Configure Notebook

# In[1]:


#from IPython.core.interactiveshell import InteractiveShell
#InteractiveShell.ast_node_interactivity = "all"

import numpy as np
import pandas as pd
import matplotlib.cm
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr

import matplotlib.lines as mlines
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import time
import json



import findspark
findspark.init()


import pyspark


from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, udf, to_timestamp, date_trunc, stddev_pop, mean
from pyspark.sql.functions import year, month, hour, dayofweek
from pyspark.sql.functions import round, concat, col, lit
from pyspark.sql.functions import log1p

from pyspark.sql.types import FloatType, StructType, IntegerType, StringType, DoubleType, StructField, TimestampType, DateType
from pyspark.sql.types import TimestampType

from pyspark.ml.feature import VectorAssembler, OneHotEncoderEstimator
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, GeneralizedLinearRegression
from pyspark.ml.feature import StringIndexer, VectorIndexer, Normalizer
from pyspark.ml.evaluation import RegressionEvaluator

import random

#spark.sparkContext._conf.getAll()
#conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.app.name', 'SingleStationRF'), ('spark.executor.cores', '3'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
#park.sparkContext.stop()
#spark = SparkSession.builder.config(conf=conf).getOrCreate()

builder = SparkSession.builder
builder = builder.config("spark.executor.memory", "4G")
builder = builder.config("spark.executor.cores", "3")
builder = builder.config("spark.driver.memory", "10G")
builder = builder.config("spark.driver.maxResultSize", "5G")
spark1 = builder.appName("SingleStationRF").getOrCreate()


import datetime as dt
print("modules imported")

randomSeed = 1984

pathWeather = "/users/sajudson/Dropbox/WPI/DS504/project/citibike/weather/"
pathData = "/users/sajudson/Dropbox/WPI/DS504/project/citibike/data/"
pathFigure = "/users/sajudson/Dropbox/WPI/DS504/project/citibike/figures/"

#load expriment dictionary
with open(pathFigure+"experiments.json","r") as f:
    experiments = json.load(f)

file_type = "csv"

plt.style.use('ggplot')


# # Load Sample Station Data

#SELECT WHICH CITY TO USE FOR ANALYSIS
city = "NYC"
#city = "JC"

tripSampleFraction = .20
stationSampleFraction = .20

sample = str(int(tripSampleFraction*100))


t0= time.time()

filenameBF = "citibike"+city+"bf3_sample_"+sample
filenameOutput = filenameBF+".csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

sampleStationDataFeatureSchema = StructType([StructField('datetime', TimestampType(), False),
                              StructField('station_id', IntegerType(), False),
                              StructField('totalDemand', IntegerType(), False),
                              StructField('totalSupply', IntegerType(), False)
                              ])

# The applied options are for CSV files. For other file types, these will be ignored.
sampleStationData = spark1.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .schema(sampleStationDataFeatureSchema)   .load(pathData+filenameOutput)

print(time.time()-t0)
# this operation takes ~0.27seconds


# In[3]:


# for testing and debugging only
#t0= time.time()
#sampleStationData.describe().show()
#sampleStationData.show()
#print(time.time()-t0)

# this operation takes ~3.4 seconds


# # Load Weather Features Data Set
#

# In[4]:


weatherFeatures = "NYC"+'weatherFeatures'
weather_file_type = 'csv'
weatherFilename = weatherFeatures + "."+weather_file_type

weatherFeatureSchema = StructType([StructField('temp', DoubleType(), False),
                            StructField('humidity', DoubleType(), True),
                            StructField('total_precip', DoubleType(), True),
                            StructField('cloud_cover', DoubleType(), True),
                            StructField('datetime', TimestampType(), True)
                           ])


# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
weatherFeatures = spark1.read.format(weather_file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .schema(weatherFeatureSchema)   .load(pathWeather+weatherFilename)

# for testing and debugging only
#weatherFeatures.describe().show()


# # Generate List of Stations included in Sample Station dataset

# In[5]:


sampleStationList = sampleStationData.select("station_id").distinct().orderBy("station_id").rdd.map(lambda row : row[0]).collect()

# for testing and debugging only
print(sampleStationList)
#Expected Output
#[3183, 3184, 3188, 3193, 3199, 3200, 3207, 3209, 3213, 3214, 3220, 3225, 3281, 3483]


# In[6]:


#inspect sample station list data set

inspectStationSample = False
if inspectStationSample == True:

    stationRecordAverage =0
    stationRecordMax =0
    stationRecordMin =999999999
    stationRecordCount = []
    stationCount = len(sampleStationList)
    for i in  range(0,stationCount):
        s = sampleStationList[i]
        sdf = sampleStationData.filter(sampleStationData.station_id == s) #.orderBy('datetime')
        stationRecordCount.append(sdf.count())
        stationRecordAverage = stationRecordAverage + stationRecordCount[i]/stationCount
        if stationRecordMax < stationRecordCount[i]:
            stationRecordMax =stationRecordCount[i]
        if stationRecordMin > stationRecordCount[i]:
            stationRecordMin =stationRecordCount[i]
        print("station_id =",s, "records = ", stationRecordCount[i])
        print(sdf.show(1))
    print(stationRecordAverage)
    print(stationRecordAverage/(3*8760))
    print(stationRecordMin/(3*8760))
    print(stationRecordMax/(3*8760))


# # Create Station Level Data Set
# - Filter based on station selected
# - Merge with weather data
# - Fill hours without any trip data (i.e., demand or supply is null) with zeroes
# - Create date based features in station data set
#

# ### The next cell defines the functions used to:
# - Create station level data set
# - Create input column list
# - Select Regression Method
# - Create test/train split based on dates
# - Run model for single station
# - Plot predicted vd actual
#
#

# In[7]:


t0 = time.time()

def createStationDataFrame(station,labelLinkFunction='none'):
    print("station_id =",station)
    bf_station = sampleStationData.filter(sampleStationData.station_id == station)

    #left join includes all intervals in weather file in output - then fill supply and demand nulls with zeroes
    #right joing only includes intervals with supply or demand
    bf_station = weatherFeatures.join(bf_station, ['datetime'],how = "left")
    bf_station = bf_station.fillna({'totalDemand':'0','totalSupply':'0'})

    #bf_station.show()
    print("rows in dataframe",bf_station.count())
    print(time.time()-t0)

    # year month and hour are redundent with metblue data fields
    bf_station = bf_station.withColumn("year", year(bf_station.datetime).cast("integer"))
    bf_station = bf_station.withColumn("month", month(bf_station.datetime).cast("integer"))

    @udf('boolean')
    def ifWeekday(dow):
        if dow > 5.0: return False
        else: return (True)

    @udf('boolean')
    def ifRain(precip):
        if precip > 0.0: return True
        else: return (False)

    bf_station = bf_station.withColumn("hourOfDay", hour(bf_station.datetime).cast('integer'))
    bf_station = bf_station.withColumn("dayOfWeek", dayofweek(bf_station.datetime).cast("double"))
    bf_station = bf_station.na.drop(how="any", subset=['dayOfWeek','hourOfDay'])
    bf_station = bf_station.withColumn("weekday", ifWeekday(bf_station.dayOfWeek))
    bf_station = bf_station.withColumn("raining", ifRain(bf_station.total_precip))

    #Label y
    #linkFunction = "log1p"
    if labelLinkFunction == "log1p":
        bf_station = bf_station.withColumn("label", log1p(bf_station.totalDemand))
    else:
        bf_station = bf_station.withColumn("label", bf_station.totalDemand)
    print('bf_station created')
    return(bf_station)

def createfeatureInputCols(stationDataFrame,ignoreColumnList):
    return([x for x in stationDataFrame.columns if x not in ignoreColumnList])


def plotPredictedvsActual(predictedData):
    tripsActual = predictedData.select("label").collect()
    tripsPredicted = predictedData.select("prediction").collect()

    x1=tripsActual
    xlabel='Actual Trips'
    y1=tripsPredicted
    title1='Predicted vs Actual'

    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2")
    evaluator3 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="explainedVariance")


    rmse = evaluator.evaluate(predictedData)
    rSquared = evaluator2.evaluate(predictedData)
    varianceExplained = evaluator2.evaluate(predictedData)


    note1 = '{0:5s} RSME = {1:<8.2n}   R2 = {2:<5.3n}'.format(method, rmse, rSquared)
    note2 = '{0}, normalized = {1}, link function = {2}'.format(featureName, normalize, linkFunction)


    y1label='Preducted Trips'
    filename="RF_yhat_vs_y_"+method+featureName+str(normalize)+linkFunction
    figurepath = pathFigure
    figsaveformat = '.png'
    colors = ['blue','green','red']
    lw_default = 1

    xScaleMin = -2
    xScaleMax = np.max(x1)*1.05


    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 8))

    def ytickformat(x):
        return '$%1.0f' % x

    ax.scatter(x1,y1,linewidth =lw_default, color = colors[0], alpha = .05)
    ax.set_title(title1)
    ax.annotate(note1, xy=(0.15,.9), xycoords = "figure fraction")
    ax.annotate(note2, xy=(0.15,.85), xycoords = "figure fraction")
    ax.set_ylabel(y1label)
    ax.set_xlim(xScaleMin, xScaleMax)
    ax.set_ylim(xScaleMin, xScaleMax)
    ax.set_xlabel(xlabel)
    ax.format_xdata = ytickformat
    ax.format_ydata = ytickformat
    ax.grid(True)
    #save figure as PNG
    figfilename = figurepath+filename+figsaveformat
    plt.savefig(figfilename, bbox_inches='tight', dpi = (300))
    #print(time.time())
    plt.show()
    return()

def selectRegressionMethod(regressionMethodName,featureName):

    if regressionMethodName == "rf":
        if test == True:
            nt = 1
        else: nt = 100
        modelParameters = {'featuresCol':featureName,'numTrees':nt,'subsamplingRate':1,'maxDepth':10}
        regressionMethod = RandomForestRegressor(featuresCol=modelParameters['featuresCol'],
                                                 numTrees = modelParameters['numTrees'],
                                                 subsamplingRate = modelParameters['subsamplingRate'],
                                                 maxDepth =modelParameters['maxDepth'])

    elif regressionMethodName == "gbt":
        modelParameters = {'featuresCol':featureName,'maxIter':10}
        regressionMethod = GBTRegressor(featuresCol = modelParameters['featuresCol'],
                                    maxIter = modelParameters['maxIter'])

    elif regressionMethodName == "glr":
        modelParameters = {'featuresCol':featureName, 'family':"poisson",'link':'log','maxIter':10, 'regParam':0.3}
        regressionMethod = GeneralizedLinearRegression(family = modelParameters['family'],
                                                       link = modelParameters['link'],
                                                       maxIter = modelParameters['maxIter'],
                                                       regParam = modelParameters['regParam'])
    else:
        print('Invalid regression method')
        return()
    #print('Regression method selected')
    return(regressionMethod,modelParameters)


print("functions defined")



def timeSeriesTestTrain(df,dates):
        train = df.where(df.datetime.between(dates['train'][0],dates['train'][1]))
        test = df.where(df.datetime.between(dates['test'][0],dates['test'][1]))
        return (train,test)

def runModel(regressionMethodName,
             stationID,
             stationDataFrame,
             featureInputCols,
             normalize,
             splitMethod = 'random'):
    print("="*80)
    print('Station:{0}'
          .format(stationID))
    print('Model:{0}, Normalize:{1}, LinkFunction:{2}, train/test splitMethod:{3}'
          .format(regressionMethodName,normalize,labelLinkFunction,splitMethod))
    print(featureInputCols)


    oneHot = OneHotEncoderEstimator(inputCols=["hourOfDay", "dayOfWeek"],
                                 outputCols=["hourOfDayVector", "dayOfWeekVector"])

    stationSummaryAll = stationDataFrame.groupBy('station_id').agg(count('label'), sum('label'), avg("label"),stddev_pop("label"))
    stationAvg = stationSummaryAll.select('avg(label)').where(col('station_id') == stationID).collect()
    stationSum = stationSummaryAll.select('sum(label)').where(col('station_id') == stationID).collect()
    stationStd = stationSummaryAll.select('stddev_pop(label)').where(col('station_id') == stationID).collect()
    stationNonZeroCount = stationSummaryAll.select('count(label)').where(col('station_id') == stationID).collect()
    stationCount = stationSummaryAll.select('count(label)').where(col('station_id')== "None").collect()

    featureInputCols.extend(["hourOfDayVector", "dayOfWeekVector"])
    assembler = VectorAssembler(
        inputCols=featureInputCols,
        outputCol='features')

    if normalize == True:
        normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
        featureName = "normFeatures"
        regressionMethod, regressionModelParameters = selectRegressionMethod('rf',featureName)
        pipeline = Pipeline(stages=[oneHot, assembler, normalizer ,regressionMethod])
    else:
        featureName = "features"
        regressionMethod, regressionModelParameters = selectRegressionMethod('rf',featureName)
        pipeline = Pipeline(stages=[oneHot, assembler ,regressionMethod])


    trainingDates = ['2016-10-01 00:00:00',
                 '2017-9-30 23:59:59']

    testDates = ['2017-10-01 00:00:00',
             '2017-10-31 23:59:59']

    dates = {'train':trainingDates, 'test':testDates}



    if splitMethod == 'random':
         # Split the data into training and test sets (30% held out for testing)
        (trainingData, testData) = stationDataFrame.randomSplit([0.6, 0.4])

    else:
        (trainingData, testData) = timeSeriesTestTrain(stationDataFrame, dates)

    #fit model and make predictions
    model = pipeline.fit(trainingData)
    predictedData = model.transform(testData)
    #predictedData.select("prediction", "label", featureName).show(5)
    predictedData
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2")
    evaluator3 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="explainedVariance")

    rmse = evaluator.evaluate(predictedData)
    rSquared = evaluator2.evaluate(predictedData)
    varianceExplained = evaluator2.evaluate(predictedData)


    print("RMSE, R2, and variance explained on test data = {0:6.3f}, {1:6.3f}, {2:6.3f}"
          .format(rmse, rSquared, varianceExplained))
    print()
    basetime = 1541216769
    experimentTimeStamp = int((time.time()-basetime)/6)
    experiment = {experimentTimeStamp:{
        "station":stationID,
        'stationNonZeroCount':stationNonZeroCount,
        'stationCount':stationCount,
        'stationSum':stationSum,
        'stationAvg':stationAvg,
        'stationStd':stationStd,
        'regressionMethodName':regressionMethodName,
        'normalize':normalize,
        'linkFunctionLabel':labelLinkFunction,
        'featureInputCols':featureInputCols,
        'rmse':rmse,
        'rSquared':rSquared,
        'varianceExplained':varianceExplained,
        'version':"Added OneHotEncode for hOD, dOW",
        'trainSplitMethod':splitMethod}}
    experiments.update(experiment)
    with open(pathFigure+"experiments.json","w") as f:
        json.dump(experiments, f)
    return()





runSingle = False
test = False

if runSingle == True:
    print('run single station model')
    t0 = time.time()
    #select single station for initial run
    station = sampleStationList[67]

    labelLinkFunction = 'none'

    #create dataframe for selected station and
    stationDataFrame = createStationDataFrame(station,labelLinkFunction=labelLinkFunction)

    #create list of columns to include in feature vector
    ignoreColumnList= ['datetime', 'station_id', 'total_precip','cloud_cover','dayOfWeek','totalDemand', 'totalSupply','label']

    featureInputCols = createfeatureInputCols(stationDataFrame, ignoreColumnList)
    regressionMethodName = "rf"
    normalize = False

    runModel(regressionMethodName,station,stationDataFrame, featureInputCols, normalize, splitMethod = "dates")

    print(time.time()-t0)


# In[ ]:





# In[ ]:


#Run model on all stations in the sample
test = False
labelLinkFunction = 'none'
ignoreColumnList= ['datetime', 'station_id', 'total_precip','cloud_cover','dayOfWeek','totalDemand', 'totalSupply','label']
#regressionMethodName = "rf"
regressionMethodName = "glr"
#regressionMethodName = "gbt"
normalize = False

normalizeList = [False]
modelList = [regressionMethodName]

for s in sampleStationList:
    sDF = createStationDataFrame(s,labelLinkFunction=labelLinkFunction)
    featureInputCols = createfeatureInputCols(sDF, ignoreColumnList)
    for m in modelList:
        runModel(m,s,sDF, featureInputCols, normalize)
    print(time.time()-t0)

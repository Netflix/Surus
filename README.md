[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/Surus.svg)]()

# Surus

A collection of tools for analysis in Pig and Hive.

## Description

Over the next year we plan to release a handful of our internal user defined functions (UDFs) that have broad adoption across Netflix.  The use 
cases for these functions are varied in nature (e.g. scoring predictive models, outlier detection, pattern matching, etc.) and together extend 
the analytical capabilities of big data.

## Functions
* ScorePMML - A tool for scoring predictive models in the cloud.
* Robust Anomaly Detection (RAD) - An implementation of the Robust PCA.

## Building Surus

Surus is a standard Maven project. After cloning the git repository you can simply run the following command from the project root directory:

    mvn clean package

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which 
can take a considerable amount of time. Subsequent builds will be faster.

## Using Surus

After building Surus you will need to move it to your Hive/Pig instance and register the JAR in your environment.  For those 
unfamiliar with this process see the [Apache Pig UDF](https://pig.apache.org/docs/r0.14.0/udf.html), 
and [Hive Plugin](https://cwiki.apache.org/confluence/display/Hive/HivePlugins), documentation.

You can also install the anomaly detection R package trivially with this code

    library(devtools)
    install_github(repo = "Surus", username = "Netflix", subdir = "resources/R/RAD")

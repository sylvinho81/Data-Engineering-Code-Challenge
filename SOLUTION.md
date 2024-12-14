# Data Engineering Code Challenge
Data Engineering Code Challenge (PySpark)


## ARCHITECTURE DESIGN

Implement a Medallion Architecture with 4 layers:

- Landing layer: where the input files are located 
- Bronze layer: raw data but with some additional metadata columns that capture the load date/time, valid/invalid flag, etc
- Silver layer: cleansed and conformed data. We filter the invalid records, we drop duplicates, etc 
- Gold layer: curated business-level tables

Tech Stack:
- Python
- PySpark 
- Delta Lake as open table format

### Implementation

3 pyspark jobs to keep consistence with what is required in the Challenge: 

- [Data Preparation](spark_jobs/data_preparation.py) 
- [Data Transformation](spark_jobs/data_transformation.py) 
- [Data Export](spark_jobs/data_export.py) 

### Tests

I just decided to implement some tests to show how I would do it by using Chispa, that is a requirement in the Challenge
and how they would be executed as part of the CI by using Github Actions

## MODULES

### VALIDATIONS MODULE

Based on a configuration yaml we can add validations on an easy way. I just implemented some of them:

- check null values
- check that a value is in a specific range so for example price can not be negative
- check that the date has the format YYYY-MM-DD

Based on a rule mapping it can be defined new Classes for validations and configure it in the yaml configuration file.

### ENFORCE SCHEMA

Instead of casting "manually" to the right data types because in the bronze layer I store everything as StringType,
directly I rely on the Delta Lake tables schema to enforce it. 

## MAKEFILE

Run next command to know which commands are available in the Makefile

```
$ make help
```

### How to check quality of the code by running linters:

#### Check errors, wrong formats etc in the code

This command will run as part of the CI. If this task doesn't pass then the CI will fail and you need to fix the issues

```
$ make lint 
```

#### How to fix some of the previous errors automatically:

```
$ make format 
```

### How to run tests

#### Run all tests

```
$ make test 
```

#### Run unit tests

```
$ make test-unit
```

#### Run integration tests

```
$ make test-integration
```

#### Run PySpark Jobs

```
$ make build
```

This is just an example to run the pyspark job: data_preparation.py

```
$ spark-submit --packages io.delta:delta-core_2.12:2.1.0 --py-files "/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0.tar.gz,/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0-py3-none-any.whl"  /home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/data_preparation.py /home/pablo/Projects/Data-Engineering-Code-Challenge/data/landing_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/bronze_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/silver_layer
```

You need to change to your specific paths in your local machine

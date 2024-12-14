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
- Delta Lake as open table format.

### Implementation

3 pyspark jobs to keep consistence with what is required in the Challenge: 

- [Data Preparation](spark_jobs/data_preparation.py) 
- [Data Transformation](spark_jobs/data_transformation.py) 
- [Data Export](spark_jobs/data_export.py) 

You will realize that I am loading the entire table from delta lake in each job. The idea would be to orchestrate 
this with Airflow or similar and just pick the necessary data by using the load timestamps added: created_at and 
updated_at

#### UDFs

I have implemented the UDF because it was required by the challenge, but I would avoid to use UDFs as much as possible
and if it is not strictly needed. Because UDFs are like black boxes for Spark. 

### QA

I would another extra layer to run Great Expectations in each Medallion layer to check the Quality of the data.
I am not expending time now implementing this. 

### Tests

I just decided to implement some tests to show how I would do it by using Pytest and Chispa, that is a requirement in 
the Challenge and how they would be executed as part of the CI by using Github Actions
In the makefile there is a variable COVERAGE_THRESHOLD that let us configure the threshold for the coverage test. 
The idea is to have a high threshold, 90% or similar but since this is a Challenge I will not be expending time
on implementing all possible tests. I prefer to focus on give some examples and indicate how it will be run the CI
through Github actions.

We could also implement integration tests to test the end to end by running the spark jobs by using "subprocess" from 
Python  and submitting the jobs. I didn't expend time on implement this.

### CI/CD

For CD, once the PR is merged into master we would have another github job in a different file called cd.yaml that will
call the recipe "build" in the Makefile and will push the wheel and tar to a specific repo  (e.g s3 bucket) that will
be used later to submit a pyspark job into EMR. 

## MODULES

### VALIDATIONS MODULE

Based on a configuration yaml we can add validations on an easy way. I just implemented some of them:

- check null values
- check that a value is in a specific range so for example price can not be negative
- check that the date has the format YYYY-MM-DD

Based on a rule mapping it can be defined new Classes for validations and configure it in the yaml configuration file.

### DEDUPLICATION

I have used the built on functionality that spark provides to remove duplicates. I have commented in the code 
that we could get the deduplicates and store it in some location for later analysis. The same for the validations,
those records that don't pass the validation could be stored in some location for later analysis as well. I didn't 
expect time on implementing this. 

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

This is just an example to run the pyspark jobs in order since it is an ETL that should be orchestrated:

IMPORTANT: You need to change to your specific paths in your local machine


data_preparation.py

```
$ spark-submit --packages io.delta:delta-core_2.12:2.1.0 --py-files "/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0.tar.gz,/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0-py3-none-any.whl"  /home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/data_preparation.py /home/pablo/Projects/Data-Engineering-Code-Challenge/data/landing_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/bronze_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/silver_layer
```



data_transformation.py

```
$ spark-submit --packages io.delta:delta-core_2.12:2.1.0 --py-files "/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0.tar.gz,/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0-py3-none-any.whl"  /home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/data_transformation.py  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/silver_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/gold_layer
```

data_export.py

```
spark-submit --packages io.delta:delta-core_2.12:2.1.0  --py-files "/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0.tar.gz,/home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/sales_transactions_etl-0.1.0-py3-none-any.whl"  /home/pablo/Projects/Data-Engineering-Code-Challenge/dist-sales/data_export.py  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/gold_layer  /home/pablo/Projects/Data-Engineering-Code-Challenge/data/gold_layer
```
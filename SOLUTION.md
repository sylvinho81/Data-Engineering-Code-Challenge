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


### VALIDATIONS MODULE

Based on a configuration yaml we can add validations on an easy way. I just implemented some of them:

- check null values
- check that a value is in a specific range so for example price can not be negative
- check that the date has the format YYYY-MM-DD

Based on a rule mapping it can be defined new Classes for validations and configure it in the yaml configuration file.

## Makefile

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


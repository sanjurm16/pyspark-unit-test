# pyspark-unit-test

This project showcases how we can use pytest to create unit test cases for pyspark application

The spark application analyses flight data to create an ordered list of the flights with maximum times late arrivals.

Pytest fixtures are used to create spark sessions and supplied them automatically to the pipeline unit cases.
Some of the important files are:

1. *conftest.py* : Defines the spark pytest fixture with module scope
2. *flight_analyzer_tests.py* : creates dataframes used for the unit test cases

unit test can be run as:
**pytest flight_analyzer_tests.py**

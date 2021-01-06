# Alteryx SDK Snowflake Output
Alteryx Python SDK tool to write output to a Snowflake Databbase

Custom Alteryx SDK tool to output data to a Snowflake Database

The tools allows for:

- Create tables (drop if already exists) and appending data
- Truncate data in exisiting table and appending
- Append data to an existing table
- Update or insert new data in a table based on a common key

## Installation
Download the yxi file and double click to install in Alteyrx. 

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/install.png" alt="Snowflake Install Dialog">

The tool will be installed in the __Connectors__ category.

<img src="hhttps://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/toolbar.png" alt="Snowflake Install Toolbar">

## Requirements

The tool installs the (https://docs.snowflake.com/en/user-guide/python-connector.html | official Snowflake Connector library)

## Usage
Configure the tool using the setting for you Snowflake instance. Note that the account is the string before __snowflakecomputing.com__

## Outputs
The tool has no output.

## Usage
This workflow demonstrates the tool in use. The workflow shown here:

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/CSVAppend_workflow.png" width="1000" alt="CSV Append Workflow">

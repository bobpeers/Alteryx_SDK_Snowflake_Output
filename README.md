# Alteryx SDK Snowflake Output
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

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/toolbar.png" alt="Snowflake Install Toolbar">

## Requirements

The tool installs the official [Snowflake Connector library](https://docs.snowflake.com/en/user-guide/python-connector.html)

## Usage
Configure the tool using the setting for you Snowflake instance. Note that the account is the string to the left of __snowflakecomputing.com__ in your URL.

If you do not select a temporary path then the tool will use the default Alteryx temp path. In this path the tool will create subfolders based on the current UNIX time.
When running temp csv files will be written to this location before being gzipped and uploaded to Snowflake. The gzipped files are not deleted automatically by the tool. The tool will create csv files with a maximum size of 25.000.000 records per file.

## Logging
The tool will create log files for each run in the temp folder supplied. These logs contain detailed information on the Snowflake connection and can be used in case of unexected errors.

## Outputs
The tool has no output.

## Usage
This workflow demonstrates the tool in use. The workflow shown here:

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/configuration.png" width="1000" alt="Snowflake Workflow">

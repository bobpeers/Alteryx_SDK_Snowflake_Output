# Alteryx SDK Snowflake Output
Custom Alteryx SDK tool to output data to a Snowflake Database

The tools allows for:

- Create tables (drop if already exists) and appending data
- Truncate data in exisiting table and appending
- Append data to an existing table
- Update or insert new data in a table based on a common key

## Advanced Options Include
- Quote all fields (they will be case sensitive in Snowflake)
- Suspend the warehouse immediately after running (this will cause Snowflake to wait until current operations are finished first)
- Auto delete temporary files created by the connector (gzipped files only, not log files)

|Import Chnage to Password Field|
|:---|
|Due to the way the Alteryx SDK encrypts passwords in a password text box it is not possible to publish these workflows to Alteryx Server and schedule
them. For this reason the password field is now no longer encrypted.|

## Installation
Download the yxi file and double click to install in Alteyrx. 

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/install.png" width='500px' alt="Snowflake Install Dialog">

The tool will be installed in the __Connectors__ category.

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/toolbar.png" alt="Snowflake Install Toolbar">

## Requirements

The tool installs the official [Snowflake Connector library](https://docs.snowflake.com/en/user-guide/python-connector.html)

## Authorisation
This can be either via Snowflake or Okta. If you select Okta authentication this must be set up on the server according to the [Snowflake Instructions](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-configure-snowflake.html). 

<img src='https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/okta.gif' width=500px alt='Snowflake Okta Authentication'>

## Usage
Configure the tool using the setting for you Snowflake instance. Note that the account is the string to the left of __snowflakecomputing.com__ in your URL.

If you are creating a new table the key is optional but you must select a key when updating/insert new.

If you do not select a temporary path then the tool will use the default Alteryx temp path. Using this path the tool will create subfolders based on the current UNIX time.

When running temp csv files will be written to this location before being gzipped and uploaded to Snowflake.

The gzipped files are not deleted automatically by the tool unless you select the advanced option. The tool will create multiple csv files with a maximum size of 25.000.000 records per file.

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/logging.png" alt="Snowflake Temp folder">

### Preserve Case Checkbox
If you don't select the preserve case option then the fields will be created as provided by the upstream tool. These fields will be checked for validity and if found to be invalid they will automatically be quested so thet become case sensitive in Snowflake. This setting also applies to table names.

|Import Note on Primary keys|
|:---|
|Snowflake does not enforce primary keys so setting as key will create a primary key and set the field as not allowing null values but it is still possible to append data to a table with duplicate valiues in the primary key field.|

## Logging
The tool will create log files for each run in the temp folder supplied. These logs contain detailed information on the Snowflake connection and can be used in case of unexected errors.

## Outputs
The tool has no output.

## Example Configuration
This workflow demonstrates the tool in use. The workflow shown here:

<img src="https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/configuration.png" width="500" alt="Snowflake Workflow">

## Technical Notes
Internally the tool uses the Snowplake `PUT` command to bulk upload files so is very efficient. The process is as follows:

1. Data is written to CSV files in chunks of 100k records (all quoted and pipe delimited)
2. CSV files are individually gzipped
3. If we need to create a table we convert Alteryx data types to Snowflake datatype and create a table
4. Data is uploaded to a table stage using the `PUT` command
5. If updating we upload to a temporary table
6. Data is copied from the staging area to the target table using `COPY`
7. If updating, data is merged from the temporary table to the target table using `MERGE`
8. The warehouse if suspended if the option is selected (alter warehouse 'wh' suspend)
9. Temporary files (gzip files only) are deleted if the option if selected

|Import Note on Auto Suspending|
|:---|
|To automatically suspend the warehouse after running your user must have OPERATE permisions on the warehouse|

## Updates
If you use the Alteryx ListRunner macro this has a bug in the handling of empty configurations in the tool. Instead of sending blank entries it sends a carriage return along with spaces (by literally parsing the raw XML without removing new lines and spaces before the closing XML tag.
The latest releases (1.6+) fixes this by sanitising the data.

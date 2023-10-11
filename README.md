# Facebook Advertising Data Unification
## What is the purpose of this dbt package?
This dbt package is for the Facebook Advertising data unification Ingested by Daton. Daton is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

## How do I use Facebook Ads dbt package?
### Supported Datawarehouses:
- [BigQuery](https://sarasanalytics.com/blog/what-is-google-bigquery/)
- [Snowflake](https://sarasanalytics.com/daton/snowflake/)

#### Typical challenges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Facebook
- Separate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following functions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are Standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are Standardized by converting to specified timezone using input offset hours.

#### Prerequisites for Facebook Ads dbt package 
Daton Integrations for  
- [Facebook Ads](https://sarasanalytics.com/daton/facebook-ads/) 
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Configuration for dbt package 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_facebook). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  facebook_ads:
    FacebookAds:
      +schema: custom_schema_extension

```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False. Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table for which you want timezone converison to be taken into account.

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Facebook.Ads.Brand_UK_Facebook_AdCreatives":-7,
    "Facebook.Ads.Brand_UK_Facebook_Adinsights":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdinsightsActionValues":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdinsightsConversionValues":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdinsightsConversions":-7,
    "Facebook.Ads.Brand_UK_Facebook_Ads":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByCountry":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByDevicePlatform":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByGender":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByGenderAge":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByPlatformPosition":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByProductId":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByPublisherPlatform":-7,
    "Facebook.Ads.Brand_UK_Facebook_AdsByRegion":-7,
    "Facebook.Ads.Brand_UK_Facebook_Campaigns":-7,

    }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
Facebook_Adinsights: False
```

## Models

This package contains models from the Facebook Advertising API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** | **Unique Key** | **Partition Key** | **Cluster Key** |
| ------------------------- | ---------------| ----------------------- | ------------------- | ---------------- | ------------------- |
|Ad Insights | [FacebookAdinsights](models/FacebookAds/FacebookAdinsights.sql)  | A report with Ad Insights. | date_start, ad_id | field: date_start, data_type: date | date_start |
|Ad Insights | [FacebookAdinsightsActionValues](models/FacebookAds/FacebookAdinsightsActionValues.sql)  | A report with Ad Insights on basis of action values. | date_start, ad_id, action_values_action_type | field: date_start, data_type: date | date_start |
|Ad Insights | [FacebookAdinsightsConversions](models/FacebookAds/FacebookAdinsightsConversions.sql)  | A report with Ad Insights on basis of conversions. | date_start, ad_id, conversions_action_type | field: date_start, data_type: date | date_start |
|Ad Insights | [FacebookAdinsightsConversionValues](models/FacebookAds/FacebookAdinsightsConversionValues.sql)  | A report with Ad Insights on basis of conversion values. | date_start, ad_id, conversion_values_action_type | field: date_start, data_type: date | date_start |
|Ad Insights | [FacebookAdCreatives](models/FacebookAds/FacebookAdCreatives.sql)  | Format which provides layout and contains content for the ad. | effective_object_story_id,name | | effective_object_story_id
|Ad Insights | [FacebookAdsByCountry](models/FacebookAds/FacebookAdsByCountry.sql)  | A report with Ad Insights on basis of countries. | ad_id,date_start,country | field: date_start, data_type: date | ad_id,country |
|Ad Insights | [FacebookAdsByDevicePlatform](models/FacebookAds/FacebookAdsByDevicePlatform.sql)  | A report with Ad Insights on basis of Device Platform. | ad_id,date_start,device_platform | field: date_start, data_type: date | ad_id,device_platform |
|Ad Insights | [FacebookAdsByGender](models/FacebookAds/FacebookAdsByGender.sql)  | A report with Ad Insights on basis of Gender. | ad_id,date_start,gender | field: date_start, data_type: date | ad_id,gender |
|Ad Insights | [FacebookAdsByGenderAge](models/FacebookAds/FacebookAdsByGenderAge.sql)  | A report with Ad Insights on basis of Gender and Age. | ad_id,date_start,gender,age | field: date_start, data_type: date | ad_id,gender,age |
|Ad Insights | [FacebookAdsByProductId](models/FacebookAds/FacebookAdsByProductId.sql)  | A report with Ad Insights on basis of Product ID. | ad_id,date_start,product_id | field: date_start, data_type: date | ad_id,product_id |
|Ad Insights | [FacebookAdsByPublisherPlatform](models/FacebookAds/FacebookAdsByPublisherPlatform.sql)  | A report with Ad Insights on basis of Publisher Platform. |ad_id,date_start,publisher_platform | field: date_start, data_type: date | ad_id,publisher_platform |
|Ad Insights | [FacebookAdsByPlatformPosition](models/FacebookAds/FacebookAdsByPlatformPosition.sql)  | A report with Ad Insights on basis of Publisher Platform and Platform Position. | ad_id,date_start,publisher_platform,platform_position | field: date_start, data_type: date | ad_id,publisher_platform,platform_position |
|Ad Insights | [FacebookAdsByRegion](models/FacebookAds/FacebookAdsByRegion.sql)  | A report with Ad Insights on basis of Region. | ad_id,date_start,region | field: date_start, data_type: date | ad_id,region |
|Ad Insights | [FacebookCampaigns](models/FacebookAds/FacebookCampaigns.sql)  | A report with Campaign Details. | campaign_id |  field: start_time, data_type: timestamp, granularity: day  | campaign_id |
|Ad Insights | [FacebookAds](models/FacebookAds/FacebookAds.sql)  | A report with Ad Creative Details. | ad_id |  field: created_time, data_type: timestamp, granularity: day  | ad_id |



### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2

models:
    - name: FacebookAds
      description: A report with Ad Insights
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'created_time', 'data_type': 'timestamp', 'granularity': 'day' }
        cluster_by: ['ad_id']
        unique_key: ['ad_id']

    - name: FacebookAdinsights
      description: A report with Ad Insights
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['date_start']
        unique_key: ['date_start', 'ad_id']

    - name: FacebookAdinsightsActionValues
      description: A report with Ad Insights on basis of action values
      config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['date_start']
        unique_key: ['date_start', 'ad_id', 'action_values_action_type']

    - name: FacebookAdinsightsConversions
      description: A report with Ad Insights on basis of conversions
      config:
        materialized: incremental
        incremental_strategy: merge
        unique_key: ['date_start', 'ad_id', 'conversions_action_type']
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['date_start']

    - name: FacebookAdinsightsConversionValues
      description: A report with Ad Insights on basis of conversion values
      config:
        materialized: incremental
        incremental_strategy: merge
        unique_key: ['date_start', 'ad_id', 'conversion_values_action_type']
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['date_start']

  - name: FacebookAdCreatives
    description: Format which provides layout and contains content for the ad.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['effective_object_story_id','name']
      cluster_by: ['effective_object_story_id']

    - name: FacebookAdsByCountry
      description: A report with Ad Insights on basis of Country.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','country']
        unique_key: ['ad_id','date_start','country']

    - name: FacebookAdsByDevicePlatform
      description: A report with Ad Insights on basis of Device Platform.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','device_platform']
        unique_key: ['ad_id','date_start','device_platform']

    - name: FacebookAdsByGender
      description: A report with Ad Insights on basis of Gender.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','gender']
        unique_key: ['ad_id','date_start','gender']

    - name: FacebookAdsByGenderAge
      description: A report with Ad Insights on basis of Gender and Age.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','gender','age']
        unique_key: ['ad_id','date_start','gender','age']

    - name: FacebookAdsByProductId
      description: A report with Ad Insights on basis of Product ID.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','product_id']
        unique_key: ['ad_id','date_start','product_id']

    - name: FacebookAdsByPublisherPlatform
      description: A report with Ad Insights on basis of Publisher Platform.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','publisher_platform']
        unique_key: ['ad_id','date_start','publisher_platform']

    - name: FacebookAdsByPlatformPosition
      description: A report with Ad Insights on basis of Publisher Platform and Platform Position.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','publisher_platform','platform_position']
        unique_key: ['ad_id','date_start','publisher_platform','platform_position']

    - name: FacebookAdsByRegion
      description: A report with Ad Insights on basis of Region.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'date_start', 'data_type': 'date' }
        cluster_by: ['ad_id','region']
        unique_key: ['ad_id','date_start','region']

    - name: FacebookCampaigns
      description: A report with Campaign Details.
      config: 
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: { 'field': 'start_time', 'data_type': 'timestamp', 'granularity': 'day' }
        cluster_by: ['campaign_id']
        unique_key: ['campaign_id']

```



## What Facebook dbt resources are available?
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or [contact us](https://sarasanalytics.com/contact).
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}

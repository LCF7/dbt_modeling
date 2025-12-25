# Sales Lifecycle Analysis with dbt and Snowflake
This project transforms raw data from a CRM system into a robust, analytical data model designed to answer complex questions about the sales lifecycle. Using dbt and Snowflake, it builds a pipeline that not only cleans and structures the information but also enriches it for advanced analytics and Machine Learning applications.

![HLD](https://drive.google.com/uc?export=view&id=1iZMiVwdTs3E1jLuzqkbOL3SuxwiS9i0z) 

# Key Features:

### Layered Data Modeling: 
Implements a scalable data architecture following best practices, separating the Staging layer (views for basic cleaning and renaming) from the Marts layer (business-facing models materialized as tables).
![HLD](https://drive.google.com/uc?export=view&id=1PiYDDr23U68lGBsW3_SSxA7mxxwuFNeZ) 
### Star Schema: 
The core of the project consists of dimension tables (dim_deal, dim_sales_rep) and fact tables (fct_deal_lifecycle, fct_daily_deal_snapshot), enabling efficient and easy-to-understand analysis in Business Intelligence tools.
### Slowly Changing Dimensions (SCD - Type 2): 
By using dbt snapshot, the project captures the complete history of changes in a deal's stages. This is crucial for analyzing how long a deal remains in each phase and understanding its evolution.
![HLD](https://drive.google.com/uc?export=view&id=1T38sMg_p_J8XPJz_FXXd2UKUhZjWL7Gr)
### Advanced Feature Engineering:
#### fct_deal_lifecycle: 
Calculates key metrics such as the days spent in each stage of the sales funnel (Discovery, Negotiation, etc.), allowing for the identification of bottlenecks.
#### fct_daily_deal_snapshot: 
Built as an incremental model, it generates a daily "snapshot" of each deal's state. This model is optimized for performance and specifically designed as a foundation for training Machine Learning models that could, for example, predict the probability of a deal closing.
### Data Quality and Testing: 
The project ensures data integrity through an exhaustive testing framework, using both native dbt tests (unique, not_null) and custom tests from the dbt_expectations package to validate ranges, accepted values, and formats.
Tech Stack: dbt, Snowflake, Git.
![HLD](https://drive.google.com/uc?export=view&id=1T38sMg_p_J8XPJz_FXXd2UKUhZjWL7Gr) 

# ETL/ELT Logic:

![HLD](https://drive.google.com/uc?export=view&id=113kciwmbgpRQijfx6qHaCoNsoMuIjMR8)  

# Scalability:

![HLD](https://drive.google.com/uc?export=view&id=1IMYNiNS2vrXn_QwUwgy7FyKzk_9nSzuV)  

# Reliability:

![HLD](https://drive.google.com/uc?export=view&id=1yfviD9fwwqIUlE7welQioK9P4y4mDg7k) 



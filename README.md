# Data Warehouse / Analytics Project with SQL

Welcome to the Data Warehouse/Analytics Project Repository!
This project demonstrates a data warehousing and analytics solution from building a data warehouse from scratch to actionable insights. Designed as a portfolio project to practice data engineering practices.

## Project Requirements

### Building The Data Warehouse

**Objective:**

Develop a modern data warehouse using SQL Server to consolidate and manage sales datas for reporting

**Specifications:**
- Data Sources: CSV files (ERP and CRM)
- Data Quality: Cleanse/Resolve issues prior to analysis
- Integration: Combine sources into one data model for querying
- Scope: Focus on latest dataset, so avoiding historization
- Documentation: Provide documentation of the model for stakeholders

### BI Analytics / Reporting

**Objective:**

Develop SQL-base analytics for insights into customer behavior, product performance, and sales trends to enable business analytics and decision making.

## License

This project is licensed under the [MIT License](LICENSE). You're free to use, modify, and share this project with proper attribution.

## About Me

Howdy, my name is Eric Rogler and my goal is to explore data engineering and pipeline creation.

## Bronze Layer

Several steps are involved with creating this layer:
- Analyzing by checking with SMEs
- Data Ingestion
- Data Completeness & Schema Checks
- Documenting & Versioning in Git

Essentially, we're discovering who owns the data, the process it supports, documentation to solve it, and any modeling and cataloging. It's also where we figured out where the data's stored and capabilities for integration.

This stage also covers ETL/ELT, where we determine Incremental vs Full loads, scope, size, volumne, limitations and constraints, performance impactors, authentication and authorization, and so on.

Once all these questions are answered, we can start scripting to go past database initialization. There won't be any transformations or modeling just yet; it's only getting everything set up for further modifications.

## Silver Layer

This layer normally takes the longest time to complete.

The following steps are expected:
- Exploring and understanding the data
- Data Cleansing (checking bronze quality and writing transformations before inserting into silver)
- Data Correctness Checks
- Data Documenting Versioning in GIT (Flow and Integration)

This can include metadata columns as well not originating from source data but applying additional context, such as when records are update, what its original source was, and the location of files.

## Gold Layer

In this layer, we're exploring business objects from our two sources. This means we're building the business objects, combining existing tables if appropriate to do so, deciding between dimension vs fact vs flat table, and renaming columns. Aftwards, there's an integration check to ensure data quality is preserved and finalizing presentation diagrams.

To add additional context to what's happening here:
Data Modeling: taking raw data and configuring it into tables to define their relationships. There's three types of data models to consider: Conceptual, Logical, and Physical (Big picture, blueprint, and implementation). To preserve time and due to current simplicity of data, only the logical model is explored.

Star Schema is the preferred model here for the reasons stated earlier. Snowflake schema would work, but we don't need many sub-dimensions for functionality.

As for Dimensions vs Facts, Dimensions are descriptive information providing context whereas Facts are quantitative information representing events. We have three tables here:

- Customer: Dimension
- Product: Dimension
- Sales: Fact



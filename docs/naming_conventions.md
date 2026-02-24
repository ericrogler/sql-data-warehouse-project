# Data Engineering
## Table Naming Conventions

### Bronze Rules

- All names must start with the source system name, and table names must match their original names without renaming.

```markdown
<sourcesystem>_<entity>
- <sourcesystem>: Name of the source system (e.g. crm, erp)
- <entity>: Exact table name from the source system
- Example: crm_customer_info -> Customer information from CRM
```

### Silver Rules

- All names must start with the source system name, and table names must match their original names without renaming.

```markdown
<sourcesystem>_<entity>
- <sourcesystem>: Name of the source system (e.g. crm, erp)
- <entity>: Exact table name from the source system
- Example: crm_customer_info -> Customer information from CRM
```

### Bronze Rules

- All names must use meaningful, business-aligned names for tables, starting with the category prefix.

```markdown
<category>_<entity>
- <category>: Describes the role of the table, such as dim (dimension) or fact (fact table)
- <entity>: Descriptive name of the table, aligned with the business domain (e.g. customers, products, sales)
- Examples:
	- dim_customers -> dimension table for customer data
	- fact_sales -> fact table containing sales transactions
```

### **Category Patterns**

| **Pattern** | **Meaning** | **Example(s)** |
| --- | --- | --- |
| dim_ | Dimension Table | dim_customer, dim_product |
| fact_ | Fact Table | fact_sales |
| agg_ | Aggregated Table | agg_customers, agg_sales_monthly |

## Column Naming Conventions

### Surrogate Keys

- All primary keys in dimension tables must use the suffix “_key”

```markdown
<table_name>_key
- <category>: Refers to the name of the table or the entity the key belongs to
- _key: Descriptive name of the table, aligned with the business domain (e.g. customers, products, sales)
- Example: customer_key -> Surrogate key in the dim_customers table
```

### Technical Columns

- All technical columns must start with the prefix “dwh_” followed by a descriptive name indicating the column’s purpose.

```markdown
dwh_<column_name>
- dwh: Prefix exclusively for system-generated metadata
- <entity>: Descriptive name indicating the column's purpose
- Examples: dwh_load_date -> System-generated column used to store the date when the record was loaded
```

## Stored Procedures

- All stored procedures used for loading data must follow the naming pattern: load_<layer>
load_<layer>
```markdown
- <layer>: Represents the layer being loaded, such as bronze, silver, or gold.
- Examples: 
	- load_bronze -> Stored procedure for loading data into the Bronze Layer
	- load_silver -> Stored procedure for loading data into the Silver layer
```

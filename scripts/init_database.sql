/*Create Database 'DataWarehouse'
PURPOSE:
Creates a new database named DataWarehouse after checking if there already is a duplicate.
If duplicate detected, it drops and recreates it and sets up Medallion architecture with
'bronze', 'silver', and 'gold'

WARNING: 
THIS SCRIPT WILL PERMANENTLY DELETE EXISTING DATA IF AN EXISTING
'DataWarehouse' TABLE EXISTS! CHECK FOR BACKUPS FIRST!
*/

USE master;
GO -- These are separators when executing code

-- Check if it exists.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE USER WITH ROLLBACK IMMEDIATE;

-- Create initial database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

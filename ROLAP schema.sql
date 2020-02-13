/*
	Northind ROLAP Bus Architecture
	By: Michael Fudge (mafudge@syr.edu)

	This script creates two conformed dimensional models in the northwind schema
		- FactSales
		- FactInventoryDailySnapshot			
	
	For use with the ETL Lab (SSIS) and OLAP Lab (SSAS)
	
	IMPORTANT: Execute this script in your data warehouse (dw) database
*/
-- Create the schema if it does not exist
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fudgemart')) 
BEGIN
    EXEC ('CREATE SCHEMA [fudgemart] AUTHORIZATION [dbo]')
	PRINT 'CREATE SCHEMA [fudgemart] AUTHORIZATION [dbo]'
END
go 
-- delete all the fact tables in the schema
DECLARE @fact_table_name varchar(100)
DECLARE cursor_loop CURSOR FAST_FORWARD READ_ONLY FOR 
	select TABLE_NAME from INFORMATION_SCHEMA.TABLES 
		where TABLE_SCHEMA='fudgemart' and TABLE_NAME like 'Fact%'
OPEN cursor_loop
FETCH NEXT FROM cursor_loop  INTO @fact_table_name
WHILE @@FETCH_STATUS= 0
BEGIN
	EXEC ('DROP TABLE [fudgemart].[' + @fact_table_name + ']')
	PRINT 'DROP TABLE [fudgemart].[' + @fact_table_name + ']'
	FETCH NEXT FROM cursor_loop  INTO @fact_table_name
END
CLOSE cursor_loop
DEALLOCATE cursor_loop
go
-- delete all the other tables in the schema
DECLARE @table_name varchar(100)
DECLARE cursor_loop CURSOR FAST_FORWARD READ_ONLY FOR 
	select TABLE_NAME from INFORMATION_SCHEMA.TABLES 
		where TABLE_SCHEMA='fudgemart' and TABLE_TYPE = 'BASE TABLE'
OPEN cursor_loop
FETCH NEXT FROM cursor_loop INTO @table_name
WHILE @@FETCH_STATUS= 0
BEGIN
	EXEC ('DROP TABLE [fudgemart].[' + @table_name + ']')
	PRINT 'DROP TABLE [fudgemart].[' + @table_name + ']'
	FETCH NEXT FROM cursor_loop  INTO @table_name
END
CLOSE cursor_loop
DEALLOCATE cursor_loop
go

-- Employee Dimension
/*PRINT 'CREATE TABLE fudgemart.DimEmployee'
CREATE TABLE northwind.DimEmployee (
   [EmployeeKey]  int IDENTITY  NOT NULL
   --attributes
,  [EmployeeID]  int   NOT NULL
,  [EmployeeName]  nvarchar(40)   NOT NULL
,  [EmployeeTitle]  nvarchar(30)   NOT NULL
,  [HireDateKey] int NULL
,  [SupervisorID]  int   NULL
,  [SupervisorName]  nvarchar(40)  NULL
,  [SupervsorTitle]  nvarchar(30)  NULL	
-- metadata
,  [RowIsCurrent]  bit   DEFAULT 1 NOT NULL
,  [RowStartDate]  datetime  DEFAULT '12/31/1899' NOT NULL
,  [RowEndDate]  datetime  DEFAULT '12/31/9999' NOT NULL
,  [RowChangeReason]  nvarchar(200)   NULL
, CONSTRAINT [pkNorthwindDimEmployee] PRIMARY KEY ( [EmployeeKey] )
);*/

-- Customer Dimension
PRINT 'CREATE TABLE fudgemart.DimCustomer'
CREATE TABLE fudgemart.DimCustomer (
   [CustomerKey]  int IDENTITY  NOT NULL
   -- Attributes
,  [CustomerID]  int NOT NULL
,  [Customer_Email] varchar(100) NOT NULL
,  [Customer_Name]  varchar(50)   NOT NULL
,  [Customer_Address]  varchar(255)   NOT NULL
,  [Customer_City]  varchar(50) DEFAULT 'N/A'  NOT NULL
,  [Customer_State]  char(2)  NOT NULL
,  [Customer_Zip]  varchar(20)   NOT NULL
,  [Customer_Phone]  varchar(30)   NOT NULL
	-- metadata
,  [RowIsCurrent]  bit  DEFAULT 1 NOT NULL
,  [RowStartDate]  datetime  DEFAULT '12/31/1899' NOT NULL
,  [RowEndDate]  datetime  DEFAULT '12/31/9999' NOT NULL
,  [RowChangeReason]  nvarchar(200)   NULL
, CONSTRAINT pkFudgemartDimCustomer PRIMARY KEY ( [CustomerKey] )
);


-- Product Dimension
PRINT 'CREATE TABLE fudgemart.DimProduct'
create table fudgemart.DimProduct
(
	ProductKey int identity not null,
	-- attributes
	ProductID int not null, 
	Product_Department varchar(20) not null,
	Product_Name varchar(50) not null,
	Product_Retail_Price money not null,
	Product_WholeSale_Price money not null,
	Product_isActive bit not null,
	Product_addDate datetime null,
	-- metadata
	RowIsCurrent bit default(1) not null,
	RowStartDate datetime default('1/1/1900') not null,
	RowEndDate datetime default('12/31/9999') not null,
	RowChangeReason nvarchar(200) default ('N/A') not null,
	-- keys
	constraint pkFudgemartDimProductKey primary key (ProductKey),	
);

--Order
PRINT 'CREATE TABLE fudgemart.DimOrder'
create table fudgemart.DimOrder
(
	OrderKey int identity not null,
	-- attrivbutes
	OrderID int not null,
	CustomerID int not null,
	Order_Date datetime not null,
	Shipped_Date datetime not null,
	Ship_Via nvarchar(20) not null,
	-- metadata
	RowIsCurrent bit default(1) not null,
	RowStartDate datetime default('1/1/1900') not null,
	RowEndDate datetime default('12/31/9999') not null,
	RowChangeReason nvarchar(200) default ('N/A') not null,
	-- keys
	constraint pkFudgemartDimOrderKey primary key (OrderKey),
);

--Order
PRINT 'CREATE TABLE fudgemart.DimOrderDetails'
create table fudgemart.DimOrderDetails
(
	OrderDetailsKey int identity not null,
	-- attrivbutes
	OrderID int not null,
	ProductID int not null,
	Order_Quantity int not null,
	-- metadata
	RowIsCurrent bit default(1) not null,
	RowStartDate datetime default('1/1/1900') not null,
	RowEndDate datetime default('12/31/9999') not null,
	RowChangeReason nvarchar(200) default ('N/A') not null,
	-- keys
	constraint pkFudgemartDimOrderDetailsKey primary key (OrderDetailsKey),
);
-- Supplier
/*PRINT 'CREATE TABLE northwind.DimSupplier'
create table northwind.DimSupplier
(
	SupplierKey int identity not null,
	-- attrivbutes
	SupplierID int not null,
	CompanyName nvarchar(40) not null,
	ContactName nvarchar(30) not null,
	ContactTitle nvarchar(30) not null,
	City nvarchar(15) not null,
	Region nvarchar(15) not null,
	Country nvarchar(15) not null,
	-- metadata
	RowIsCurrent bit default(1) not null,
	RowStartDate datetime default('1/1/1900') not null,
	RowEndDate datetime default('12/31/9999') not null,
	RowChangeReason nvarchar(200) default ('N/A') not null,
	-- keys
	constraint pkNorthwindDimSupplierKey primary key (SupplierKey),
);*/

-- date dimension
PRINT 'CREATE TABLE fudgemart.DimDate'
CREATE TABLE fudgemart.[DimDate](
	[DateKey] [int] NOT NULL,
	[Date] [datetime] NULL,
	[FullDateUSA] [nchar](10) NOT NULL,
	[DayOfWeek] [tinyint] NOT NULL,
	[DayName] [nvarchar](10) NOT NULL,
	[DayOfMonth] [tinyint] NOT NULL,
	[DayOfYear] [int] NOT NULL,
	[WeekOfYear] [tinyint] NOT NULL,
	[MonthName] [nvarchar](10) NOT NULL,
	[MonthOfYear] [tinyint] NOT NULL,
	[Quarter] [tinyint] NOT NULL,
	[QuarterName] [nvarchar](10) NOT NULL,
	[Year] [int] NOT NULL,
	[IsAWeekday] varchar(1) NOT NULL DEFAULT (('N')),
	constraint pkFudgemartDimDate PRIMARY KEY ([DateKey])
)


-- Periodic Snapshot for Inventory analysis
/*PRINT 'CREATE TABLE northwind.FactInventoryDailySnapshot'
create table northwind.FactInventoryDailySnapshot
(
	ProductKey int not null,
	SupplierKey int not null,
	DateKey int not null,
	-- facts
	UnitsInStock int not null,
	UnitsOnOrder int not null
	-- keys
	constraint pkNorthwindFactInventoryKey primary key (DateKey, ProductKey),

	constraint fkNorthwindFactInventoryProductKey foreign key (ProductKey) 
		references northwind.DimProduct(ProductKey),
	constraint fkNorthwindFactInventorySupplierKey foreign key (SupplierKey) 
		references northwind.DimSupplier(SupplierKey),
	constraint fkNorthwindFactInventoryDateKey foreign key (DateKey) 
		references northwind.DimDate(DateKey),
);*/

-- sales coverage fact table
PRINT 'CREATE TABLE fudgemart.FactSalesCoverage'
CREATE TABLE fudgemart.FactSalesCoverage (
   [ProductKey]  int   NOT NULL
	-- dimensions
,  [CustomerKey]  int   NOT NULL
--,  [OrderDetailsKey] int NOT NULL
,  [OrderDateKey]  int   NOT NULL
,  [ShippedDateKey]  int   NOT NULL
,  [OrderID] int NOT NULL
	-- facts
,  [Quantity]  int   NOT NULL
,  [Retail_price]  money NOT NULL
,  [Wholesale_price]  money  DEFAULT 0 NOT NULL
,  [SoldAmount]  money  NOT NULL
,  [Profit] money NOT NULL
   --keys
, CONSTRAINT pkFudgemartFactSales PRIMARY KEY ( ProductKey, OrderID)
, CONSTRAINT fkFudgemartFactSalesProductKey FOREIGN KEY ( ProductKey )
	REFERENCES fudgemart.DimProduct (ProductKey)
, CONSTRAINT fkFudgemartFactSalesCustomerKey FOREIGN KEY ( CustomerKey )
	REFERENCES fudgemart.DimCustomer (CustomerKey)
--,CONSTRAINT fkFudgemartFactSalesOrderKey FOREIGN KEY ( OrderKey )
	--REFERENCES fudgemart.DimOrder (OrderKey)-
--, CONSTRAINT fkFudgemartFactSalesOrderDetailsKey FOREIGN KEY ( OrderDetailsKey )
	--REFERENCES fudgemart.DimOrderDetails (OrderDetailsKey)
, CONSTRAINT fkFudgemartFactSalesOrderDateKey FOREIGN KEY (OrderDateKey )
	REFERENCES fudgemart.DimDate (DateKey)
, CONSTRAINT fkNorthwindFactSalesShippedDateKey FOREIGN KEY (ShippedDateKey )
	REFERENCES fudgemart.DimDate (DateKey)
) 
;

PRINT 'Insert special dimension values for null'
go
-- Unknown Customer
SET IDENTITY_INSERT [fudgemart].[DimCustomer] ON
go
INSERT INTO [fudgemart].[DimCustomer]
           ([CustomerKey]
		   ,[CustomerID]
           ,[Customer_Email]
           ,[Customer_Name]
           ,[Customer_Address]
           ,[Customer_City]
           ,[Customer_State]
           ,[Customer_Zip]
           ,[Customer_Phone])
     VALUES
           (-1
		   ,-1
           ,'Unknown Email'
           ,'Unknown Name'
           ,'None'
           ,'None'
           ,'NA'
           ,-1
           ,-1)
GO
SET IDENTITY_INSERT [fudgemart].[DimCustomer] OFF
go
-- Unknown Date Value
INSERT INTO [fudgemart].[DimDate]
           ([DateKey]
           ,[Date]
           ,[FullDateUSA]
           ,[DayOfWeek]
           ,[DayName]
           ,[DayOfMonth]
           ,[DayOfYear]
           ,[WeekOfYear]
           ,[MonthName]
           ,[MonthOfYear]
           ,[Quarter]
           ,[QuarterName]
           ,[Year]
           ,[IsAWeekday])
     VALUES
           (-1
           ,null
           ,'Unknown'
           ,0
           ,'Unknown'
           ,0
           ,0
           ,0
           ,'Unknown'
           ,0
           ,0
           ,'Unknown'
           ,0
           ,'?')
GO
/*-- unknown Employee
SET IDENTITY_INSERT [northwind].[DimEmployee] ON
GO
INSERT INTO [northwind].[DimEmployee]
           ([EmployeeKey]
		   ,[EmployeeID]
           ,[EmployeeName]
           ,[EmployeeTitle]
           ,[HireDateKey]
           ,[SupervisorID]
           ,[SupervisorName]
           ,[SupervsorTitle])
     VALUES
           (-1
		   ,-1
           ,'Unknown'
           ,'Unknown'
           ,-1
           ,-1
           ,'Unknown'
           ,'Unknown')
GO
SET IDENTITY_INSERT [northwind].[DimEmployee] OFF
GO
USE [ist722_mafudge_dw]
GO

*/
SET IDENTITY_INSERT [fudgemart].[DimProduct] ON
GO
INSERT INTO [fudgemart].[DimProduct]
           ([ProductKey]
		   ,[ProductID]
           ,[Product_Department]
           ,[Product_Name]
           ,[Product_Retail_Price]
           ,[Product_Wholesale_Price]
           ,[Product_isActive]
           ,[Product_addDate])
     VALUES
           (-1
		   ,-1
           ,'Unknown'
           ,'Unknown'
           ,-1
           ,-1
           ,0
           ,null)
GO
SET IDENTITY_INSERT [fudgemart].[DimProduct] OFF
GO

-- Default for Order
SET IDENTITY_INSERT [fudgemart].[DimOrder] ON
GO
INSERT INTO [fudgemart].[DimOrder]
           ([OrderKey]
		   ,[OrderID]
           ,[CustomerID]
           ,[Order_Date]
           ,[Shipped_Date]
           ,[Ship_Via])
     VALUES
           (-1
		   ,-1
           ,-1
           ,'01/01/1898 00:00:00'
           ,'01/01/1898 00:00:00'
           ,'Unknown')
GO
SET IDENTITY_INSERT [fudgemart].[DimOrder] OFF
GO

-- Default for Supplier
SET IDENTITY_INSERT [fudgemart].[DimOrderDetails] ON
GO
INSERT INTO [fudgemart].[DimOrderDetails]
           ([OrderDetailsKey]
		   ,[OrderID]
           ,[ProductID]
           ,[Order_Quantity])
     VALUES
           (-1
		   ,-1
           ,-1
           ,0)
GO
SET IDENTITY_INSERT [fudgemart].[DimOrderDetails] OFF
GO
PRINT 'SCRIPT COMPLETE'
GO
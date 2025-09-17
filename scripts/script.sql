-- 1.Create Master KEY 
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<YourStrongPasswordHere>';

-- 2.Create CREDENTIAL for going to Storage Account
CREATE DATABASE SCOPED CREDENTIAL [https://<'Your Storage Account'>.blob.core.windows.net/<'Your Container Name'>/]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<YOUR_SAS_TOKEN_HERE>';

-- 3.CREATE TABLE Customers 
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Email VARCHAR(255),
    Country VARCHAR(255)
);

-- 4.INSERT customers.csv
BULK INSERT dbo.Customers
FROM 'https://demoazureproject.blob.core.windows.net/raw-data/customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    CREDENTIAL = 'https://demoazureproject.blob.core.windows.net/raw-data/'
);

-- 5.Check Customers Table
SELECT * FROM dbo.Customers;

-- ============================================================
-- MEP Test Database — Sample data for script validation
-- ============================================================

-- Create test database
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'MEP_TestDB')
    CREATE DATABASE MEP_TestDB;
GO

USE MEP_TestDB;
GO

-- ============================================================
-- Schemas
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
    EXEC('CREATE SCHEMA dim');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
    EXEC('CREATE SCHEMA fact');
GO

-- ============================================================
-- Tables — dbo schema
-- ============================================================
CREATE TABLE dbo.Customers (
    CustomerID   INT IDENTITY(1,1) PRIMARY KEY,
    FirstName    NVARCHAR(100) NOT NULL,
    LastName     NVARCHAR(100) NOT NULL,
    Email        NVARCHAR(255) UNIQUE,
    CreatedDate  DATETIME2 DEFAULT GETDATE(),
    IsActive     BIT DEFAULT 1
);
GO

CREATE TABLE dbo.Products (
    ProductID    INT IDENTITY(1,1) PRIMARY KEY,
    ProductName  NVARCHAR(200) NOT NULL,
    CategoryName NVARCHAR(100),
    UnitPrice    DECIMAL(18,2) NOT NULL CHECK (UnitPrice >= 0),
    Stock        INT DEFAULT 0
);
GO

CREATE TABLE dbo.Orders (
    OrderID      INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID   INT NOT NULL FOREIGN KEY REFERENCES dbo.Customers(CustomerID),
    OrderDate    DATETIME2 DEFAULT GETDATE(),
    TotalAmount  DECIMAL(18,2),
    Status       NVARCHAR(50) DEFAULT 'Pending'
);
GO

CREATE TABLE dbo.OrderDetails (
    DetailID     INT IDENTITY(1,1) PRIMARY KEY,
    OrderID      INT NOT NULL FOREIGN KEY REFERENCES dbo.Orders(OrderID),
    ProductID    INT NOT NULL FOREIGN KEY REFERENCES dbo.Products(ProductID),
    Quantity     INT NOT NULL CHECK (Quantity > 0),
    UnitPrice    DECIMAL(18,2) NOT NULL
);
GO

-- Table WITHOUT primary key (for S03 no-PK detection)
CREATE TABLE dbo.AuditLog (
    LogDate      DATETIME2 DEFAULT GETDATE(),
    UserName     NVARCHAR(100),
    Action       NVARCHAR(500),
    Details      NVARCHAR(MAX)
);
GO

-- ============================================================
-- Tables — staging schema
-- ============================================================
CREATE TABLE staging.RawCustomers (
    RowID        INT IDENTITY(1,1),
    RawData      NVARCHAR(MAX),
    LoadDate     DATETIME2 DEFAULT GETDATE(),
    IsProcessed  BIT DEFAULT 0
);
GO

-- ============================================================
-- Tables — dim schema
-- ============================================================
CREATE TABLE dim.DimDate (
    DateKey      INT PRIMARY KEY,
    FullDate     DATE NOT NULL,
    Year         INT,
    Month        INT,
    Day          INT,
    Quarter      INT,
    DayOfWeek    INT,
    MonthName    NVARCHAR(20)
);
GO

CREATE TABLE dim.DimCustomer (
    CustomerKey  INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID   INT NOT NULL,
    FullName     NVARCHAR(200),
    Email        NVARCHAR(255),
    ValidFrom    DATETIME2 DEFAULT GETDATE(),
    ValidTo      DATETIME2,
    IsCurrent    BIT DEFAULT 1
);
GO

-- ============================================================
-- Tables — fact schema
-- ============================================================
CREATE TABLE fact.FactSales (
    SalesKey     INT IDENTITY(1,1) PRIMARY KEY,
    DateKey      INT FOREIGN KEY REFERENCES dim.DimDate(DateKey),
    CustomerKey  INT FOREIGN KEY REFERENCES dim.DimCustomer(CustomerKey),
    ProductID    INT,
    Quantity     INT,
    Amount       DECIMAL(18,2),
    LoadDate     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- Indexes
-- ============================================================
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID ON dbo.Orders(CustomerID);
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate ON dbo.Orders(OrderDate) INCLUDE (TotalAmount, Status);
CREATE NONCLUSTERED INDEX IX_OrderDetails_OrderProduct ON dbo.OrderDetails(OrderID, ProductID);
CREATE NONCLUSTERED INDEX IX_FactSales_DateCustomer ON fact.FactSales(DateKey, CustomerKey) INCLUDE (Amount);
GO

-- ============================================================
-- Views
-- ============================================================
CREATE OR ALTER VIEW dbo.vw_OrderSummary AS
SELECT
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(o.TotalAmount) AS TotalSpent
FROM dbo.Customers c
LEFT JOIN dbo.Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName;
GO

CREATE OR ALTER VIEW fact.vw_SalesDashboard AS
SELECT
    d.FullDate,
    d.Year,
    d.MonthName,
    dc.FullName AS Customer,
    fs.Quantity,
    fs.Amount
FROM fact.FactSales fs
JOIN dim.DimDate d ON fs.DateKey = d.DateKey
JOIN dim.DimCustomer dc ON fs.CustomerKey = dc.CustomerKey;
GO

-- ============================================================
-- Stored Procedures
-- ============================================================
CREATE OR ALTER PROCEDURE dbo.sp_GetCustomerOrders
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT o.OrderID, o.OrderDate, o.TotalAmount, o.Status,
           od.ProductID, p.ProductName, od.Quantity, od.UnitPrice
    FROM dbo.Orders o
    JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
    JOIN dbo.Products p ON od.ProductID = p.ProductID
    WHERE o.CustomerID = @CustomerID
    ORDER BY o.OrderDate DESC;
END;
GO

CREATE OR ALTER PROCEDURE staging.sp_ProcessRawCustomers
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Customers (FirstName, LastName, Email)
    SELECT
        JSON_VALUE(RawData, '$.firstName'),
        JSON_VALUE(RawData, '$.lastName'),
        JSON_VALUE(RawData, '$.email')
    FROM staging.RawCustomers
    WHERE IsProcessed = 0;

    UPDATE staging.RawCustomers SET IsProcessed = 1 WHERE IsProcessed = 0;
END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_RefreshFactSales
AS
BEGIN
    SET NOCOUNT ON;
    -- Simulated ETL refresh
    INSERT INTO fact.FactSales (DateKey, CustomerKey, ProductID, Quantity, Amount)
    SELECT
        CONVERT(INT, FORMAT(o.OrderDate, 'yyyyMMdd')),
        dc.CustomerKey,
        od.ProductID,
        od.Quantity,
        od.Quantity * od.UnitPrice
    FROM dbo.Orders o
    JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
    JOIN dim.DimCustomer dc ON o.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.FactSales fs
        WHERE fs.DateKey = CONVERT(INT, FORMAT(o.OrderDate, 'yyyyMMdd'))
          AND fs.CustomerKey = dc.CustomerKey
          AND fs.ProductID = od.ProductID
    );
END;
GO

-- ============================================================
-- Functions
-- ============================================================
CREATE OR ALTER FUNCTION dbo.fn_GetCustomerTotalSpent(@CustomerID INT)
RETURNS DECIMAL(18,2)
AS
BEGIN
    DECLARE @total DECIMAL(18,2);
    SELECT @total = SUM(TotalAmount) FROM dbo.Orders WHERE CustomerID = @CustomerID;
    RETURN ISNULL(@total, 0);
END;
GO

CREATE OR ALTER FUNCTION dbo.fn_GetOrderItems(@OrderID INT)
RETURNS TABLE
AS
RETURN (
    SELECT p.ProductName, od.Quantity, od.UnitPrice, od.Quantity * od.UnitPrice AS LineTotal
    FROM dbo.OrderDetails od
    JOIN dbo.Products p ON od.ProductID = p.ProductID
    WHERE od.OrderID = @OrderID
);
GO

-- ============================================================
-- Triggers
-- ============================================================
CREATE OR ALTER TRIGGER dbo.tr_Orders_UpdateTotal
ON dbo.Orders
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF UPDATE(Status)
    BEGIN
        INSERT INTO dbo.AuditLog (UserName, Action, Details)
        SELECT SUSER_NAME(), 'Order Status Changed',
               'OrderID=' + CAST(i.OrderID AS VARCHAR) + ' NewStatus=' + i.Status
        FROM inserted i;
    END
END;
GO

-- ============================================================
-- Extended Properties (descriptions)
-- ============================================================
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'Main customer table',
    @level0type=N'SCHEMA', @level0name=N'dbo',
    @level1type=N'TABLE',  @level1name=N'Customers';

EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'Customer email address (unique)',
    @level0type=N'SCHEMA', @level0name=N'dbo',
    @level1type=N'TABLE',  @level1name=N'Customers',
    @level2type=N'COLUMN', @level2name=N'Email';

EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'Sales fact table for BI analysis',
    @level0type=N'SCHEMA', @level0name=N'fact',
    @level1type=N'TABLE',  @level1name=N'FactSales';
GO

-- ============================================================
-- Sample Data
-- ============================================================
INSERT INTO dbo.Customers (FirstName, LastName, Email) VALUES
('Juan', 'Pérez', 'juan.perez@test.com'),
('María', 'García', 'maria.garcia@test.com'),
('Carlos', 'López', 'carlos.lopez@test.com');

INSERT INTO dbo.Products (ProductName, CategoryName, UnitPrice, Stock) VALUES
('Laptop HP', 'Electronics', 2500.00, 50),
('Mouse Logitech', 'Accessories', 45.00, 200),
('Monitor Dell 27"', 'Electronics', 800.00, 30);

INSERT INTO dbo.Orders (CustomerID, TotalAmount, Status) VALUES
(1, 2545.00, 'Completed'),
(2, 800.00, 'Shipped'),
(3, 90.00, 'Pending');

INSERT INTO dbo.OrderDetails (OrderID, ProductID, Quantity, UnitPrice) VALUES
(1, 1, 1, 2500.00), (1, 2, 1, 45.00),
(2, 3, 1, 800.00),
(3, 2, 2, 45.00);

INSERT INTO dim.DimDate (DateKey, FullDate, Year, Month, Day, Quarter, DayOfWeek, MonthName)
VALUES
(20240101, '2024-01-01', 2024, 1, 1, 1, 2, 'January'),
(20240201, '2024-02-01', 2024, 2, 1, 1, 4, 'February'),
(20240301, '2024-03-01', 2024, 3, 1, 1, 5, 'March');

INSERT INTO dim.DimCustomer (CustomerID, FullName, Email) VALUES
(1, 'Juan Pérez', 'juan.perez@test.com'),
(2, 'María García', 'maria.garcia@test.com'),
(3, 'Carlos López', 'carlos.lopez@test.com');
GO

-- Create a second database to test multi-DB discovery
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'MEP_Staging')
    CREATE DATABASE MEP_Staging;
GO

USE MEP_Staging;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl');
GO

CREATE TABLE dbo.ImportLog (
    LogID       INT IDENTITY(1,1) PRIMARY KEY,
    Source      NVARCHAR(200),
    ImportCount INT,
    LoadDate    DATETIME2 DEFAULT GETDATE()
);
GO

CREATE TABLE etl.StagingBuffer (
    BufferID  INT IDENTITY(1,1) PRIMARY KEY,
    RawJSON   NVARCHAR(MAX),
    Status    NVARCHAR(50) DEFAULT 'New'
);
GO

CREATE OR ALTER PROCEDURE etl.sp_LoadBuffer
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.ImportLog (Source, ImportCount)
    SELECT 'StagingBuffer', COUNT(*) FROM etl.StagingBuffer WHERE Status = 'New';
    UPDATE etl.StagingBuffer SET Status = 'Processed' WHERE Status = 'New';
END;
GO

PRINT '=== MEP Test Data Initialized Successfully ===';
GO

# Load necessary libraries
library(dplyr)
library(lubridate)
library(ggplot2)
library(readr)
library(RSQLite)
library(DBI)
library(readxl)

# Connect to the SQLite database
db <- dbConnect(RSQLite::SQLite(), dbname = "e_commerce_database.sqlite")

# Define SQL commands for creating tables using DDL or Data Defination language
sql_commands <- c(
  "CREATE TABLE IF NOT EXISTS Supplier (
    SUPPLIER_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    SUPPLIER_NAME VARCHAR(255) NOT NULL,
    SUPPLIER_PHONE VARCHAR(255),
    SUPPLIER_EMAIL VARCHAR(255)
  );",
  "CREATE TABLE IF NOT EXISTS Product (
    PRODUCT_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    PRODUCT_NAME VARCHAR(255) NOT NULL,
    PRODUCT_CATEGORY VARCHAR(255),
    PRICE FLOAT NOT NULL,
    SUPPLIER_ID VARCHAR(255) NOT NULL,
    FOREIGN KEY(SUPPLIER_ID) REFERENCES Supplier(SUPPLIER_ID)
  );",
  "CREATE TABLE IF NOT EXISTS Inventory (
    INVENTORY_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    STOCK INTEGER NOT NULL,
    SHELF_NO VARCHAR(255),
    PRODUCT_ID VARCHAR(255) NOT NULL,
    FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
  );",
  "CREATE TABLE IF NOT EXISTS Customer (
    CUSTOMER_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    CUSTOMER_FIRSTNAME VARCHAR(255) NOT NULL,
    CUSTOMER_LASTNAME VARCHAR(255) NOT NULL,
    CUSTOMER_EMAIL VARCHAR(255),
    CUSTOMER_PHONE VARCHAR(255),
    CUSTOMER_BIRTHDAY DATE,
    CUSTOMER_GENDER VARCHAR(50),
    SHIPMENT_ID VARCHAR(255),
    PAYMENT_ID VARCHAR(255)
  );",
  "CREATE TABLE IF NOT EXISTS Shipment (
    SHIPMENT_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    SHIPMENT_DATE DATE NOT NULL,
    SHIPMENT_ADDRESS VARCHAR(255) NOT NULL,
    SHIPMENT_CITY VARCHAR(255) NOT NULL,
    SHIPMENT_ZIPCODE VARCHAR(255) NOT NULL,
    SHIPMENT_COUNTRY VARCHAR(255) NOT NULL,
    CUSTOMER_ID VARCHAR(255) NOT NULL,
    PRODUCT_ID VARCHAR(255) NOT NULL,
    FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
    FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
  );",
  "CREATE TABLE IF NOT EXISTS Payment (
    PAYMENT_ID VARCHAR(255) PRIMARY KEY NOT NULL,
    PAYMENT_METHOD VARCHAR(255) NOT NULL,
    ORDER_AMOUNT FLOAT NOT NULL,
    PAYMENT_DATE DATE NOT NULL,
    BILLING_ADDRESS VARCHAR(255) NOT NULL,
    BILLING_CITY VARCHAR(255) NOT NULL,
    BILLING_ZIPCODE VARCHAR(255) NOT NULL,
    BILLING_COUNTRY VARCHAR(255) NOT NULL,
    CUSTOMER_ID VARCHAR(255) NOT NULL,
    PRODUCT_ID VARCHAR(255) NOT NULL,
    FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
    FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
  );"
)

# Execute each SQL command to create the tables
for(sql_command in sql_commands) {
  dbExecute(db, sql_command)
}



# ETL process for Extraqcting data and laoding Data
# Read data from CSV files

suppliers_data <- read_csv('supplier_ecommerce.csv')
products_data <- read_csv('products_ecommerce.csv')
inventories_data <- read_csv('inventory_ecommerce.csv')
customers_data <- read_csv('customers_ecommerce.csv')
shipments_data <- read_csv('shipment_ecommerce.csv')
payments_data <- read_csv('payments_ecommerce.csv')

# Insert data into tables
dbWriteTable(db, "Supplier", suppliers_data, overwrite = TRUE)
dbWriteTable(db, "Product", products_data, overwrite = TRUE)
dbWriteTable(db, "Inventory", inventories_data, overwrite = TRUE)
dbWriteTable(db, "Customer", customers_data, overwrite = TRUE)
dbWriteTable(db, "Shipment", shipments_data, overwrite = TRUE)
dbWriteTable(db, "Payment", payments_data, overwrite = TRUE)

# List all tables
tables <- dbListTables(db)
print(tables)



# Lets check for referential integrity, and do some validation checks
# to mention when creating databases, i took my time to create databases from scratch in order to maintain consistency between products, categories, suppliers and so on, and made sure of cross referencing

# Check for duplicate Customer records
duplicate_customers_query <- "
SELECT CUSTOMER_EMAIL, COUNT(*)
FROM Customer
GROUP BY CUSTOMER_EMAIL
HAVING COUNT(*) > 1;"
duplicate_customers <- dbGetQuery(db, duplicate_customers_query)
# Print the results
print("Duplicate Customers:")
print(duplicate_customers)

# Check for Products without a Category
products_without_category_query <- "
SELECT PRODUCT_ID, PRODUCT_NAME
FROM Product
WHERE PRODUCT_CATEGORY IS NULL OR PRODUCT_CATEGORY = '';"
products_without_category <- dbGetQuery(db, products_without_category_query)
print("Products without a Category:")
print(products_without_category)

# Check for Suppliers without contact information (both phone and email missing)
suppliers_without_contact_query <- "
SELECT SUPPLIER_ID, SUPPLIER_NAME
FROM Supplier
WHERE SUPPLIER_PHONE IS NULL AND SUPPLIER_EMAIL IS NULL;"
suppliers_without_contact <- dbGetQuery(db, suppliers_without_contact_query)
print("Suppliers without Contact Information:")
print(suppliers_without_contact)

# Check for Payments linked to non-existent Customers
invalid_customer_payments_query <- "
SELECT pa.PAYMENT_ID, pa.CUSTOMER_ID
FROM Payment pa
LEFT JOIN Customer c ON pa.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL;"
invalid_customer_payments <- dbGetQuery(db, invalid_customer_payments_query)
print("Payments linked to Non-existent Customers:")
print(invalid_customer_payments)

# Check Inventory for negative stock values
negative_stock_query <- "
SELECT PRODUCT_ID, STOCK
FROM Inventory
WHERE STOCK < 0;"
negative_stock <- dbGetQuery(db, negative_stock_query)
print("Inventory Items with Negative Stock:")
print(negative_stock)





# Lets use DQL or Data query language
# lets try some basic Queries

customers <- DBI::dbGetQuery(db, "SELECT * FROM CUSTOMER;")
# Query 1: Select all records from the Customer table
customers <- dbGetQuery(db, "SELECT * FROM CUSTOMER;")
# Assuming you want to print the results to inspect them
print("Customers:")
print(customers)

# Query 2: Select specific columns from the Customer table
customer_names <- dbGetQuery(db, "SELECT CUSTOMER_ID, CUSTOMER_FIRSTNAME, CUSTOMER_LASTNAME FROM CUSTOMER;")
print("Customer Names:")
print(customer_names)

# Query 3: Select product names and prices from the Product table
product_prices <- dbGetQuery(db, "SELECT PRODUCT_NAME, PRICE FROM PRODUCT;")
print("Product Prices:")
print(product_prices)

# Query 4: Select all records from the Supplier table with a limit of 5
suppliers_limit <- dbGetQuery(db, "SELECT * FROM SUPPLIER LIMIT 5;")
print("Suppliers (Limit 5):")
print(suppliers_limit)

# Query 5: Select the top five most expensive items
top_expensive_items <- dbGetQuery(db, "SELECT DISTINCT PRODUCT_ID, PRODUCT_NAME, PRICE FROM Product WHERE PRICE > 1000 ORDER BY PRICE DESC LIMIT 5;")
print("Top Expensive Items:")
print(top_expensive_items)

# Query 6: Select the five cheapest items
cheapest_items <- dbGetQuery(db, "SELECT DISTINCT PRODUCT_ID, PRODUCT_NAME, PRICE FROM Product ORDER BY PRICE ASC LIMIT 5;")
print("Cheapest Items:")
print(cheapest_items)


# Some Advanced Queries

# Total Sales
total_sales_query <- "SELECT SUM(ORDER_AMOUNT) AS Total_Sales FROM Payment;"
total_sales <- dbGetQuery(db, total_sales_query)
# Print the results
print("Total Sales:")
print(total_sales)

# Most Sold Item
most_sold_item_query <- "
SELECT p.PRODUCT_NAME, p.PRICE, COUNT(p.PRODUCT_ID) AS Units_Sold, SUM(pay.ORDER_AMOUNT) AS Total_Sales
FROM Product p
JOIN Payment pay ON p.PRODUCT_ID = pay.PRODUCT_ID
GROUP BY p.PRODUCT_ID
ORDER BY Units_Sold DESC, Total_Sales DESC
LIMIT 1;"
most_sold_item <- dbGetQuery(db, most_sold_item_query)
print("Most Sold Item:")
print(most_sold_item)

# Category with Most Shipments
most_shipments_category_query <- "
SELECT p.PRODUCT_CATEGORY, COUNT(s.SHIPMENT_ID) AS Total_Shipments
FROM Product p
JOIN Shipment s ON p.PRODUCT_ID = s.PRODUCT_ID
GROUP BY p.PRODUCT_CATEGORY
ORDER BY Total_Shipments DESC
LIMIT 1;"
most_shipments_category <- dbGetQuery(db, most_shipments_category_query)
print("Category with Most Shipments:")
print(most_shipments_category)

# Category with Least Shipments
least_shipments_category_query <- "
SELECT p.PRODUCT_CATEGORY, COUNT(s.SHIPMENT_ID) AS Total_Shipments
FROM Product p
JOIN Shipment s ON p.PRODUCT_ID = s.PRODUCT_ID
GROUP BY p.PRODUCT_CATEGORY
ORDER BY Total_Shipments ASC
LIMIT 1;"
least_shipments_category <- dbGetQuery(db, least_shipments_category_query)
print("Category with Least Shipments:")
print(least_shipments_category)

# Customers with Highest Spending
highest_spending_customers_query <- "
SELECT c.CUSTOMER_FIRSTNAME, c.CUSTOMER_LASTNAME, p.PRODUCT_NAME, SUM(pa.ORDER_AMOUNT) AS Total_Spent
FROM Customer c
JOIN Payment pa ON c.CUSTOMER_ID = pa.CUSTOMER_ID
JOIN Product p ON pa.PRODUCT_ID = p.PRODUCT_ID
GROUP BY c.CUSTOMER_ID, p.PRODUCT_NAME
ORDER BY Total_Spent DESC
LIMIT 10;"
highest_spending_customers <- dbGetQuery(db, highest_spending_customers_query)
print("Customers with the Highest Spending:")
print(highest_spending_customers)

# Inventory Levels Below Reorder Threshold, assuming threshold is 10
low_inventory_levels_query <- "
SELECT p.PRODUCT_NAME, i.STOCK
FROM Inventory i
JOIN Product p ON i.PRODUCT_ID = p.PRODUCT_ID
WHERE i.STOCK < 10;"
low_inventory_levels <- dbGetQuery(db, low_inventory_levels_query)
print("Low Inventory Levels:")
print(low_inventory_levels)


# Close the database connection
dbDisconnect(db)

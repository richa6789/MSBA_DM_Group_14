library(dplyr)
library(lubridate)
library(ggplot2)
library(readr)
library(RSQLite)
library(DBI)
library(readxl)


db <- dbConnect(RSQLite::SQLite(), dbname = "e_commerce_database.db")
# consider changing above to the way nikois do in his class
# Token(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)

table_exists <- function(db, table_name) {
  dbExistsTable(db, table_name)
}

# Create Supplier table
if (table_exists(db, 'Supplier')) {
  dbExecute(db, "DROP TABLE Supplier")
  cat("Existing table 'Supplier' dropped.\n")
}

dbExecute(db, "CREATE TABLE Supplier (
                  SUPPLIER_ID TEXT PRIMARY KEY,
                  SUPPLIER_NAME TEXT,
                  SUPPLIER_PHONE TEXT,
                  SUPPLIER_EMAIL TEXT
                )")

# Create Product table if it doesn't exist
if (table_exists(db, 'Product')) {
  dbExecute(db, "DROP TABLE Product")
  cat("Existing table 'Product' dropped.\n")
}

dbExecute(db, "CREATE TABLE Product (
                  PRODUCT_ID TEXT PRIMARY KEY,
                  PRODUCT_NAME TEXT,
                  PRODUCT_CATEGORY TEXT,
                  PRICE NUMERIC,
                  SUPPLIER_ID TEXT,
                  FOREIGN KEY(SUPPLIER_ID) REFERENCES Supplier(SUPPLIER_ID)
                )")

# Create Inventory table if it doesn't exist
if (table_exists(db, 'Inventory')) {
  dbExecute(db, "DROP TABLE Inventory")
  cat("Existing table 'Inventory' dropped.\n")
}

dbExecute(db, "CREATE TABLE Inventory (
                INVENTORY_ID TEXT PRIMARY KEY,
                STOCK INTEGER,
                SHELF_NO TEXT,
                PRODUCT_ID TEXT,
                FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
              )")

# Create Customer table if it doesn't exist
if (table_exists(db, 'Customer')) {
  dbExecute(db, "DROP TABLE Customer")
  cat("Existing table 'Customer' dropped.\n")
}

dbExecute(db, "CREATE TABLE Customer (
                CUSTOMER_ID TEXT PRIMARY KEY,
                CUSTOMER_FIRSTNAME TEXT,
                CUSTOMER_LASTNAME TEXT,
                CUSTOMER_EMAIL TEXT,
                CUSTOMER_PHONE TEXT,
                CUSTOMER_BIRTHDAY DATE,
                CUSTOMER_GENDER TEXT,
                SHIPMENT_ID TEXT,
                PAYMENT_ID TEXT
              )")

# Create Shipping table if it doesn't exist
if (table_exists(db, 'Shipping')) {
  dbExecute(db, "DROP TABLE Shipping")
  cat("Existing table 'Shipping' dropped.\n")
}

dbExecute(db, "CREATE TABLE Shipping (
                SHIPMENT_ID TEXT PRIMARY KEY,
                SHIPMENT_DATE DATE,
                SHIPMENT_ADDRESS TEXT,
                SHIPMENT_CITY TEXT,
                SHIPMENT_ZIPCODE TEXT,
                BILLING_COUNTRY TEXT,
                CUSTOMER_ID TEXT,
                PRODUCT_ID TEXT,
                FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
                FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
              )")

# Create Payment table if it doesn't exist
if (table_exists(db, 'Payment')) {
  dbExecute(db, "DROP TABLE Payment")
  cat("Existing table 'Payment' dropped.\n")
}

dbExecute(db, "CREATE TABLE Payment (
                PAYMENT_ID TEXT PRIMARY KEY,
                PAYMENT_METHOD TEXT,
                ORDER_AMOUNT NUMERIC,
                PAYMENT_DATE DATE,
                BILLING_ADDRESS TEXT,
                BILLING_CITY TEXT,
                BILLING_ZIPCODE TEXT,
                BILLING_COUNTRY TEXT,
                CUSTOMER_ID TEXT,
                PRODUCT_ID TEXT,
                FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
                FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
              )")


# Read Excel files into R data frames
# Replace 'path_to_excel_file.xlsx' with the actual path to your Excel files
# Read CSV files into R data frames
# Replace the paths with the actual paths to your CSV files
suppliers_data <- read_csv('DM_project/supplier_ecommerce.csv')
products_data <- read_csv('DM_project/products_ecommerce.csv')
inventory_data <- read_csv('DM_project/inventory_ecommerce.csv')
customers_data <- read_csv('DM_project/customers_ecommerce.csv')
shipment_data <- read_csv('DM_project/shipment_ecommerce.csv')
payments_data <- read_csv('DM_project/payments_ecommerce.csv')
# For Excel files, use read_excel instead of read_csv


# Establish a connection to the database
db <- dbConnect(RSQLite::SQLite(), dbname = "e_commerce_database.db")

# Function to check if a table exists in the database
table_exists <- function(db, table_name) {
  exists <- dbGetQuery(db, paste0("SELECT name FROM sqlite_master WHERE type='table' AND name='", table_name, "';"))
  nrow(exists) > 0
}

# Function to create a table if it does not exist
create_table_if_not_exists <- function(db, table_name, create_table_sql) {
  if (!table_exists(db, table_name)) {
    dbExecute(db, create_table_sql)
  }
}

# SQL statements to create tables
create_supplier_table_sql <- "CREATE TABLE Supplier (
  SUPPLIER_ID TEXT PRIMARY KEY,
  SUPPLIER_NAME TEXT,
  SUPPLIER_PHONE TEXT,
  SUPPLIER_EMAIL TEXT
)"

create_product_table_sql <- "CREATE TABLE Product (
  PRODUCT_ID TEXT PRIMARY KEY,
  PRODUCT_NAME TEXT,
  PRODUCT_CATEGORY TEXT,
  PRICE NUMERIC,
  SUPPLIER_ID TEXT,
  FOREIGN KEY(SUPPLIER_ID) REFERENCES Supplier(SUPPLIER_ID)
)"

create_inventory_table_sql <- "CREATE TABLE Inventory (
  INVENTORY_ID TEXT PRIMARY KEY,
  STOCK INTEGER,
  SHELF_NO TEXT,
  PRODUCT_ID TEXT,
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
)"

create_customer_table_sql <- "CREATE TABLE Customer (
  CUSTOMER_ID TEXT PRIMARY KEY,
  CUSTOMER_FIRSTNAME TEXT,
  CUSTOMER_LASTNAME TEXT,
  CUSTOMER_EMAIL TEXT,
  CUSTOMER_PHONE TEXT,
  CUSTOMER_BIRTHDAY DATE,
  CUSTOMER_GENDER TEXT,
  SHIPMENT_ID TEXT,
  PAYMENT_ID TEXT
)"

create_shipping_table_sql <- "CREATE TABLE Shipping (
  SHIPMENT_ID TEXT PRIMARY KEY,
  SHIPMENT_DATE DATE,
  SHIPMENT_ADDRESS TEXT,
  SHIPMENT_CITY TEXT,
  SHIPMENT_ZIPCODE TEXT,
  BILLING_COUNTRY TEXT,
  CUSTOMER_ID TEXT,
  PRODUCT_ID TEXT,
  FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
)"

create_payment_table_sql <- "CREATE TABLE Payment (
  PAYMENT_ID TEXT PRIMARY KEY,
  PAYMENT_METHOD TEXT,
  ORDER_AMOUNT NUMERIC,
  PAYMENT_DATE DATE,
  BILLING_ADDRESS TEXT,
  BILLING_CITY TEXT,
  BILLING_ZIPCODE TEXT,
  BILLING_COUNTRY TEXT,
  CUSTOMER_ID TEXT,
  PRODUCT_ID TEXT,
  FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
)"

# Create tables if they do not exist
create_table_if_not_exists(db, 'Supplier', create_supplier_table_sql)
create_table_if_not_exists(db, 'Product', create_product_table_sql)
create_table_if_not_exists(db, 'Inventory', create_inventory_table_sql)
create_table_if_not_exists(db, 'Customer', create_customer_table_sql)
create_table_if_not_exists(db, 'Shipping', create_shipping_table_sql)
create_table_if_not_exists(db, 'Payment', create_payment_table_sql)

# Read data from CSV files
suppliers_data <- read_csv('supplier_ecommerce.csv')
products_data <- read_csv('products_ecommerce.csv')
inventory_data <- read_csv('inventory_ecommerce.csv')
customers_data <- read_csv('customers_ecommerce.csv')
shipment_data <- read_csv('shipment_ecommerce.csv')
payments_data <- read_csv('payments_ecommerce.csv')


# Write data to the database
dbWriteTable(db, 'Supplier', suppliers_data, append = FALSE, overwrite = TRUE)
dbWriteTable(db, 'Product', products_data, append = FALSE, overwrite = TRUE)
dbWriteTable(db, 'Inventory', inventory_data, append = FALSE, overwrite = TRUE)
dbWriteTable(db, 'Customer', customers_data, append = FALSE, overwrite = TRUE)
dbWriteTable(db, 'Shipping', shipment_data, append = FALSE, overwrite = TRUE)
dbWriteTable(db, 'Payment', payments_data, append = FALSE, overwrite = TRUE)


#consider Ads
#dbWriteTable(db, 'Ads', ads_data, append = TRUE, overwrite = FALSE)


# Function to get the first ten records of a given table
get_first_ten_records <- function(db, table_name) {
  query <- sprintf("SELECT * FROM %s LIMIT 10", table_name)
  dbGetQuery(db, query)
}

# List of your table names
table_names <- c('Supplier', 'Product', 'Inventory', 'Customer', 'Shipping', 'Payment')

# Using lapply to apply the function across all your tables
first_ten_records_list <- lapply(table_names, get_first_ten_records, db = db)

# Now you can print or view the records
# This will print the first ten records from each table
first_ten_records_list

# Additional queries
product_count <- dbGetQuery(db, "SELECT COUNT(*) AS TotalProducts FROM Product")


# Retrieve the first 10 records from the Supplier table
suppliers_first_10 <- dbGetQuery(db, "SELECT * FROM Supplier LIMIT 10")

# Count the number of records in the Product table
product_count <- dbGetQuery(db, "SELECT COUNT(*) AS TotalProducts FROM Product")

# Retrieve all products with a price greater than 100

products_above_100 <- dbGetQuery(db, "SELECT * FROM Product WHERE PRICE > 100")

# Retrieve and print additional data
suppliers_first_10 <- dbGetQuery(db, "SELECT * FROM Supplier LIMIT 10")
distinct_categories <- dbGetQuery(db, "SELECT DISTINCT PRODUCT_CATEGORY FROM Product")


print(suppliers_first_10)
print(product_count)
print(products_above_100)
print(distinct_categories)


products_with_suppliers <- dbGetQuery(db, "
  SELECT p.PRODUCT_ID, p.PRODUCT_NAME, p.PRICE, s.SUPPLIER_NAME 
  FROM Product p
  INNER JOIN Supplier s ON p.SUPPLIER_ID = s.SUPPLIER_ID
")

# Printing query results
print(suppliers_first_10)
print(product_count)
print(products_above_100)
print(distinct_categories)
print(products_with_suppliers)




dbDisconnect(db)
library(dplyr)
library(lubridate)
library(ggplot2)
library(readr)
library(RSQLite)
library(DBI)
library(readxl)



db <- dbConnect(RSQLite::SQLite(), dbname = "e_commerce_database.db")

# Token(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)



# Create Supplier table
dbExecute(db, "CREATE TABLE Supplier (
  SUPPLIER_ID TEXT PRIMARY KEY,
  SUPPLIER_NAME TEXT,
  SUPPLIER_PHONE TEXT,
  SUPPLIER_EMAIL TEXT
)")

# Create Product table
dbExecute(db, "CREATE TABLE Product (
  PRODUCT_ID TEXT PRIMARY KEY,
  PRODUCT_NAME TEXT,
  PRODUCT_CATEGORY TEXT,
  PRICE NUMERIC,
  SUPPLIER_ID TEXT,
  FOREIGN KEY(SUPPLIER_ID) REFERENCES Supplier(SUPPLIER_ID)
)")

# Create Inventory table
dbExecute(db, "CREATE TABLE Inventory (
  INVENTORY_ID TEXT PRIMARY KEY,
  STOCK INTEGER,
  SHELF_NO TEXT,
  PRODUCT_ID TEXT,
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
)")

# Create Customer table
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

# Create Shipping table
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

# Create Payment table
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
suppliers_data <- read_csv('supplier_ecommerce.csv')
products_data <- read_csv('products_ecommerce.csv')
inventory_data <- read_csv('inventory_ecommerce.csv')
customers_data <- read_csv('customers_ecommerce.csv')
shipment_data <- read_csv('shipment_ecommerce.csv')
payments_data <- read_csv('payments_ecommerce.csv')
# For Excel files, use read_excel instead of read_csv


# Write the data frames to the SQLite database
dbWriteTable(db, 'Supplier', suppliers_data, append = TRUE, overwrite = FALSE)
dbWriteTable(db, 'Product', products_data, append = TRUE, overwrite = FALSE)
dbWriteTable(db, 'Inventory', inventory_data, append = TRUE, overwrite = FALSE)
dbWriteTable(db, 'Customer', customers_data, append = TRUE, overwrite = FALSE)
dbWriteTable(db, 'Shipping', shipment_data, append = TRUE, overwrite = FALSE)
dbWriteTable(db, 'Payment', payments_data, append = TRUE, overwrite = FALSE)

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

products_above_100 <- dbGetQuery(db, "SELECT * FROM Product WHERE PRICE > 100")
product_count <- dbGetQuery(db, "SELECT COUNT(*) AS TotalProducts FROM Product")







# Retrieve the first 10 records from the Supplier table
suppliers_first_10 <- dbGetQuery(db, "SELECT * FROM Supplier LIMIT 10")

# Count the number of records in the Product table
product_count <- dbGetQuery(db, "SELECT COUNT(*) AS TotalProducts FROM Product")

# Retrieve all products with a price greater than 100
products_above_100 <- dbGetQuery(db, "SELECT * FROM Product WHERE PRICE > 100")

# Retrieve all distinct categories from the Product table
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

print(products_with_suppliers)



dbDisconnect(db)

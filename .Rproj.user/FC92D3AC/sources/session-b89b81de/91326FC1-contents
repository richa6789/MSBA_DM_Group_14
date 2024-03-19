# Load necessary libraries
library(readr)
library(RSQLite)
library(DBI)


# Connect to the SQLite database
db <- dbConnect(RSQLite::SQLite(), dbname = "e_commerce_database.db")

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


dbGetQuery(db, "SELECT COUNT(*)
FROM Customer;")

# ETL process for Extraqcting data and laoding Data
# Read data from CSV files

suppliers_data <- read_csv('supplier_ecommerce.csv')
products_data <- read_csv('products_ecommerce.csv')
inventories_data <- read_csv('inventory_ecommerce.csv')
customers_data <- read_csv('customers_ecommerce.csv')
shipments_data <- read_csv('shipment_ecommerce.csv')
payments_data <- read_csv('payments_ecommerce.csv')


#Data Validation

#Customer check

##customer id
unique_customer_id <- nrow(customers_data) == length(unique(customers_data$CUSTOMER_ID))
customers_data <- customers_data[unique_customer_id, ]

validate_customer_id <- function(customer_id) {
  !is.na(customer_id) && substr(customer_id, 1, 1) == "C" && nchar(customer_id) == 6 && grepl("^[A-Za-z0-9]+$", customer_id)
}

# Apply the validation function to the CUSTOMER_ID column
valid_customer_id <- sapply(customers_data$CUSTOMER_ID, validate_customer_id)
customers_data <- customers_data[valid_customer_id, ]

# Check which entries fail validation
invalid_entries <- customers_data[!valid_customer_id, "CUSTOMER_ID"]

## Phone Number - Numeric, Length and Uniqueness
validate_phone_number <- function(phone_number) {
  all(grepl("^[0-9]{9}$", phone_number) & !duplicated(phone_number))
}

customers_data$CUSTOMER_PHONE <- as.integer(customers_data$CUSTOMER_PHONE)

# Apply the validation function to the CUSTOMER_PHONE column
valid_phone_number <- sapply(customers_data$CUSTOMER_PHONE, validate_phone_number)
invalid_entries <- customers_data[!valid_phone_number, "CUSTOMER_PHONE"]
customers_data <- customers_data[valid_phone_number, ]

##email
### Define domain list
validate_email <- function(email) {
  domains <- c("gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "icloud.com")
  all(grepl("@", email) & grepl(paste(domains, collapse="|"), email))
}

### Apply the validation function to the CUSTOMER_EMAIL column
valid_email <- sapply(customers_data$CUSTOMER_EMAIL, validate_email)
invalid_entries <- customers_data[!valid_email, "CUSTOMER_EMAIL"]
invalid_entries
customers_data <- customers_data[valid_email, ]

## First Name - Characters and Max Length
validate_firstname <- function(firstname) {
  !is.na(firstname) && all(grepl("^[[:alpha:]]+$", firstname)) && nchar(firstname) <= 25
}

### Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_firstname <- sapply(customers_data$CUSTOMER_FIRSTNAME, validate_firstname)

### Keep only the rows with valid first names
customers_data <- customers_data[valid_firstname, ]

## Last Name

validate_lastname <- function(lastname) {
  !is.na(lastname) && all(grepl("^[-'[:alpha:][:space:]]+$", lastname)) && nchar(lastname) <= 25
}

### Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_lastname <- sapply(customers_data$CUSTOMER_LASTNAME, validate_lastname)

###check for invalid entries
invalid_entries <- customers_data[!valid_lastname, "CUSTOMER_LASTNAME"]
invalid_entries

### Keep only the rows with valid first names
customers_data <- customers_data[valid_lastname, ]



##Gender check

###function for check
validate_gender <- function(gender) {
  !is.na(gender) && gender %in% c("Male", "Female", "Other")
}

###filtering invalid data
valid_gender <- sapply(customers_data$CUSTOMER_GENDER, validate_gender)
customers_data <- customers_data[valid_gender, ]

## Birthday
validate_date <- function(date) {
  !is.na(date) && !is.na(as.Date(date, format = "%d/%m/%Y", tryFormats = c("%d/%m/%Y")))
}


#Products check

##Product ID
unique_product_id <- nrow(products_data) == length(unique(products_data$PRODUCT_ID))

validate_product_id <- function(product_id) {
  !is.na(product_id) && substr(product_id, 1, 1) == "P" && nchar(product_id) == 4 && grepl("^[A-Za-z0-9]+$", product_id)
}

valid_product_id <- sapply(products_data$PRODUCT_ID, validate_product_id)
products_data <- products_data[valid_product_id, ]

##PRICE
products_data$PRICE <- as.integer(products_data$PRICE)
validate_price <- function(price) {
  !is.na(price) && grepl("^[0-9]{1,10}$", price)
}

### Apply the validation function to the PRICE column
valid_price <- sapply(products_data$PRICE, validate_price)

### Check which entries fail validation
invalid_entries <- products_data[!valid_price, "PRICE"]
invalid_entries
products_data <- products_data[valid_price, ]

##Product Category
validate_category <- function(category) {
  !is.na(category) && all(grepl("^[-'[:alpha:]&[:space:]]+$", category)) && nchar(category) <= 100
}

### Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_category <- sapply(products_data$PRODUCT_CATEGORY, validate_category)

###check for invalid entries
invalid_entries <- products_data[!valid_category, "PRODUCT_CATEGORY"]

products_data <- products_data[valid_category, ]

##Product name
validate_product_name <- function(product_name) {
  !is.na(product_name) && all(grepl("^[-'[:alnum:]&[:space:],.()\"\\\\]+$", product_name)) && nchar(product_name) <= 100
}
### Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_product_name <- sapply(products_data$PRODUCT_NAME, validate_product_name)

###check for invalid entries
invalid_entries <- products_data[!valid_product_name, "PRODUCT_NAME"]
invalid_entries

products_data <- products_data[valid_product_name, ]

##Supplier data
unique_supplier_id <- nrow(suppliers_data) == length(unique(suppliers_data$SUPPLIER_ID))

validate_supplier_id <- function(supplier_id) {
  !is.na(supplier_id) && substr(supplier_id, 1, 1) == "S" && nchar(supplier_id) == 4 && grepl("^[A-Za-z0-9]+$", supplier_id)
}

valid_supplier_id <- sapply(suppliers_data$SUPPLIER_ID, validate_supplier_id)
suppliers_data <- suppliers_data[valid_supplier_id, ]

##Supplier phone
valid_sphone_number <- sapply(suppliers_data$SUPPLIER_PHONE, validate_phone_number)
invalid_entries <- suppliers_data[!valid_sphone_number, "SUPPLIER_PHONE"]
suppliers_data <- suppliers_data[valid_sphone_number, ]

##Supplier email

# Apply the validation function to the SUPPLIER_EMAIL column
valid_semail <- sapply(suppliers_data$SUPPLIER_EMAIL, validate_email)
invalid_entries <- suppliers_data[!valid_semail, "SUPPLIER_PHONE"]
suppliers_data <- suppliers_data[valid_semail, ]

##Supplier name
validate_supplier_name <- function(name) {
  !is.na(name) && all(grepl("^[-'[:alpha:]&[:space:],.]+$", name))
}

### Apply the validation function to the SUPPLIER_NAME column
valid_supplier_name <- sapply(suppliers_data$SUPPLIER_NAME, validate_supplier_name)

### Check which entries fail validation
invalid_entries <- suppliers_data[!valid_supplier_name, "SUPPLIER_NAME"]
suppliers_data <- suppliers_data[valid_supplier_name, ]

#Inventory

##inventory id
unique_inventory_id <- nrow(inventories_data) == length(unique(inventories_data$INVENTORY_ID))

validate_inventory_id <- function(inventory_id) {
  !is.na(inventory_id) && substr(inventory_id, 1, 3) == "INV" && nchar(inventory_id) <= 7 && grepl("^[A-Za-z0-9]+$", inventory_id)
}

valid_inventory_id <- sapply(inventories_data$INVENTORY_ID, validate_inventory_id)
invalid_entries <- inventories_data[!valid_inventory_id, "INVENTORY_ID"]
inventories_data <- inventories_data[valid_inventory_id, ]

##stock
valid_stock <- sapply(inventories_data$STOCK, validate_price)
invalid_entries <- inventories_data[!valid_stock, "STOCK"]
inventories_data <- inventories_data[valid_stock, ]

##shelf no.
validate_shelf_no <- function(shelf_no) {
  !is.na(shelf_no) && nchar(shelf_no) == 2 && 
    grepl("^[A-Z][1-9]$", shelf_no)
}

# Apply the validation function to the SHELF_NO column
valid_shelf_no <- sapply(inventories_data$SHELF_NO, validate_shelf_no)

# Check which entries fail validation
invalid_entries <- inventories_data[!valid_shelf_no, "SHELF_NO"]
inventories_data <- inventories_data[valid_shelf_no, ]

#Shipments

##ID
unique_shipment_id <- nrow(shipments_data) == length(unique(shipments_data$SHIPMENT_ID))

validate_shipment_id <- function(shipment_id) {
  !is.na(shipment_id) && substr(shipment_id, 1, 1) == "E" && nchar(shipment_id) <= 10 && grepl("^[A-Za-z0-9]+$", shipment_id)
}

valid_shipment_id <- sapply(shipments_data$SHIPMENT_ID, validate_shipment_id)
invalid_entries <- shipments_data[!valid_shipment_id, "SHIPMENT_ID"]
shipments_data <- shipments_data[valid_shipment_id, ]

## shipment date
valid_shipment_date <- sapply(shipments_data$SHIPMENT_DATE, validate_date)

# Check which entries fail validation
invalid_entries <- shipments_data[!valid_shipment_date, "SHIPMENT_DATE"]
shipments_data <- shipments_data[valid_shipment_date, ]

shipments_data
## shipping city
unique(shipments_data$SHIPMENT_ZIPCODE)

valid_uk_cities <- c("London", "Birmingham", "Manchester", "Glasgow", "Edinburgh", "Liverpool", "Bristol", "Belfast", "Leeds", "Newcastle upon Tyne", "Sheffield", "Cardiff", "Nottingham", "Southampton", "Oxford", "Cambridge", "Aberdeen", "York", "Brighton", "Portsmouth", "Leicester", "Coventry", "Stoke-on-Trent", "Plymouth", "Wolverhampton", "Derby", "Swansea", "Hull", "Reading", "Preston", "Milton Keynes", "Sunderland", "Norwich", "Luton", "Swindon", "Warrington", "Dudley", "Bournemouth", "Peterborough", "Southend-on-Sea", "Walsall", "Colchester", "Middlesbrough", "Blackburn")

valid_uk_zipcodes <- c("CV35", "NE46", "BS14", "BD23", "RH5", "S33", "CT15", "DN36", "WC1B", "BT66", "LE15", "NN4", "NG22", "TF6", "WC2H", "AB55", "DL10", "SN13", "NG34", "SY4", "LN6", "BS37", "L33", "RG20", "LS6", "AB56", "BS41", "WF9", "B40", "OX12", "NR34", "N3", "DT10", "M14", "M34", "CH48", "G4", "ST20", "NR29", "GL54", "DL8", "NN11", "PH43", "W1F", "SN1", "EC3M", "EH9", "CB4", "SW19", "S8", "LE14", "S1", "PR1", "EH52", "SG4", "LE16", "CT16", "L74", "BT2", "LS9", "SW1E", "B12", "BH21", "KW10", "AB39", "GU32", "EC1V", "OX7", "DN21", "DN22", "IV1", "BD7")

### Function to validate shipment city and zipcode
validate_shipment_city <- function(city) {
  return(city %in% valid_uk_cities)
}

### Function to validate shipment zipcode
validate_shipment_zipcode <- function(zipcode) {
  return(zipcode %in% valid_uk_zipcodes)
}

### Check the validity of each shipment city and zipcode
valid_city <- sapply(shipments_data$SHIPMENT_CITY, validate_shipment_city)
valid_zipcode <- sapply(shipments_data$SHIPMENT_ZIPCODE, validate_shipment_zipcode)

# Filter out invalid entries
invalid_entries <- shipments_data[!valid_city | !valid_zipcode, ]

## Shipment country
validate_shipment_country <- function(country) {
  return(country %in% c("UK", "United Kingdom"))
}

# Check the validity of each shipment country
valid_country <- sapply(shipments_data$BILLING_COUNTRY, validate_shipment_country)

# Filter out invalid entries
invalid_entries <- shipments_data[!valid_country, ]

#Payments

##ID
unique_payment_id <- nrow(payments_data) == length(unique(payments_data$PAYMENT_ID))

validate_payment_id <- function(payment_id) {
  !is.na(payment_id) && substr(payment_id, 1, 1) == "P" && nchar(payment_id) <= 10 && grepl("^[A-Za-z0-9]+$", payment_id)
}

valid_payment_id <- sapply(payments_data$PAYMENT_ID, validate_payment_id)
invalid_entries <- payments_data[!valid_payment_id, "PAYMENT_ID"]
payments_data <- payments_data[valid_payment_id, ]

## Payment Method
validate_payment_method <- function(payment_method) {
  payment_methods <- c("Credit card", "Klarna", "Apple Pay", "PayPal", "Debit card")
  !is.na(payment_method) && payment_method %in% payment_methods
}

### Apply the validation function to the PAYMENT_METHOD column
valid_payment_method <- sapply(payments_data$PAYMENT_METHOD, validate_payment_method)

### Check which entries fail validation
invalid_entries <- payments_data[!valid_payment_method, "PAYMENT_METHOD"]
payments_data <- payments_data[valid_payment_method, ]

## Order Amount
valid_amount <- sapply(payments_data$ORDER_AMOUNT, validate_price)
invalid_entries <- payments_data[!valid_amount, "ORDER_AMOUNT"]
payments_data <- payments_data[valid_amount, ]

##Payment Date
valid_payment_date <- sapply(payments_data$PAYMENT_DATE, validate_date)

# Check which entries fail validation
invalid_entries <- payments_data[!valid_payment_date, "PAYMENT_DATE"]
payments_data <- payments_data[valid_payment_date, ]



data_exists <- function(db, table_name, criteria) {
  query <- paste0("SELECT COUNT(*) FROM ", table_name, " WHERE ", criteria, ";")
  result <- dbGetQuery(db, query)
  return(result[[1]] > 0)
}

# Function to insert data into a database table for each entity
insert_data <- function(db, table_name, data_frame, unique_column) {
  for (i in 1:nrow(data_frame)) {
    # Extract row data
    row_data <- data_frame[i, ]
    
    # Extract unique value for validation
    unique_value <- row_data[[unique_column]]
    
    # Check if data already exists
    if (data_exists(db, table_name, paste0(unique_column, " = '", unique_value, "'"))) {
      print(paste("Data with", unique_column, unique_value, "already exists in", table_name, ". Skipping insertion."))
    } else {
      # Insert data into the table
      dbWriteTable(db, table_name, row_data, append = TRUE)
      print(paste("Data with", unique_column, unique_value, "inserted into", table_name))
    }
  }
}

insert_data(db, "Supplier", suppliers_data, "SUPPLIER_ID")
insert_data(db, "Product", products_data, "PRODUCT_ID")
insert_data(db, "Inventory", inventories_data, "INVENTORY_ID")
insert_data(db, "Customer", customers_data, "CUSTOMER_ID")
insert_data(db, "Shipment", shipments_data, "SHIPMENT_ID")
insert_data(db, "Payment", payments_data, "PAYMENT_ID")

# List all tables
tables <- dbListTables(db)
print(tables)

table_name <- "Payment"

# Execute SQL query to count number of rows in the table
query <- paste("SELECT COUNT(*) FROM", table_name)
result <- dbGetQuery(db, query)

# Print the number of rows
print(result)

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

#Check for duplicate customer phone numbers
duplicate_phone_query <- "
SELECT CUSTOMER_PHONE, COUNT(*)
FROM Customer
GROUP BY CUSTOMER_PHONE
HAVING COUNT(*) > 1;"
duplicate_phone <- dbGetQuery(db, duplicate_phone_query)
# Print the results
print("Duplicate Phone number:")
print(duplicate_phone)

duplicate_phone_query <- "
SELECT DISTINCT CUSTOMER_PHONE
FROM Customer
GROUP BY CUSTOMER_PHONE
HAVING COUNT(*) > 1;"
duplicate_phone <- dbGetQuery(db, duplicate_phone_query)

# Print the results
print("Duplicate Phone numbers:")
print(duplicate_phone)

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


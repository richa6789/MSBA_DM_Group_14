-- !preview conn=DBI::dbConnect(RSQLite::SQLite())


CREATE TABLE Product (
  PRODUCT_ID VARCHAR(255) PRIMARY KEY,
  PRODUCT_NAME VARCHAR(255),pwd
  PRODUCT_CATEGORY VARCHAR(255),
  PRICE DECIMAL(10, 2),
  SUPPLIER_ID VARCHAR(255),
  FOREIGN KEY(SUPPLIER_ID) REFERENCES Supplier(SUPPLIER_ID)
);

CREATE TABLE Inventory (
  INVENTORY_ID VARCHAR(255) PRIMARY KEY,
  STOCK INTEGER,
  SHELF_NO VARCHAR(255),
  PRODUCT_ID VARCHAR(255),
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
);

CREATE TABLE Customer (
  CUSTOMER_ID VARCHAR(255) PRIMARY KEY,
  CUSTOMER_FIRSTNAME VARCHAR(255),
  CUSTOMER_LASTNAME VARCHAR(255),
  CUSTOMER_EMAIL VARCHAR(255),
  CUSTOMER_PHONE VARCHAR(255),
  CUSTOMER_BIRTHDAY DATE,
  CUSTOMER_GENDER VARCHAR(50),
  SHIPMENT_ID VARCHAR(255),
  PAYMENT_ID VARCHAR(255)
);

CREATE TABLE Shipping (
  SHIPMENT_ID VARCHAR(255) PRIMARY KEY,
  SHIPMENT_DATE DATE,
  SHIPMENT_ADDRESS VARCHAR(255),
  SHIPMENT_CITY VARCHAR(255),
  SHIPMENT_ZIPCODE VARCHAR(255),
  BILLING_COUNTRY VARCHAR(255),
  CUSTOMER_ID VARCHAR(255),
  PRODUCT_ID VARCHAR(255),
  FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
);

CREATE TABLE Payment (
  PAYMENT_ID VARCHAR(255) PRIMARY KEY,
  PAYMENT_METHOD VARCHAR(255),
  ORDER_AMOUNT DECIMAL(10, 2),
  PAYMENT_DATE DATE,
  BILLING_ADDRESS VARCHAR(255),
  BILLING_CITY VARCHAR(255),
  BILLING_ZIPCODE VARCHAR(255),
  BILLING_COUNTRY VARCHAR(255),
  CUSTOMER_ID VARCHAR(255),
  PRODUCT_ID VARCHAR(255),
  FOREIGN KEY(CUSTOMER_ID) REFERENCES Customer(CUSTOMER_ID),
  FOREIGN KEY(PRODUCT_ID) REFERENCES Product(PRODUCT_ID)
);


SELECT 
CREATE TABLE Supplier (
  SUPPLIER_ID VARCHAR(255) PRIMARY KEY,
  SUPPLIER_NAME VARCHAR(255),
  SUPPLIER_PHONE VARCHAR(255),
  SUPPLIER_EMAIL VARCHAR(255)
);
CREATE DATABASE sales;

CREATE SCHEMA raw;
CREATE TABLE raw.sales(
    "SaleID"            int primary key,
    "ProductID"         int,
    "ProductName"       text,
    "Brand"             text,
    "Category"          text,
    "RetailerID"        int,
    "RetailerName"      text,
    "Channel"           text,
    "Location"          text,
    "Quantity"          int,
    "Price"             decimal(18,2),
    "Date"              date
);
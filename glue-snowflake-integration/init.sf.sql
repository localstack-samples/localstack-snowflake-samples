-- create the database the Glue job reads from, then a table with sample data

CREATE DATABASE IF NOT EXISTS test;
USE DATABASE test;
USE SCHEMA public;

-- create a table
CREATE TABLE src_glue (id int, name varchar, status boolean);

-- insert data to table
INSERT INTO src_glue (id, name, status) VALUES
(1, 'Alice', TRUE),
(2, 'Bob', FALSE),
(3, 'Charlie', TRUE),
(4, 'David', FALSE),
(5, 'Eve', TRUE);

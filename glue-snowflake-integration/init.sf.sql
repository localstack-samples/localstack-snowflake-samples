-- script to create a table and populate it with the data

-- create a table
CREATE TABLE src_glue (id int, name varchar, status bool);

-- insert data to table
INSERT INTO src_glue (id, name, status) VALUES
(1, 'Alice', TRUE),
(2, 'Bob', FALSE),
(3, 'Charlie', TRUE),
(4, 'David', FALSE),
(5, 'Eve', TRUE);

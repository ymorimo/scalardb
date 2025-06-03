--CREATE DATABASE scalardb;
-- \c scalardb;

CREATE SCHEMA coordinator;
CREATE TABLE coordinator.state (
  tx_id varchar(128) NOT NULL,
  tx_state int DEFAULT NULL,
  tx_created_at bigint DEFAULT NULL,
  PRIMARY KEY (tx_id)
);


CREATE SCHEMA scalardb;
-- CREATE SCHEMA coordinator;
-- Metadata table
-- DROP TABLE IF EXISTS scalardb.metadata;
CREATE TABLE scalardb.metadata (
  full_table_name VARCHAR(128) NOT NULL,
  column_name VARCHAR(128) NOT NULL,
  data_type VARCHAR(20) NOT NULL,
  key_type VARCHAR(20),
  clustering_order VARCHAR(10),
  indexed BOOLEAN NOT NULL,
  ordinal_position INT NOT NULL,
  PRIMARY KEY (full_table_name, column_name)
);

INSERT INTO scalardb.metadata VALUES
  ('test.employee','email','TEXT',NULL,NULL,FALSE,3),
  ('test.employee','id','INT','PARTITION',NULL,FALSE,1),
  ('test.employee','name','TEXT',NULL,NULL,FALSE,2),
  ('test.employee_trn','before_email','TEXT',NULL,NULL,FALSE,15),
  ('test.employee_trn','before_name','TEXT',NULL,NULL,FALSE,14),
  ('test.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,FALSE,13),
  ('test.employee_trn','before_tx_id','TEXT',NULL,NULL,FALSE,9),
  ('test.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,FALSE,12),
  ('test.employee_trn','before_tx_state','INT',NULL,NULL,FALSE,10),
  ('test.employee_trn','before_tx_version','INT',NULL,NULL,FALSE,11),
  ('test.employee_trn','email','TEXT',NULL,NULL,FALSE,3),
  ('test.employee_trn','id','INT','PARTITION',NULL,FALSE,1),
  ('test.employee_trn','name','TEXT',NULL,NULL,FALSE,2),
  ('test.employee_trn','tx_committed_at','BIGINT',NULL,NULL,FALSE,8),
  ('test.employee_trn','tx_id','TEXT',NULL,NULL,FALSE,4),
  ('test.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,FALSE,7),
  ('test.employee_trn','tx_state','INT',NULL,NULL,FALSE,5),
  ('test.employee_trn','tx_version','INT',NULL,NULL,FALSE,6),
  ('test.all_columns','before_col10','TIMESTAMP',NULL,NULL,FALSE,26),
  ('test.all_columns','before_col11','TIMESTAMPTZ',NULL,NULL,FALSE,24),
  ('test.all_columns','before_col4','FLOAT',NULL,NULL,FALSE,28),
  ('test.all_columns','before_col5','DOUBLE',NULL,NULL,FALSE,29),
  ('test.all_columns','before_col6','TEXT',NULL,NULL,FALSE,25),
  ('test.all_columns','before_col7','BLOB',NULL,NULL,FALSE,27),
  ('test.all_columns','before_col8','DATE',NULL,NULL,FALSE,22),
  ('test.all_columns','before_col9','TIME',NULL,NULL,FALSE,23),
  ('test.all_columns','before_tx_committed_at','BIGINT',NULL,NULL,FALSE,21),
  ('test.all_columns','before_tx_id','TEXT',NULL,NULL,FALSE,17),
  ('test.all_columns','before_tx_prepared_at','BIGINT',NULL,NULL,FALSE,20),
  ('test.all_columns','before_tx_state','INT',NULL,NULL,FALSE,18),
  ('test.all_columns','before_tx_version','INT',NULL,NULL,FALSE,19),
  ('test.all_columns','col1','BIGINT','PARTITION',NULL,FALSE,1),
  ('test.all_columns','col10','TIMESTAMP',NULL,NULL,FALSE,10),
  ('test.all_columns','col11','TIMESTAMPTZ',NULL,NULL,FALSE,11),
  ('test.all_columns','col2','INT','CLUSTERING','ASC',FALSE,2),
  ('test.all_columns','col3','BOOLEAN','CLUSTERING','ASC',FALSE,3),
  ('test.all_columns','col4','FLOAT',NULL,NULL,FALSE,4),
  ('test.all_columns','col5','DOUBLE',NULL,NULL,FALSE,5),
  ('test.all_columns','col6','TEXT',NULL,NULL,FALSE,6),
  ('test.all_columns','col7','BLOB',NULL,NULL,FALSE,7),
  ('test.all_columns','col8','DATE',NULL,NULL,FALSE,8),
  ('test.all_columns','col9','TIME',NULL,NULL,FALSE,9),
  ('test.all_columns','tx_committed_at','BIGINT',NULL,NULL,FALSE,16),
  ('test.all_columns','tx_id','TEXT',NULL,NULL,FALSE,12),
  ('test.all_columns','tx_prepared_at','BIGINT',NULL,NULL,FALSE,15),
  ('test.all_columns','tx_state','INT',NULL,NULL,FALSE,13),
  ('test.all_columns','tx_version','INT',NULL,NULL,FALSE,14),
  ('coordinator.state','tx_id','TEXT','PARTITION',NULL,FALSE,1),
  ('coordinator.state','tx_state','INT',NULL,NULL,FALSE,2),
  ('coordinator.state','tx_created_at','BIGINT',NULL,NULL,FALSE,3),
  ('test.emp_department','id','INT','PARTITION',NULL,FALSE,1),
  ('test.emp_department','emp_id','INT','CLUSTERING','ASC',FALSE,2),
  ('test.emp_department','department','TEXT',NULL,NULL,FALSE,3);

CREATE SCHEMA test;


-- Table: employee
CREATE TABLE test.employee (
  id INT PRIMARY KEY,
  name TEXT,
  email TEXT
);

CREATE TABLE test.emp_department (
  id INT NOT NULL,
  emp_id INT NOT NULL,
  department TEXT,
  PRIMARY KEY (id,emp_id)
);


-- Table: employee_trn
CREATE TABLE test.employee_trn (
  id INT PRIMARY KEY,
  name TEXT,
  email TEXT,
  tx_id TEXT,
  tx_state INT,
  tx_version INT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  before_tx_id TEXT,
  before_tx_state INT,
  before_tx_version INT,
  before_tx_prepared_at BIGINT,
  before_tx_committed_at BIGINT,
  before_name TEXT,
  before_email TEXT
);

-- Table: all_columns
CREATE TABLE test.all_columns (
  col1 BIGINT NOT NULL,
  col2 INT NOT NULL,
  col3 BOOLEAN NOT NULL,
  col4 DOUBLE PRECISION,
  col5 DOUBLE PRECISION,
  col6 TEXT,
  col7 BYTEA,
  col8 DATE,
  col9 TIME(6),
  col10 TIMESTAMP(3),
  col11 TIMESTAMPTZ(3),
  tx_id TEXT,
  tx_state INT,
  tx_version INT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  before_tx_id TEXT,
  before_tx_state INT,
  before_tx_version INT,
  before_tx_prepared_at BIGINT,
  before_tx_committed_at BIGINT,
  before_col8 DATE,
  before_col9 TIME(6),
  before_col11 TIMESTAMPTZ(3),
  before_col6 TEXT,
  before_col10 TIMESTAMP(3),
  before_col7 BYTEA,
  before_col4 DOUBLE PRECISION,
  before_col5 DOUBLE PRECISION,
  PRIMARY KEY (col1, col2, col3)
);
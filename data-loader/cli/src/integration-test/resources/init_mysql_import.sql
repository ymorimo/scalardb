CREATE DATABASE IF NOT EXISTS scalardb;
USE scalardb;
DROP TABLE IF EXISTS `metadata`;
CREATE TABLE `metadata` (
  `full_table_name` varchar(128) NOT NULL,
  `column_name` varchar(128) NOT NULL,
  `data_type` varchar(20) NOT NULL,
  `key_type` varchar(20) DEFAULT NULL,
  `clustering_order` varchar(10) DEFAULT NULL,
  `indexed` tinyint(1) NOT NULL,
  `ordinal_position` int NOT NULL,
  PRIMARY KEY (`full_table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
LOCK TABLES `metadata` WRITE;
INSERT INTO `metadata` VALUES ('test.employee','email','TEXT',NULL,NULL,0,3),('test.employee','id','INT','PARTITION',NULL,0,1),('test.employee','name','TEXT',NULL,NULL,0,2),('testt.employee_trn','before_email','TEXT',NULL,NULL,0,15),('testt.employee_trn','before_name','TEXT',NULL,NULL,0,14),('testt.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,0,13),('testt.employee_trn','before_tx_id','TEXT',NULL,NULL,0,9),('testt.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,0,12),('testt.employee_trn','before_tx_state','INT',NULL,NULL,0,10),('testt.employee_trn','before_tx_version','INT',NULL,NULL,0,11),('testt.employee_trn','email','TEXT',NULL,NULL,0,3),('testt.employee_trn','id','INT','PARTITION',NULL,0,1),('testt.employee_trn','name','TEXT',NULL,NULL,0,2),('testt.employee_trn','tx_committed_at','BIGINT',NULL,NULL,0,8),('testt.employee_trn','tx_id','TEXT',NULL,NULL,0,4),('testt.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,0,7),('testt.employee_trn','tx_state','INT',NULL,NULL,0,5),('testt.employee_trn','tx_version','INT',NULL,NULL,0,6);
UNLOCK TABLES;

CREATE DATABASE IF NOT EXISTS test;
USE test;

CREATE TABLE `employee` (
  `id` int NOT NULL,
  `name` longtext,
  `email` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `employee_trn` (
`id` int NOT NULL,
`name` longtext,
`email` longtext,
`tx_id` longtext,
`tx_state` int DEFAULT NULL,
`tx_version` int DEFAULT NULL,
`tx_prepared_at` bigint DEFAULT NULL,
`tx_committed_at` bigint DEFAULT NULL,
`before_tx_id` longtext,
`before_tx_state` int DEFAULT NULL,
`before_tx_version` int DEFAULT NULL,
`before_tx_prepared_at` bigint DEFAULT NULL,
`before_tx_committed_at` bigint DEFAULT NULL,
`before_name` longtext,
`before_email` longtext,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

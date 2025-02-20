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
INSERT INTO `metadata` VALUES ('test.employee','email','TEXT',NULL,NULL,0,3),('test.employee','id','INT','PARTITION',NULL,0,1),('test.employee','name','TEXT',NULL,NULL,0,2);
UNLOCK TABLES;

CREATE DATABASE IF NOT EXISTS TEST;
USE TEST;

CREATE TABLE `employee` (
  `id` int NOT NULL,
  `name` longtext,
  `email` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
LOCK TABLES `employee` WRITE;

INSERT INTO `employee` VALUES (0,'emp0','emp0@example.com'),(1,'emp1','emp1@example.com'),(2,'emp2','emp2@example.com'),(3,'emp3','emp3@example.com'),(4,'emp4','emp4@example.com'),(5,'emp5','emp5@example.com'),(6,'emp6','emp6@example.com'),(7,'emp7','emp7@example.com'),(8,'emp8','emp8@example.com'),(9,'emp9','emp9@example.com'),(10,'emp10','emp10@example.com'),(11,'emp11','emp11@example.com'),(12,'emp12','emp12@example.com'),(13,'emp13','emp13@example.com'),(14,'emp14','emp14@example.com'),(15,'emp15','emp15@example.com'),(16,'emp16','emp16@example.com'),(17,'emp17','emp17@example.com'),(18,'emp18','emp18@example.com'),(19,'emp19','emp19@example.com'),(20,'emp20','emp20@example.com'),(21,'emp21','emp21@example.com'),(22,'emp22','emp22@example.com'),(23,'emp23','emp23@example.com'),(24,'emp24','emp24@example.com');
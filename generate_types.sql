# Some basic parameter tuning
SET max_heap_table_size = 4294967295 ;
SET tmp_table_size = 4294967295 ;
SET bulk_insert_buffer_size = 256217728 ;

# $$PREFIX$$ for different datasets
# replace all '$$PREFIX$$' with the language specific prefix like 'en'

# Tables to import data
DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_types_original` ;
CREATE TABLE `$$PREFIX$$_dbpedia_types_original` (
  `resource` varchar(1000) NOT NULL ,
  `type` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_properties_original` ;
CREATE TABLE `$$PREFIX$$_dbpedia_properties_original` (
  `subject` varchar(1000) NOT NULL ,
  `predicate` varchar(1000) NOT NULL ,
  `object` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
  
# Import data
# Note: requires preprocessed data files using NT2CSV.java
LOAD DATA INFILE 'PATH_TO_YOUR_DATA/instance_types_$$PREFIX$$.csv' IGNORE INTO TABLE $$PREFIX$$_dbpedia_types_original FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\'' LINES TERMINATED BY '\n' ;
LOAD DATA INFILE 'PATH_TO_YOUR_DATA/mappingbased_properties_$$PREFIX$$.csv' IGNORE INTO TABLE $$PREFIX$$_dbpedia_properties_original FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\'' LINES TERMINATED BY '\n' ;

# Some transformations to allow better indexing - everything is converted to md5 with lookup tables
DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_types_md5` ;
CREATE TABLE `$$PREFIX$$_dbpedia_types_md5` (
  `resource` char(32) NOT NULL ,
  `type` char(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_properties_md5`;
CREATE TABLE `$$PREFIX$$_dbpedia_properties_md5` (
  `subject` char(32) NOT NULL ,
  `predicate` char(32) NOT NULL ,
  `object` char(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
INSERT INTO $$PREFIX$$_dbpedia_types_md5
  SELECT md5(resource), md5(type)
  FROM $$PREFIX$$_dbpedia_types_original ;

ALTER TABLE `$$PREFIX$$_dbpedia_types_md5` 
  ADD INDEX `idx_dbpedia_types_resource` (`resource` ASC) ,
  ADD INDEX `idx_dbpedia_types_type` (`type` ASC) ;

INSERT INTO $$PREFIX$$_dbpedia_properties_md5
  SELECT md5(subject), md5(predicate), md5(object)
  FROM $$PREFIX$$_dbpedia_properties_original ;

ALTER TABLE `$$PREFIX$$_dbpedia_properties_md5` 
  ADD INDEX `idx_dbpedia_properties_subject` (`subject` ASC) ,
  ADD INDEX `idx_dbpedia_properties_predicate` (`predicate` ASC) ,
  ADD INDEX `idx_dbpedia_properties_object` (`object` ASC) ;

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_type_to_md5` ;
CREATE TABLE `$$PREFIX$$_dbpedia_type_to_md5` (
  `type` varchar(1000) NOT NULL ,
  `type_md5` char(32) NOT NULL ,
  PRIMARY KEY (`type_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT IGNORE INTO $$PREFIX$$_dbpedia_type_to_md5
  SELECT type, md5(type)
  FROM $$PREFIX$$_dbpedia_types_original ;

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_resource_to_md5` ;
CREATE TABLE `$$PREFIX$$_dbpedia_resource_to_md5` (
  # default: `resource` varchar(1000) NOT NULL ,
  `resource` varchar(255) NOT NULL ,
  `resource_md5` char(32) NOT NULL ,
  PRIMARY KEY (`resource_md5`) ,
  key `idx_resource_to_md5` (`resource`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT IGNORE INTO $$PREFIX$$_dbpedia_resource_to_md5
  SELECT subject, md5(subject)
  FROM $$PREFIX$$_dbpedia_properties_original ;
INSERT IGNORE INTO $$PREFIX$$_dbpedia_resource_to_md5
  SELECT object, md5(object)
  FROM $$PREFIX$$_dbpedia_properties_original ;

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_predicate_to_md5` ;
CREATE TABLE `$$PREFIX$$_dbpedia_predicate_to_md5` (
  `predicate` varchar(1000) NOT NULL ,
  `predicate_md5` char(32) NOT NULL ,
  PRIMARY KEY `idx_predicate_to_md5_type_md5` (`predicate_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT IGNORE INTO $$PREFIX$$_dbpedia_predicate_to_md5
  SELECT predicate, md5(predicate)
  FROM $$PREFIX$$_dbpedia_properties_original ;

# Compile the statistics
DROP TABLE IF EXISTS `$$PREFIX$$_stat_type_count` ;
CREATE TABLE `$$PREFIX$$_stat_type_count` (
  `type` char(32) NOT NULL ,
  `type_count` int(11) NOT NULL ,
  KEY `idx_type_count_type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_type_count
  SELECT type, COUNT(resource)
  FROM $$PREFIX$$_dbpedia_types_md5
  GROUP BY (type) ;

DROP TABLE IF EXISTS `$$PREFIX$$_stat_type_apriori_probability` ;
CREATE TABLE `$$PREFIX$$_stat_type_apriori_probability` (
  `type` char(32) NOT NULL ,
  `probability` float NOT NULL ,
  KEY `idx_type_apriori_probability_type` (`type`) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_type_apriori_probability
  SELECT type, type_count/(SELECT COUNT(resource_md5) FROM $$PREFIX$$_dbpedia_resource_to_md5) AS rel_count
  FROM $$PREFIX$$_stat_type_count ;

DROP TABLE IF EXISTS `$$PREFIX$$_stat_resource_predicate_tf`;
CREATE TABLE `$$PREFIX$$_stat_resource_predicate_tf` (
  `resource` char(32) NOT NULL ,
  `predicate` char(32) NOT NULL ,
  `tf` int(11) NOT NULL ,
  `outin` int(11) NOT NULL ,
  KEY `idx_resource_predicate_tf_resource` (`resource`) ,
  KEY `idx_resource_predicate_tf_predicate` (`predicate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_resource_predicate_tf
  SELECT subject, predicate, COUNT(object), 0
  FROM $$PREFIX$$_dbpedia_properties_md5
  GROUP BY subject, predicate ;
INSERT INTO $$PREFIX$$_stat_resource_predicate_tf
  SELECT object, predicate, COUNT(subject), 1
  FROM $$PREFIX$$_dbpedia_properties_md5
  GROUP BY object, predicate ;

DROP TABLE IF EXISTS `$$PREFIX$$_stat_type_predicate_percentage` ;
CREATE TABLE `$$PREFIX$$_stat_type_predicate_percentage` (
  `type` char(32) NOT NULL ,
  `predicate` char(32) NOT NULL ,
  `outin` int(11) NOT NULL ,
  `percentage` float NOT NULL ,
  KEY `idx_type_predicate_percentage_type` (`type`) ,
  KEY `idx_type_predicate_percentage_predicate` (`predicate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_type_predicate_percentage
  SELECT types.type, res.predicate, 0, COUNT(subject)/(SELECT COUNT(subject) FROM $$PREFIX$$_dbpedia_properties_md5 AS resinner WHERE res.predicate = resinner.predicate)
  FROM $$PREFIX$$_dbpedia_properties_md5 AS res, $$PREFIX$$_dbpedia_types_md5 AS types
  WHERE res.subject = types.resource
  GROUP BY res.predicate, types.type ;

INSERT INTO $$PREFIX$$_stat_type_predicate_percentage SELECT types.type, res.predicate, 1, COUNT(object)/(SELECT COUNT(object) FROM $$PREFIX$$_dbpedia_properties_md5 AS resinner WHERE res.predicate = resinner.predicate)
  FROM $$PREFIX$$_dbpedia_properties_md5 AS res, $$PREFIX$$_dbpedia_types_md5 AS types
  WHERE res.object = types.resource
  GROUP BY res.predicate, types.type ;

DROP TABLE IF EXISTS `$$PREFIX$$_stat_predicate_weight_apriori` ;
CREATE TABLE `$$PREFIX$$_stat_predicate_weight_apriori` (
  `predicate` char(32) NOT NULL ,
  `outin` int(11) NOT NULL ,
  `weight` float NOT NULL ,
  KEY `idx_predicate_weight_apriori_predicate` (`predicate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_predicate_weight_apriori
  SELECT predicate, outin, SUM((percentage - probability)*(percentage - probability))
  FROM $$PREFIX$$_stat_type_predicate_percentage 
  LEFT JOIN $$PREFIX$$_stat_type_apriori_probability ON $$PREFIX$$_stat_type_predicate_percentage.type = $$PREFIX$$_stat_type_apriori_probability.type
  GROUP BY predicate, outin ;

# Materialize the Types
# uses one intermediate table

DROP TABLE IF EXISTS `$$PREFIX$$_dbpedia_untyped_instance` ;
CREATE  TABLE `$$PREFIX$$_dbpedia_untyped_instance` (
  `resource` VARCHAR(1000) NOT NULL ,
  `resource_md5` CHAR(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_dbpedia_untyped_instance
  SELECT res.resource, res.resource_md5
  FROM $$PREFIX$$_dbpedia_resource_to_md5 AS res
  LEFT JOIN $$PREFIX$$_dbpedia_types_md5 AS typ ON res.resource_md5=typ.resource
  WHERE ISNULL(type) ;

DROP TABLE IF EXISTS `$$PREFIX$$_stat_resource_predicate_type` ;
CREATE TABLE `$$PREFIX$$_stat_resource_predicate_type` (
  `resource` char(32) NOT NULL ,
  `predicate` char(32) NOT NULL ,
  `type` char(32) NOT NULL ,
  `tf` float NOT NULL ,
  `percentage` float NOT NULL ,
  `weight` float NOT NULL ,
  KEY `idx_$$PREFIX$$_stat_resource_predicate_type` (`resource`, `type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_stat_resource_predicate_type
  SELECT instance.resource_md5, tf.predicate,perc.type, tf, percentage, weight
  FROM $$PREFIX$$_dbpedia_untyped_instance AS instance
  LEFT JOIN $$PREFIX$$_stat_resource_predicate_tf AS tf ON instance.resource_md5 = tf.resource
  LEFT JOIN $$PREFIX$$_stat_type_predicate_percentage AS perc ON tf.predicate = perc.predicate AND tf.outin = perc.outin 
  LEFT JOIN $$PREFIX$$_stat_predicate_weight_apriori AS weight ON tf.predicate = weight.predicate AND tf.outin = weight.outin
  LEFT JOIN $$PREFIX$$_stat_type_apriori_probability AS tap ON perc.type = tap.type
  LEFT JOIN $$PREFIX$$_dbpedia_type_to_md5 AS t2md5 ON tap.type = t2md5.type_md5
  WHERE NOT perc.type IS NULL ;

DROP TABLE IF EXISTS `$$PREFIX$$_resulting_types` ;
CREATE  TABLE `$$PREFIX$$_resulting_types` (
  `resource` VARCHAR(1000) NOT NULL ,
  `type` VARCHAR(1000) NOT NULL ,
  `score` FLOAT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

INSERT INTO $$PREFIX$$_resulting_types 
  SELECT resource, type, SUM(tf*percentage*weight)/SUM(tf*weight) AS score
  FROM $$PREFIX$$_stat_resource_predicate_type 
  GROUP BY resource,type 
  HAVING score>=0.05 ;

# Read types at the threshold you like, e.g.
# SELECT r2md5.resource,t2md5.type FROM resulting_types AS res
# LEFT JOIN dbpedia_resource_to_md5 AS r2md5 ON res.resource=r2md5.resource_md5
# LEFT JOIN dbpedia_type_to_md5 AS t2md5 ON res.type=t2md5.type_md5
# WHERE score>=0.4
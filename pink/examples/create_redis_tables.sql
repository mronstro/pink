DROP DATABASE IF EXISTS redis_0;
CREATE DATABASE redis_0;
USE redis_0;
CREATE TABLE redis_main_key(
  key_val VARBINARY(3000) NOT NULL,
  key_id BIGINT UNSIGNED,
  expiry_date INT UNSIGNED,
  value VARBINARY(26500) NOT NULL,
  tot_value_len INT UNSIGNED NOT NULL,
  num_rows INT UNSIGNED NOT NULL,
  row_state INT UNSIGNED NOT NULL,
  tot_key_len INT UNSIGNED NOT NULL,
  PRIMARY KEY (key_val) USING HASH,
  UNIQUE KEY (key_id) USING HASH,
  KEY expiry_index(expiry_date))
  ENGINE NDB
  CHARSET=latin1
  COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8";

CREATE TABLE redis_key_value(
  key_id BIGINT UNSIGNED NOT NULL,
  ordinal INT UNSIGNED NOT NULL,
  value VARBINARY(29500) NOT NULL,
  PRIMARY KEY (key_id, ordinal),
  FOREIGN KEY (key_id)
   REFERENCES redis_main_key(key_id)
   ON UPDATE RESTRICT ON DELETE CASCADE)
  ENGINE NDB,
  COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8"
  PARTITION BY KEY (key_id);

CREATE TABLE redis_main_field(
  key_id BIGINT UNSIGNED NOT NULL,
  field_name VARBINARY(3000) NOT NULL,
  field_id BIGINT UNSIGNED,
  value VARBINARY(26500) NOT NULL,
  num_value_rows INT UNSIGNED NOT NULL,
  tot_value_len INT UNSIGNED NOT NULL,
  tot_key_len INT UNSIGNED NOT NULL,
  PRIMARY KEY (key_id, field_name) USING HASH,
  UNIQUE KEY (field_id) USING HASH)
  ENGINE NDB
  COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8";

CREATE TABLE redis_field_value(
  field_id BIGINT UNSIGNED NOT NULL,
  ordinal INT UNSIGNED NOT NULL,
  value VARBINARY(29500) NOT NULL,
  PRIMARY KEY (field_id, ordinal),
  FOREIGN KEY (field_id)
   REFERENCES redis_main_field(field_id)
   ON UPDATE RESTRICT ON DELETE CASCADE)
  ENGINE NDB,
  COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8"
  PARTITION BY KEY (field_id);


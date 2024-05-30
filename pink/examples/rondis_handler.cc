/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#include <string.h>
#include <stdio.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

int
rondb_get_command(pink::RedisCmdArgsType&,
                  std::string* response,
                  int fd);
int
rondb_set_command(pink::RedisCmdArgsType&,
                  std::string* response,
                  int fd);
#define MAX_CONNECTIONS 1
#define MAX_NDB_PER_CONNECTION 1
Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];
Ndb *rondb_ndb[MAX_CONNECTIONS][MAX_NDB_PER_CONNECTION];

int
rondb_connect(const char *connect_string,
              unsigned int num_connections)
{
  ndb_init();
  for (unsigned int i = 0; i < MAX_CONNECTIONS; i++)
  {
    rondb_conn[i] = new Ndb_cluster_connection(connect_string);
    if (rondb_conn[i]->connect() != 0)
    {
      printf("Kilroy C\n");
      return -1;
    }
    printf("Connected to cluster\n");
    if (rondb_conn[i]->wait_until_ready(30,0) != 0)
    {
      printf("Kilroy CI\n");
      return -1;
    }
    printf("Connected to started cluster\n");
    for (unsigned int j = 0; j < MAX_NDB_PER_CONNECTION; j++)
    {
      Ndb *ndb = new Ndb(rondb_conn[i], "redis_0");
      if (ndb == nullptr)
      {
        printf("Kilroy CII\n");
        return -1;
      }
      if (ndb->init() != 0)
      {
        printf("Kilroy CIII\n");
        return -1;
      }
      rondb_ndb[i][j] = ndb;
    }
  }
  return 0;
}

void rondb_end()
{
  ndb_end(0);
}

int
rondb_redis_handler(pink::RedisCmdArgsType& argv,
                    std::string* response,
                    int fd)
{
  if (argv.size() == 0)
  {
    return -1;
  }
  const char* cmd_str = argv[0].c_str();
  unsigned int cmd_len = strlen(cmd_str);
  if (cmd_len == 3)
  {
    const char *set_str = "set";
    const char *get_str = "get";
    if (memcmp(cmd_str, get_str, 3) == 0)
    {
      return rondb_get_command(argv, response, fd);
    }
    else if (memcmp(cmd_str, set_str, 3) == 0)
    {
      return rondb_set_command(argv, response, fd);
    }
    return -1;
  }
  else if (cmd_len == 1)
  {
    const char *shutdown_str = "shutdown";
    if (memcmp(cmd_str, shutdown_str, 8) == 0)
    {
      printf("Shutdown Rondis server\n");
      return -1;
    }
  }
  return -1;
}

/**
 * Mapping the Redis commands to RonDB requests is done
 * in the following manner.
 *
 * Each database in Redis is a separate table belonging
 * to a database. In Redis the commands are sent to the
 * current database selected by the SELECT command.
 * So e.g. SELECT 0 will select database 0, thus the
 * table name of Redis tables in RonDB will always be
 * Redis, but the database will be "0" or the number of
 * the database used in Redis.
 *
 * All Redis tables will have the same format.
 CREATE TABLE redis_main_key(
   redis_key VARBINARY(3000) NOT NULL,
   key_id BIGINT UNSIGNED,
   expiry_date INT UNSIGNED NOT NULL,
   redis_value VARBINARY(26500) NOT NULL,
   tot_value_len INT UNSIGNED NOT NULL,
   value_rows INT UNSIGNED NOT NULL,
   field_rows INT UNSIGNED NOT NULL,
   row_state INT UNSIGNED NOT NULL,
   tot_key_len INT UNSIGNED NOT NULL,
   PRIMARY KEY (redis_key) USING HASH,
   UNIQUE KEY (key_id),
   KEY expiry_index(expiry_date))
   ENGINE NDB
   CHARSET=latin1
   COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8"
 *
 * The redis_main table is the starting point for key-value
 * objects, for hashes and other data structures in Redis.
 * The current object is always using version_id = 0.
 * When a row has expired, but it is still required to be
 * around, then the version_id is set to the key_id.
 *
 * We have an ordered index on expiry_date, by scanning
 * this from the oldest value we quickly find keys that
 * require deletion. The expiry_date column is also used
 * to figure out whether a key value actually exists or
 * not.
 *
 * If value_rows is 0, there are no value rows attached.
 * Otherwise it specifies the number of value rows.
 *
 * The this_value_len specifies the number of bytes of
 * values stored in this row. The tot_value_len specifies
 * the total number of value bytes of the key.
 *
 * For hash keys the fields are stored in their own rows.
 * The field_rows specifies the number of fields this key
 * has. It must be a number bigger than 0. If there are
 * fields in the key there is no value in the key, so
 * obviously also requires value_rows to be 0.
 *
 * The row_state contains information about data type of
 * the key, of the value, whether we have all fields
 * inlined in the values object and whether the row is
 * expired and deletion process is ongoing.
 *
 * Bit 0-1 in row_state is the data type of the key.
 * 0 means a string
 * 1 means a number
 * 2 means a binary string
 *
 * Bit 2-3 in row_state is the data type of the value.
 *
 * The key_id is a unique reference of the row that is
 * never reused (it is a 64 bit value and should last
 * for 100's of years). The key_id removes the need to
 * store the full key value in multiple tables.
 *
 * The value extensions are stored in a separate table
 * for keys which have the following format:
 *
 * CREATE TABLE redis_key_value(
 *   key_id BIGINT NOT NULL
 *   ordinal UNSIGNED INT NOT NULL,
 *   value VARBINARY(29500) NOT NULL,
 *   PRIMARY KEY (key_id, ordinal),
 *   FOREIGN KEY (key_id)
 *    REFERENCES redis_main_key(key_id)
 *    ON UPDATE RESTRICT ON DELETE CASCADE)
 *   ENGINE NDB
 *   PARTITION BY KEY (key_id)
 *   COMMENT="PARTITION_BALANCE=RP_BY_LDM_X_8"
 *
 * For the data type Hash we will use another separate
 * table to store the field values. Each field value
 * will have some value data stored inline, but could
 * also have parts of the value stored in the redis_ext_value
 * table. The field_id is a unique identifier that is
 * referencing the redis_ext_value table.
 *
 * CREATE TABLE redis_main_field(
 *   key_id BIGINT NOT NULL,
 *   field_name VARBINARY(3000) NOT NULL,
 *   field_id BIGINT UNSIGNED,
 *   value VARBINARY(26500) NOT NULL,
 *   value_rows UNSIGNED INT NOT NULL,
 *   tot_value_len UNSIGNED INT NOT NULL,
 *   tot_key_len UNSIGNED INT NOT NULL,
 *   PRIMARY KEY (key_id, field_name),
 *   UNIQUE KEY (field_id))
 *   ENGINE NDB
 *   COMMENT="PARTITION_BALANCE=RP_BY_LDM_X_8"
 *
 * The value extensions are stored in a separate table
 * for keys which have the following format:
 * CREATE TABLE redis_field_value(
 *   field_id BIGINT NOT NULL
 *   ordinal UNSIGNED INT NOT NULL,
 *   value VARBINARY(29500) NOT NULL,
 *   PRIMARY KEY (field_id, ordinal),
 *   FOREIGN KEY (field_id)
 *    REFERENCES redis_main_field(field_id)
 *    ON UPDATE RESTRICT ON DELETE CASCADE)
 *   ENGINE NDB
 *   PARTITION BY KEY (field_id)
 *   COMMENT="PARTITION_BALANCE=RP_BY_LDM_X_8"
 *
 * For most rows the key will fit in the key field and
 * the value will fit in the value field. In this case
 * row in Redis maps directly to a row in RonDB. However
 * for large rows with large values, several RonDB rows
 * might be required. The extended rows will be stored
 * ordinal set to 0, 1, 2, 3 and so forth. The first row
 * will have ordinal set to 0. Since we have an ordered
 * index on key, we can find all rows using a partition
 * pruned index scan on the key field. We also use key
 * as the partition key to ensure these scans are
 * partition pruned index scans. It is also possible to
 * use batched primary key lookups to get the value rows.
 *
 * By using a Read lock on the primary key lookup of
 * the first row, we can ensure that the read of the
 * entire row is stable since the row cannot be changed
 * without having an exclusive lock on the first row.
 *
 * As an optimisation GET operations will first perform
 * a READ COMMITTED, this will suffice if there are no
 * value rows and there are no fields. For hash keys this
 * optimisation isn't used.
 */
#define INLINE_VALUE_LEN 26500
#define EXTENSION_VALUE_LEN 29500
#define MAX_KEY_VALUE_LEN 3000

int
execute_no_commit(NdbTransaction *trans)
{
  printf("Execute NoCommit\n");
  if (trans->execute(NdbTransaction::NoCommit) != 0)
  {
    printf("Kilroy XXXI, error: %d\n", trans->getNdbError().code);
    return -1;
  }
  return 0;
}

int
execute_commit(NdbTransaction *trans)
{
  printf("Execute transaction\n");
  if (trans->execute(NdbTransaction::Commit) != 0)
  {
    printf("Kilroy I, error: %d\n", trans->getNdbError().code);
    return -1;
  }
  ndb->closeTransaction(trans);
  return 0;
}


int
create_key_value_row(const NdbDictionary::Dictionary *dict,
                     NdbTransaction *trans,
                     const char* start_value_ptr,
                     Uint64 key_id,
                     Uint32 this_value_len,
                     Uint32 ordinal,
                     char *buf)
{
  const NdbDictionary::Table *tab = dict->getTable("redis_key_values");
  if (tab == nullptr)
  {
    printf("Kilroy XXIV\n");
    return -1;
  }
  NdbOperation *op = trans->getNdbOperation(tab);
  if (op == nullptr)
  {
    printf("Kilroy XXIII\n");
    return -1;
  }
  op->insertTuple();
  op->equal("key_id", key_id);
  op->equal("ordinal", ordinal);
  memcpy(&buf[2], start_value_ptr, this_value_len);
  buf[0] = this_value_len & 255;
  buf[1] = this_value_len >> 8;
  op->equal("value", buf);
  return 0;
}

int
create_key_row(const NdbDictionary::Table *tab
               NdbTransaction *trans,
               const char *key_str,
               Uint32 key_len,
               const char *value_str,
               Uint32 value_len,
               Uint32 field_rows,
               Uint32 value_rows,
               Uint32 row_state,
               char *buf)
{
  NdbOperation *op = trans->getNdbOperation(tab);
  if (op == nullptr)
  {
    printf("Kilroy II\n");
    return -1;
  }
  op->writeTuple();

  memcpy(&buf[2], key_str, key_len);
  varsize_param[0] = key_len & 255;
  varsize_param[1] = key_len >> 8;
  op->equal("redis_key", buf);

  op->setValue("tot_value_len", value_len);
  op->setValue("value_rows", value_rows);
  op->setValue("field_rows", field_rows);
  op->setValue("tot_key_len", key_len);
  op->setValue("row_state", row_state);
  op->setValue("expiry_date", 0);

  if (value_len > INLINE_VALUE_LEN)
  {
    value_len = INLINE_VALUE_LEN;
  }
  memcpy(&buf[2], value_str, value_len);
  varsize_param[0] = value_len & 255;
  varsize_param[1] = value_len >> 8;
  op->setValue("redis_value", buf);
  if (op->getNdbError().code != 0)
  {
    printf("Error: %d\n", trans->getNdbError().code);
    return -1;
  }
  return 0;
}

int rondb_get_key_id(const NdbDictionary::Table *tab,
                     Uint64& key_id,
                     Ndb *ndb)
{
  if (ndb->getAutoIncrementValue(tab, key_id, unsigned(1024)) != 0)
  {
    if (ndb->getNdbError().code == 626)
    {
      if (ndb->setAutoIncrementValue(tab, Uint64(1), false) != 0)
      {
        printf("Kilroy IV: error: %d\n", ndb->getNdbError().code);
        return -1;
      }
      key_id = Uint64(1);
    }
    else
    {
      printf("Kilroy EIV: error: %d\n", ndb->getNdbError().code);
      return -1;
    }
  }
  return 0;
}

int
rondb_get_command(pink::RedisCmdArgsType& argv,
                  std::string* response,
                  int fd)
{
  if (argv.size() < 2)
  {
    return -1;
  }
  const char *key_str = argv[1].c_str();
  Uint32 key_len = strlen(key_str);
  return 0;
}

int
rondb_set_command(pink::RedisCmdArgsType& argv,
                  std::string* response,
                  int fd)
{
  if (argv.size() < 3)
  {
    return -1;
  }
  Ndb *ndb = rondb_ndb[0][0];
  const char *key_str = argv[1].c_str();
  Uint32 key_len = strlen(key_str);
  const char *value_str = argv[2].c_str();
  Uint32 value_len = strlen(value_str);
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *tab = dict->getTable("redis_main_key");
  if (tab == nullptr)
  {
    printf("Kilroy V\n");
    return -1;
  }
  Uint64 key_id;
  if (key_len > INLINE_KEY_VALUE_LEN)
  {
    int ret_code = rondb_get_key_id(tab, key_id, ndb);
    if (ret_code == -1)
    {
      printf("Kilroy XXV\n");
      return -1;
    }
  }
  NdbTransaction *trans = ndb->startTransaction(tab, key_str, key_len);
  if (trans == nullptr)
  {
    printf("Kilroy III\n");
    return -1;
  }
  char varsize_param[EXTENSION_VALUE_LEN + 500];
  if (key_len > MAX_KEY_VALUE_LEN)
  {
    printf("Kilroy XX, error: %d\n", op->getNdbError().code);
    return -1;
  }
  Uint32 value_rows = 0;
  if (value_len > INLINE_VALUE_LEN)
  {
    /**
     * The row doesn't fit in one RonDB row, create more rows
     * in the redis_key_values table.
     *
     * We also use the generated key_id which is the foreign
     * key column in the redis_main_key table such that
     * deleting the row in the main table ensures that all
     * value rows are also deleted.
     */
    Uint32 remaining_len = value_len - INLINE_VALUE_LEN;
    op->setValue("key_id", key_id);
    char *start_value_ptr = &value_str[INLINE_VALUE_LEN];
    do
    {
      Uint32 this_value_len = remaining_len;
      if (remaining_len > EXTENSION_VALUE_LEN)
      {
        this_value_len = EXTENSION_VALUE_LEN;
      }
      int ret_code = create_key_value_row(dict,
                                          trans,
                                          start_value_ptr,
                                          key_id,
                                          this_value_len,
                                          value_rows,
                                          &varsize_param[0]);
      if (ret_code == -1)
      {
        printf("Kilroy XXI\n");
        return -1;
      }
      if ((value_rows & 1) == 1)
      {
        if (execute_no_commit(trans) != 0)
        {
          printf("Kilroy XXXII\n");
          return -1;
        }
      }
      remaining_len -= this_value_len;
      start_value_ptr += this_value_len;
      value_rows++;
    } while (remaining_len > 0);
    value_len = INLINE_VALUE_LEN;
  }
  {
    int ret_code = create_key_row(tab,
                                  trans,
                                  key_str,
                                  key_len,
                                  value_str,
                                  value_len,
                                  Uint32(0),
                                  value_rows,
                                  Uint32(0),
                                  &varsize_param[0]);
    if (ret_code == -1)
    {
      printf("Kilroy XXI\n");
      return -1;
    }
  }
  if (execute_commit(trans) != 0)
  {
    printf("Kilroy XXXIII\n");
    return -1;
  }
  response->append("+OK\r\n");
}

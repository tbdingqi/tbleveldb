/* Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_ldb.cc

  @brief
  The ha_ldb engine is a stubbed storage engine for ldb purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/ldb/ha_ldb.h.

  @details
  ha_ldb will let you create/open/delete tables, but
  nothing further (for ldb, indexes are not supported nor can data
  be stored in the table). Use this ldb as a template for
  implementing the same functionality in your own storage engine. You
  can enable the ldb storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-ldb-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=LEVELDB;

  The ldb storage engine is set up to use table locks. It
  implements an ldb "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  ldb handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_ldb.h before reading the rest
  of this file.

  @note
  When you create an LEVELDB table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an ldb select that would do a scan of an entire
  table:

  @code
  ha_ldb::store_lock
  ha_ldb::external_lock
  ha_ldb::info
  ha_ldb::rnd_init
  ha_ldb::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::rnd_next
  ha_ldb::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_ldb::external_lock
  ha_ldb::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the ldb storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_ldb::open() would also have been necessary. Calls to
  ha_ldb::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be 
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#define MYSQL_SERVER 
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_ldb.h"
#include "probes_mysql.h"
#include "sql_plugin.h"


static handler *ldb_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

leveldb::Options options;    
leveldb::WriteOptions wo= leveldb::WriteOptions();
handlerton *ldb_hton;

/* Variables for ldb share methods */

/* 
   Hash used to track the number of open tables; variable for ldb share
   methods
*/
static HASH ldb_open_tables;

/* The mutex used to init the hash; variable for ldb share methods */
mysql_mutex_t ldb_mutex;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

static uchar* ldb_get_key(LEVELDB_SHARE *share, size_t *length,
                             my_bool not_used __attribute__((unused)))
{
  *length=share->table_name_length;
  return (uchar*) share->table_name;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_ldb, ex_key_mutex_LEVELDB_SHARE_mutex;

static PSI_mutex_info all_ldb_mutexes[]=
{
  { &ex_key_mutex_ldb, "ldb", PSI_FLAG_GLOBAL},
  { &ex_key_mutex_LEVELDB_SHARE_mutex, "LEVELDB_SHARE::mutex", 0}
};

static void init_ldb_psi_keys()
{
  const char* category= "ldb";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_ldb_mutexes);
  PSI_server->register_mutex(category, all_ldb_mutexes, count);
}
#endif



static int ldb_done_func(void *p)
{
  int error= 0;
  DBUG_ENTER("ldb_done_func");

  if (ldb_open_tables.records)
    error= 1;
  my_hash_free(&ldb_open_tables);
  mysql_mutex_destroy(&ldb_mutex);

  DBUG_RETURN(error);
}


/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each ldb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

static LEVELDB_SHARE *get_share(const char *table_name, TABLE *table)
{
  LEVELDB_SHARE *share;
  uint length;
  char *tmp_name;

  mysql_mutex_lock(&ldb_mutex);
  length=(uint) strlen(table_name);

  if (!(share=(LEVELDB_SHARE*) my_hash_search(&ldb_open_tables,
                                              (uchar*) table_name,
                                              length)))
  {
    if (!(share=(LEVELDB_SHARE *)
          my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                          &share, sizeof(*share),
                          &tmp_name, length+1,
                          NullS)))
    {
      mysql_mutex_unlock(&ldb_mutex);
      return NULL;
    }

    leveldb::Status s= leveldb_open(table_name, false, share->db);

    share->use_count=0;
    share->table_name_length=length;
    share->table_name=tmp_name;
    strmov(share->table_name,table_name);
    if (my_hash_insert(&ldb_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    mysql_mutex_init(ex_key_mutex_LEVELDB_SHARE_mutex,
                     &share->mutex, MY_MUTEX_INIT_FAST);
  }
  share->use_count++;
  mysql_mutex_unlock(&ldb_mutex);

  return share;

error:
  mysql_mutex_destroy(&share->mutex);
  my_free(share);

  return NULL;
}


/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the share, then we free memory associated with it.
*/

static int free_share(LEVELDB_SHARE *share)
{
  mysql_mutex_lock(&ldb_mutex);
  if (!--share->use_count)
  {
    delete share->db;
    share->db= NULL;
    my_hash_delete(&ldb_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&ldb_mutex);

  return 0;
}

static handler* ldb_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_ldb(hton, table);
}

static void free_trx(handlerton *hton, trx_t *trx)
{
  if (!trx)
    trx= (trx_t*) thd_get_ha_data(current_thd, hton);

  delete trx;
  trx= NULL;
  thd_set_ha_data(current_thd, hton, NULL);
}

static int ldb_init_func(void *p)
{
  DBUG_ENTER("ldb_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_ldb_psi_keys();
#endif

  ldb_hton= (handlerton *)p;
  mysql_mutex_init(ex_key_mutex_ldb, &ldb_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&ldb_open_tables,system_charset_info,32,0,0,
                      (my_hash_get_key) ldb_get_key,0,0);

  handlerton * hton = (handlerton*) p;
  hton->state        = SHOW_OPTION_YES;
  hton->db_type      = DB_TYPE_DEFAULT;
  hton->create      = ldb_create_handler;
  hton->show_status    = NULL;
//  hton->flags        = HTON_CAN_RECREATE;

//  ldb_hton->state= SHOW_OPTION_YES;
//  ldb_hton->create= ldb_create_handler;
//  ldb_hton->commit= ldb_commit;
//  ldb_hton->rollback= ldb_rollback;

//  ldb_hton->flags=   HTON_CAN_RECREATE;

  DBUG_RETURN(0);
}


ha_ldb::ha_ldb(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
}


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_ldb_exts[] = {
  NullS
};

const char **ha_ldb::bas_ext() const
{
  return ha_ldb_exts;
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/



int ha_ldb::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_ldb::open");
  if (!(share = get_share(name, table)))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
  
  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table. We call the free_share() function to free any resources
  that we have allocated in the "shared" structure.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_ldb::close(void)
{
  DBUG_ENTER("ha_ldb::close");
  DBUG_RETURN(free_share(share));
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an ldb of extracting all of the data as strings.
  ha_berekly.cc has an ldb of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

void ha_ldb::get_key(uchar *buf, std::string &key)
{
  KEY_PART_INFO *key_part= table->key_info[0].key_part;
  int bit_start= 0;

  if (key_part->type == HA_KEYTYPE_VARTEXT1 || key_part->type == HA_KEYTYPE_VARBINARY1)
    bit_start= 1;
  if (key_part->type == HA_KEYTYPE_VARTEXT2 || key_part->type == HA_KEYTYPE_VARBINARY2)
    bit_start= 2;

  key.append((char*)buf+key_part->offset+bit_start, key_part->length);
 
}

int ha_ldb::write_row(uchar *buf)
{
  DBUG_ENTER("ha_ldb::write_row");

  std::string key;
  std::string value;

  get_key(buf, key);

  size_t raw_len= table->s->rec_buff_length;
  size_t compressed_len= 0;

  if (my_compress(buf, &raw_len, &compressed_len))
  {
    compressed_len= raw_len;
  }

  value.append((char*)buf, raw_len);

  trx_t *trx= (trx_t*)thd_get_ha_data(current_thd, ldb_hton);
  trx->batch.Put(key, value);

  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for ldb by doing:
  @code
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
    table->timestamp_field->set_time();
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_ldb::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_ldb::update_row");

  std::string old_key;
  std::string new_key;

  get_key((uchar*) old_data, old_key);
  get_key((uchar*) new_data, new_key);
  
  trx_t *trx= (trx_t*)thd_get_ha_data(current_thd, ldb_hton);

  if (old_key.compare(new_key) != 0)
  {
    trx->batch.Delete(old_key);
  }

  write_row(new_data);

  DBUG_RETURN(0);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_ldb::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_ldb::delete_row");

  std::string key;
  get_key((uchar*)buf, key);

  trx_t *trx= (trx_t*)thd_get_ha_data(current_thd, ldb_hton);
  trx->batch.Delete(key);

  DBUG_RETURN(0);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/
int ha_ldb::index_read(uchar *buf, const uchar *key, uint key_len, ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_ldb::index_read");

  std::string skey;
  std::string svalue;
  int bit_start= table->key_info[0].key_part[0].key_part_flag & HA_VAR_LENGTH_PART ? 2 : 0;

  skey.append((char*)(key+bit_start), key_len-bit_start);
  leveldb::Status s = share->db->Get(leveldb::ReadOptions(), skey, &svalue);

  if (!s.ok())
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  memcpy(buf, svalue.c_str(), svalue.length());

  size_t uncomlen= table->s->rec_buff_length;
  size_t comlen= svalue.length();
  my_uncompress(buf, comlen, &uncomlen);
  

  table->status= 0;
  DBUG_RETURN(0);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_ldb::index_next(uchar *buf)
{
  DBUG_ENTER("ha_ldb::index_next");
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(HA_ERR_END_OF_FILE);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_ldb::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_ldb::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_ldb::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_ldb::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_ldb::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_ldb::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the ldb in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_ldb::rnd_init(bool scan)
{
  DBUG_ENTER("ha_ldb::rnd_init");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_ldb::rnd_end()
{
  DBUG_ENTER("ha_ldb::rnd_end");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_ldb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_ldb::rnd_next");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_ldb::position(const uchar *record)
{
  DBUG_ENTER("ha_ldb::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_ldb::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_ldb::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_ldb::info(uint flag)
{
  DBUG_ENTER("ha_ldb::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_ldb::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_ldb::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_ldb::delete_all_rows()
{
  DBUG_ENTER("ha_ldb::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_ldb::truncate()
{
  DBUG_ENTER("ha_ldb::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_ldb::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_ldb::external_lock");
  thd= current_thd;
  trx_t *trx;
  if (lock_type != F_UNLCK)
  {
    trx= (trx_t*) thd_get_ha_data(thd, ldb_hton);
    if (!trx)
    {
      trx= new trx_t;
      trx->pobj= this;
      thd_set_ha_data(current_thd, ldb_hton, trx);
    }
    DBUG_RETURN(0);
  }
  else
  {
    trx= (trx_t*) thd_get_ha_data(thd, ldb_hton);

    if (!trx)
    {
      DBUG_RETURN(1);
    }
 
    leveldb::Status s= trx->pobj->share->db->Write(wo, &trx->batch);
 
    free_trx(ldb_hton, trx);

    DBUG_RETURN(!s.ok());
  }
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for ldb, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_ldb::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  DBUG_ENTER("store_lock");

  const bool in_lock_tables = thd_in_lock_tables(thd);
  const uint sql_command = thd_sql_command(thd);
  if ((lock_type >= TL_WRITE_CONCURRENT_INSERT
       && lock_type <= TL_WRITE)
       && !(in_lock_tables
       && sql_command == SQLCOM_LOCK_TABLES)
       && !thd_tablespace_op(thd)
       && sql_command != SQLCOM_TRUNCATE
       && sql_command != SQLCOM_OPTIMIZE
       && sql_command != SQLCOM_CREATE_TABLE)
  {
    lock_type = TL_WRITE_ALLOW_WRITE;
  }

  if ( lock_type!=TL_IGNORE && lock.type==TL_UNLOCK )
    lock.type=lock_type;

  *to++ = &lock;
  DBUG_RETURN(to);
}
/*
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}
*/

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_ldb::delete_table(const char *name)
{
  DBUG_ENTER("ha_ldb::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  dbpath.assign(name);
  leveldb::DestroyDB(dbpath, leveldb::Options());
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_ldb::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_ldb::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_ldb::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_ldb::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


leveldb::Status leveldb_open(const char *name, bool create_if_missing, leveldb::DB* &db)
{
  std::string dbpath;    
  leveldb::Status status;

  dbpath.assign(name);
  options.write_buffer_size= 33554432;
  options.create_if_missing= create_if_missing;
  status = leveldb::DB::Open(options, dbpath, &db);
  wo.sync= true;

  return status;
}

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_ldb::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_ldb::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */

  if (table_arg->s->key_parts != 1 || table_arg->s->uniques != 1)
  {
    DBUG_RETURN(my_errno= HA_ERR_WRONG_INDEX);
  }

  leveldb::DB* db= NULL;
  leveldb::Status s;

  dbpath.assign(name);
  s= leveldb_open(name, true, db);
  DBUG_RETURN(!s.ok());
}

struct st_mysql_storage_engine ldb_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static struct st_mysql_sys_var* ldb_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  NULL
};

// this is an ldb of SHOW_FUNC and of my_snprintf() service
static int show_func_ldb(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"ldb_func_ldb",  (char *)show_func_ldb, SHOW_FUNC},
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(ldb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &ldb_storage_engine,
  "LEVELDB",
  "dingqi, taobao.com",
  "LevelDB storage engine",
  PLUGIN_LICENSE_GPL,
  ldb_init_func,                            /* Plugin Init */
  ldb_done_func,                            /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  ldb_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;

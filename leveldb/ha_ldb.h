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

/** @file ha_ldb.h

    @brief
  The ha_ldb engine is a stubbed storage engine for ldb purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/ldb/ha_ldb.cc.

    @note
  Please read ha_ldb.cc before reading this file.
  Reminder: The ldb storage engine implements all methods that are *required*
  to be implemented. For a full list of all methods that you can implement, see
  handler.h.

   @see
  /sql/handler.h and /storage/ldb/ha_ldb.cc
*/
#undef max
#undef min
#undef _POSIX_TIMEOUTS

#define _GLIBCXX_GTHREAD_USE_WEAK 0
#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include "db.h"
#include "leveldb/write_batch.h"

#define LDB_MAX_KEY_LENGTH 3500 // Same as innodb
/** @brief
  LEVELDB_SHARE is a structure that will be shared among all open handlers.
  This ldb implements the minimum of what you will probably need.
*/
class ha_ldb;

typedef struct st_ldb_share {
  char *table_name;
  uint table_name_length,use_count;
  leveldb::DB* db;
  mysql_mutex_t mutex;
  THR_LOCK lock;
} LEVELDB_SHARE;

typedef struct st_trx_t{
  ha_ldb *pobj;
  leveldb::WriteBatch batch;
}trx_t;

leveldb::Status leveldb_open(const char *name, bool create_if_missing, leveldb::DB* &db);
/** @brief
  Class definition for the storage engine
*/
class ha_ldb: public handler
{
  THR_LOCK_DATA lock;      ///< MySQL lock

  std::string dbpath;    
  THD *thd;
  void get_key(uchar* buf, std::string &key);
public:
  LEVELDB_SHARE *share;    ///< Shared lock info

  ha_ldb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_ldb()
  {
  }
  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const { return "LEVELDB"; }

  /** @brief
    The name of the index type that will be used for display.
    Don't implement this method unless you really have indexes.
   */
  const char *index_type(uint inx) { return "HASH"; }

  /** @brief
    The file extensions.
   */
  const char **bas_ext() const;

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const
  {
    /*
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    */
    return HA_NO_TRANSACTIONS | HA_BINLOG_FLAGS | HA_NO_AUTO_INCREMENT | HA_PRIMARY_KEY_REQUIRED_FOR_DELETE;
  }

  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return (HA_READ_NEXT);
  }
  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys()          const { return MAX_KEY; }
  uint max_supported_key_parts()     const { return MAX_REF_PARTS; }
  uint max_supported_key_length()    const { return LDB_MAX_KEY_LENGTH; }
  uint max_supported_key_part_length() const { return LDB_MAX_KEY_LENGTH; }

  virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  virtual double read_time(uint, uint, ha_rows rows)
  { return (double) rows /  20.0+1; }

  /*
    Everything below are methods that we implement in ha_ldb.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_ldb.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked);    // required

  /** @brief
    We implement this in ha_ldb.cc; it's a required method.
  */
  int close(void);                                              // required

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read(uchar *buf, const uchar *key,
                             uint key_len, ha_rkey_function find_flag);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf);

  /** @brief
    We implement this in ha_ldb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf);

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan);                                      //required
  int rnd_end();
  int rnd_next(uchar *buf);                                     ///< required
  int rnd_pos(uchar *buf, uchar *pos);                          ///< required
  void position(const uchar *record);                           ///< required
  int info(uint);                                               ///< required
  int extra(enum ha_extra_function operation);
  int external_lock(THD *thd, int lock_type);                   ///< required
  int delete_all_rows(void);
  int truncate();
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key);
  int delete_table(const char *from);
  int rename_table(const char * from, const char * to);
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info);                      ///< required

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required
};

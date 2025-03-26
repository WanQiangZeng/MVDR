#pragma once

#include "global.h"
#include "mcs_spinlock.h"
#include "my_list.h"
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <list>
#include <mm_malloc.h>
#include <mutex>

#define DECL_SET_VALUE(type) void set_value(int col_id, type value);

#define SET_VALUE(type)                                                        \
  void row_t::set_value(int col_id, type value) { set_value(col_id, &value); }

#define DECL_GET_VALUE(type) void get_value(int col_id, type &value);

#define GET_VALUE(type)                                                        \
  void row_t::get_value(int col_id, type &value) {                             \
    value = *(type *)get_value(col_id);                                        \
  }

//		int pos = get_schema()->get_field_index(col_id);
//		value = *(type *)&data[pos];
//	}

class table_t;
class Catalog;

struct LockEntry {
  // 保护row
  pthread_mutex_t row_mutex = PTHREAD_MUTEX_INITIALIZER;
  // 保护锁表的owner和waiter等变量
  std::mutex entry_mutex;
  lock_t lock_type = LOCK_NONE;
  int64_t owner = -1;
};

class row_t {
public:
  // 复制构造函数
  row_t(const row_t &other) {
    _primary_key = other._primary_key;
    _part_id = other._part_id;
    _row_id = other._row_id;
    table = other.table;
    version = other.version;
    writing_id = other.writing_id;

    if (other.data) {
      data = (char *)_mm_malloc(sizeof(char) * other.datasize, 64);
      memcpy(data, other.data, other.datasize);
    } else {
      data = nullptr;
    }

    lock_entry.owner = other.lock_entry.owner;
    lock_entry.lock_type = other.lock_entry.lock_type;
  }

  RC init(table_t *host_table, uint64_t part_id, uint64_t row_id = 0);
  void init(int size);
  RC switch_schema(table_t *host_table);

  table_t *get_table();
  Catalog *get_schema();
  const char *get_table_name();
  uint64_t get_field_cnt();
  uint64_t get_tuple_size();
  uint64_t get_row_id() { return _row_id; };

  void copy(row_t *src); // 仅复制data部分值
  void copy(row_t *src, int idx);

  void set_primary_key(uint64_t key) { _primary_key = key; };
  uint64_t get_primary_key() { return _primary_key; };
  uint64_t get_part_id() { return _part_id; };

  void set_value(int id, void *ptr);
  void set_value_plain(int id, void *ptr);
  void set_value(int id, void *ptr, int size);
  void set_value(const char *col_name, void *ptr);
  char *get_value(int id);
  char *get_value_plain(uint64_t id);
  char *get_value(char *col_name);
  void inc_value(int id, uint64_t val);
  void dec_value(int id, uint64_t val);

  DECL_SET_VALUE(uint64_t);
  DECL_SET_VALUE(int64_t);
  DECL_SET_VALUE(double);
  DECL_SET_VALUE(UInt32);
  DECL_SET_VALUE(SInt32);

  DECL_GET_VALUE(uint64_t);
  DECL_GET_VALUE(int64_t);
  DECL_GET_VALUE(double);
  DECL_GET_VALUE(UInt32);
  DECL_GET_VALUE(SInt32);

  void set_data(char *data, uint64_t size);
  char *get_data();

  void free_row();

  char *data;
  table_t *table;
  uint64_t datasize;

  LockEntry lock_entry;

  // 用于silo,writing_id作为写锁
  int64_t version = -1;
  int64_t writing_id = -1;

private:
  uint64_t _primary_key; // 主键
  uint64_t _part_id;
  uint64_t _row_id;
};

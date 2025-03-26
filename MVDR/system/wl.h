#pragma once

#include "global.h"
#include <atomic>

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class index_base;

class workload {
public:
  // 表通过表名索引
  map<string, table_t *> tables;
  map<string, INDEX *> indexes;

  virtual RC init();
  // 通过目录文件初始化表的属性
  virtual RC init_schema(string schema_file);
  virtual RC init_table() = 0;

  std::atomic_bool sim_done;

  void index_insert(string index_name, uint64_t key, row_t *row); // 已废弃
  void index_insert(INDEX *index, uint64_t key, row_t *row,
                    int64_t part_id = -1); // 将row插入索引
};

#pragma once

#include "global.h"
#include <mutex>

class table_t;

enum RC;

class index_base {
public:
  virtual RC init() { return RCOK; };
  virtual RC init(uint64_t size) { return RCOK; };

  virtual bool index_exist(idx_key_t key) = 0; // 检验索引是否存在

  virtual RC index_insert(idx_key_t key, itemid_t *item,
                          int part_id = -1) = 0; // 插入索引

  virtual RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1) = 0;

  virtual RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1,
                        int thd_id = 0) = 0;

  virtual RC index_remove(idx_key_t key) { return RCOK; };

  // 此表上的索引
  table_t *table;

  mutex index_mutex;
};

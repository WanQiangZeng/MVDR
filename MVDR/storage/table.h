#pragma once

#include "global.h"

class Catalog;
class row_t;

class table_t {
public:
  void init(Catalog *schema);
  RC get_new_row(row_t *&row); // 已废弃
  // 生成新行并初始化
  RC get_new_row(row_t *&row, uint64_t part_id, uint64_t &row_id);

  void delete_row(); // 已废弃

  uint64_t get_table_size() { return cur_tab_size; };
  Catalog *get_schema() { return schema; };
  const char *get_table_name() { return table_name; };
  void set_table_name(const char *name) { table_name = name; };

  // 目录
  Catalog *schema;

private:
  const char *table_name;
  uint64_t cur_tab_size;                  // 存在的行数
  char pad[CL_SIZE - sizeof(void *) * 3]; // 填充到cache line大小
};

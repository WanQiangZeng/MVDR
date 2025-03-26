#pragma once

#include "global.h"
#include "helper.h"
#include <map>
#include <vector>

// 属性
class Column {
public:
  Column() {
    this->type = new char[80];
    this->name = new char[80];
  }
  Column(uint64_t size, char *type, char *name, uint64_t id, uint64_t index) {
    this->size = size;
    this->id = id;
    this->index = index;
    this->type = new char[80];
    this->name = new char[80];
    strcpy(this->type, type);
    strcpy(this->name, name);
  };

  UInt64 id;    // 从0到字段总数-1
  UInt32 size;  // 此字段的字节大小
  UInt32 index; // char* data，即row中的偏移量
  char *type;
  char *name;
  char pad[CL_SIZE - sizeof(uint64_t) * 3 - sizeof(char *) * 2]; // 填充
};

// 目录
class Catalog { // 存储表的元数据
public:
  void init(char *table_name, int field_cnt);
  void add_col(char *col_name, uint64_t size, char *type);

  UInt32 field_cnt; // 已经存在的字段数量
  char *table_name;

  UInt32 get_tuple_size() { return tuple_size; };

  uint64_t get_field_cnt() { return field_cnt; };
  uint64_t get_field_size(int id) { return _columns[id].size; };
  uint64_t get_field_index(int id) { return _columns[id].index; };
  char *get_field_type(uint64_t id);
  char *get_field_name(uint64_t id);
  uint64_t get_field_id(const char *name);
  char *get_field_type(char *name);
  uint64_t get_field_index(char *name);

  void print_schema();
  Column *_columns;  // 属性
  UInt32 tuple_size; // 此表中所有字段的字节大小数之和
};

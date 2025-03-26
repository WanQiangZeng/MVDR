#include "row.h"
#include "catalog.h"
#include "global.h"
#include "mem_alloc.h"
#include "table.h"
#include <mm_malloc.h>

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id) {
  lock_entry.owner = -1;
  lock_entry.lock_type = LOCK_NONE;
  version = -1;
  writing_id = -1;
  _row_id = row_id;
  _part_id = part_id;
  this->table = host_table;
  Catalog *schema = host_table->get_schema();
  datasize = schema->get_tuple_size();
  data = (char *)_mm_malloc(sizeof(char) * datasize, 64);
  return RCOK;
}

void row_t::init(int size) { data = (char *)_mm_malloc(size, 64); }

RC row_t::switch_schema(table_t *host_table) {
  this->table = host_table;
  return RCOK;
}

table_t *row_t::get_table() { return table; }

Catalog *row_t::get_schema() { return get_table()->get_schema(); }

const char *row_t::get_table_name() { return get_table()->get_table_name(); };
uint64_t row_t::get_tuple_size() { return get_schema()->get_tuple_size(); }

uint64_t row_t::get_field_cnt() { return get_schema()->field_cnt; }

void row_t::inc_value(int id, uint64_t val) {
  int pos = get_schema()->get_field_index(id);
  ATOM_ADD(data[pos], val);
}

void row_t::dec_value(int id, uint64_t val) {
  int pos = get_schema()->get_field_index(id);
  ATOM_SUB(data[pos], val);
}

void row_t::set_value(int id, void *ptr) {
  int datasize = get_schema()->get_field_size(id);
  int pos = get_schema()->get_field_index(id);
  memcpy(&data[pos], ptr, datasize);
  // debugging
  assert(data);
  assert(ptr);
}

void row_t::set_value(int id, void *ptr, int size) {
  int pos = get_schema()->get_field_index(id);
  memcpy(&data[pos], ptr, size);
  // debugging
  assert(data);
  assert(ptr);
}

void row_t::set_value(const char *col_name, void *ptr) {
  uint64_t id = get_schema()->get_field_id(col_name);
  set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char *row_t::get_value(int idx) {
  uint64_t id = (uint64_t)idx;
  return get_value_plain(id);
}

char *row_t::get_value_plain(uint64_t id) {
  int pos = get_schema()->get_field_index(id);
  return &data[pos];
}

char *row_t::get_value(char *col_name) {
  uint64_t pos = get_schema()->get_field_index(col_name);
  return &data[pos];
}

char *row_t::get_data() { return data; }

void row_t::set_data(char *data, uint64_t size) {
  memcpy(this->data, data, size);
  assert(data);
  assert(this->data);
}
// copy from the src to this
void row_t::copy(row_t *src) {
  set_data(src->get_data(), src->get_tuple_size());
}

void row_t::set_value_plain(int idx, void *ptr) {
  int datasize = get_schema()->get_field_size(idx);
  int pos = get_schema()->get_field_index(idx);
  memcpy(&data[pos], ptr, datasize);
  // debugging
  assert(data);
  assert(ptr);
}

void row_t::copy(row_t *src, int idx) {
  char *ptr = src->get_value_plain(idx);
  set_value_plain(idx, ptr);
  // debugging
  assert(data);
  assert(src->data);
}

void row_t::free_row() { free(data); }

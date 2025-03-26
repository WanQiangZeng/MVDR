#include "index_btree.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include "test.h"

RC TestWorkload::init() {
  workload::init();
  string path;
  path = "./benchmarks/TEST_schema.txt";
  init_schema(path.c_str());

  init_table();
  return RCOK;
}

RC TestWorkload::init_schema(const char *schema_file) {
  workload::init_schema(schema_file);
  the_table = tables["MAIN_TABLE"];
  the_index = indexes["MAIN_INDEX"];
  return RCOK;
}

RC TestWorkload::init_table() {
  RC rc = RCOK;
  for (int rid = 0; rid < 10; rid++) {
    row_t *new_row = NULL;
    uint64_t row_id;
    int part_id = 0;
    rc = the_table->get_new_row(new_row, part_id, row_id);
    assert(rc == RCOK);
    uint64_t primary_key = rid;
    new_row->set_primary_key(primary_key);
    new_row->set_value(0, rid);
    new_row->set_value(1, 0);
    new_row->set_value(2, 0);
    index_insert(the_index, primary_key, new_row, part_id);
    assert(rc == RCOK);
    std::cout << "cur_tab_size: " << the_table->get_table_size() << std::endl;
  }
  return rc;
}

void TestWorkload::summarize() {
  uint64_t curr_time = get_sys_clock();
  if (g_test_case == CONFLICT) {
    assert(curr_time - time > g_thread_cnt * 1e9);
    int total_wait_cnt = 0;
    for (UInt32 tid = 0; tid < g_thread_cnt; tid++) {
      total_wait_cnt += stats._stats[tid]->wait_cnt;
    }
    printf("CONFLICT TEST. PASSED.\n");
  }
}

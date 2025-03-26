#include "global.h"
#include "index_hash.h"
#include "row.h"
#include "test.h"
#include <iostream>

void TestTxnMan::init(workload *h_wl) { _wl = (TestWorkload *)h_wl; }

RC TestTxnMan::run_txn(int type, int access_num) {
  switch (type) {
  case READ_WRITE:
    return testReadwrite(access_num);
  case CONFLICT:
    return testConflict(access_num);
  default:
    assert(false);
    return Abort;
  }
}

RC TestTxnMan::testReadwrite(int access_num) {
  RC rc = RCOK;
  itemid_t *m_item;
  for (int rid = 0; rid < 10; rid++) {
    _wl->the_index->index_read(rid, m_item, 0, 0);
    row_t *row = ((row_t *)m_item->location);
    row_t *row_local(row);
    if (access_num == 0) { // 写
      char str[] = "hello";
      row_local->set_value(0, (rid) * 10);
      row_local->set_value(1, (rid) * 100);
      row_local->set_value(2, rid * 1000);
      row_local->set_value(3, str);
      cout << "row_id=" << row_local->get_row_id() << "row_primary= " << rid
           << " has already write" << endl;
    } else { // 读
      int v1;
      double v2;
      uint64_t v3;

      row_local->get_value(0, v1);
      row_local->get_value(1, v2);
      row_local->get_value(2, v3);
      char *v4;
      v4 = row_local->get_value(3);

      assert(v1 == rid * 10);
      assert(v2 == rid * 100);
      assert(v3 == rid * 1000);
      assert(strcmp(v4, "hello") == 0);
      cout << "row_primary " << rid << " has already read" << endl;
      cout << "v1=" << v1 << " v2=" << v2 << " v3=" << v3 << " v4=" << v4
           << endl;
    }
  }
  if (access_num == 0)
    return RCOK;
  else
    return FINISH;
}

RC TestTxnMan::testConflict(int access_num) {
  RC rc = RCOK;
  itemid_t *m_item;

  idx_key_t key;
  for (key = 0; key < 1; key++) {
    _wl->the_index->index_read(key, m_item, 0, 0);
    row_t *row = ((row_t *)m_item->location);
    row_t *row_local(row);
    if (row_local) {
      char str[] = "hello";
      row_local->set_value(0, 1234);
      row_local->set_value(1, 1234.5);
      row_local->set_value(2, 8589934592UL);
      row_local->set_value(3, str);
      sleep(1);
    } else {
      rc = Abort;
      break;
    }
  }
  return rc;
}

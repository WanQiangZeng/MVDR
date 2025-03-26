#ifndef _BTREE_H_
#define _BTREE_H_

#include "global.h"
#include "helper.h"
#include "index_base.h"

typedef struct bt_node {
  // 对于非叶子节点，指向bt_nodes
  void **pointers;
  bool is_leaf;
  idx_key_t *keys;
  bt_node *parent;
  UInt32 num_keys;
  bt_node *next;
  bool latch;
  pthread_mutex_t locked;
  latch_t latch_type;
  UInt32 share_cnt;
} bt_node;

struct glob_param {
  uint64_t part_id;
};

class index_btree : public index_base {
public:
  RC init(uint64_t part_cnt);
  RC init(uint64_t part_cnt, table_t *table);
  bool index_exist(idx_key_t key);
  RC index_insert(idx_key_t key, itemid_t *item, int part_id = -1);
  RC index_read(idx_key_t key, itemid_t *&item, uint64_t thd_id,
                int64_t part_id = -1);
  RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1);
  RC index_read(idx_key_t key, itemid_t *&item);
  RC index_next(uint64_t thd_id, itemid_t *&item, bool samekey = false);

private:
  uint64_t part_cnt;
  RC make_lf(uint64_t part_id, bt_node *&node);
  RC make_nl(uint64_t part_id, bt_node *&node);
  RC make_node(uint64_t part_id, bt_node *&node);

  RC start_new_tree(glob_param params, idx_key_t key, itemid_t *item);
  RC find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type,
               bt_node *&leaf, bt_node *&last_ex);
  RC find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type,
               bt_node *&leaf);
  RC insert_into_leaf(glob_param params, bt_node *leaf, idx_key_t key,
                      itemid_t *item);

  RC split_lf_insert(glob_param params, bt_node *leaf, idx_key_t key,
                     itemid_t *item);
  RC split_nl_insert(glob_param params, bt_node *node, UInt32 left_index,
                     idx_key_t key, bt_node *right);
  RC insert_into_parent(glob_param params, bt_node *left, idx_key_t key,
                        bt_node *right);
  RC insert_into_new_root(glob_param params, bt_node *left, idx_key_t key,
                          bt_node *right);

  int leaf_has_key(bt_node *leaf, idx_key_t key);

  UInt32 cut(UInt32 length);
  UInt32 order;    // 节点中的键数（对于叶子和非叶子）
  bt_node **roots; // 每个分区都有一个不同的根
  bt_node *find_root(uint64_t part_id);

  bool latch_node(bt_node *node, latch_t latch_type);
  latch_t release_latch(bt_node *node);
  RC upgrade_latch(bt_node *node);

  RC cleanup(bt_node *node, bt_node *last_ex);

  // 叶子节点和线程最后访问的叶子之间的idx
  bt_node ***cur_leaf_per_thd;
  UInt32 **cur_idx_per_thd;
};

#endif

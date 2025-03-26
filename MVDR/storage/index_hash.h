#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

// 一个bucketnode可以哈希存储若干个item
class BucketNode {
public:
  BucketNode(idx_key_t key) { init(key); };
  void init(idx_key_t key) {
    this->key = key;
    next = NULL;
    items = NULL;
  }
  idx_key_t key;

  BucketNode *next;
  // 此bucketnode节点上存储的item链表,头插法
  itemid_t *items;
};

// bucketheader实现并发线程安全
class BucketHeader {
public:
  void init();
  void insert_item(idx_key_t key, itemid_t *item,
                   int part_id); // 插入到对应的bucketnode的items的链头
  void read_item(idx_key_t key, itemid_t *&item, const char *tname);
  BucketNode *first_node;
  uint64_t node_cnt;
  bool locked;
  pthread_rwlock_t *rwlock; // 读写锁
};

class IndexHash : public index_base {
public:
  RC init(uint64_t bucket_cnt, int part_cnt);
  RC init(int part_cnt, table_t *table, uint64_t bucket_cnt);
  bool index_exist(idx_key_t key); // 检验索引是否存在
  RC index_insert(idx_key_t key, itemid_t *item,
                  int part_id = -1); // 读写索引通过加线程锁实现
  // the following call returns a single item
  RC index_read(idx_key_t key, itemid_t *&item,
                int part_id = -1); // 根据key获取目标item
  RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1,
                int thd_id = 0);

private:
  void get_latch(BucketHeader *bucket);
  void get_latch(BucketHeader *bucket, access_t access);
  void release_latch(BucketHeader *bucket);

  // TODO implement more complex hash function
  uint64_t hash(idx_key_t key) { return key % _bucket_cnt_per_part; }

  BucketHeader **_buckets; // part_cnt个bucket链
  uint64_t _bucket_cnt;
  uint64_t _bucket_cnt_per_part;
};

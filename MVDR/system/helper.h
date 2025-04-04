#pragma once

#include "global.h"
#include "mcs_spinlock.h"
#include "my_list.h"
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <list>
#include <mutex>
#include <queue>
#include <row.h>
#include <stdint.h>
#include <vector>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) __sync_fetch_and_sub(&(dest), value)
#define ATOM_CAS(dest, oldval, newval)                                         \
  __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) __sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) __sync_sub_and_fetch(&(dest), value)

#define COMPILER_BARRIER asm volatile("" ::: "memory");
#define PAUSE                                                                  \
  { __asm__("pause;"); }
// #define PAUSE usleep(1);

/************************************************/
// ASSERT Helper
/************************************************/
#define M_ASSERT(cond, ...)                                                    \
  if (!(cond)) {                                                               \
    printf("ASSERTION FAILURE [%s : %d] ", __FILE__, __LINE__);                \
    printf(__VA_ARGS__);                                                       \
    assert(false);                                                             \
  }

#define ASSERT(cond) assert(cond)

/************************************************/
// QUEUE helper (push & pop)
/************************************************/
#define RETURN_PUSH(head, entry)                                               \
  {                                                                            \
    if (head == NULL) {                                                        \
      head = entry;                                                            \
      entry->next = NULL;                                                      \
    } else {                                                                   \
      entry->next = head;                                                      \
      head = entry;                                                            \
    }                                                                          \
  }

#define QUEUE_PUSH(head, tail, entry)                                          \
  {                                                                            \
    entry->next = NULL;                                                        \
    if (head == NULL) {                                                        \
      head = entry;                                                            \
      tail = entry;                                                            \
    } else {                                                                   \
      tail->next = entry;                                                      \
      tail = entry;                                                            \
    }                                                                          \
  }

#define QUEUE_RM(head, tail, prev, en, cnt)                                    \
  {                                                                            \
    if (prev != NULL)                                                          \
      prev->next = en->next;                                                   \
    else if (head == en)                                                       \
      head = en->next;                                                         \
    if (tail == en)                                                            \
      tail = prev;                                                             \
    cnt--;                                                                     \
  }

/************************************************/
// STACK helper (push & pop)
/************************************************/
#define STACK_POP(stack, top)                                                  \
  {                                                                            \
    if (stack == NULL)                                                         \
      top = NULL;                                                              \
    else {                                                                     \
      top = stack;                                                             \
      stack = stack->next;                                                     \
    }                                                                          \
  }
#define STACK_PUSH(stack, entry)                                               \
  {                                                                            \
    entry->next = stack;                                                       \
    stack = entry;                                                             \
  }

/************************************************/
// LIST helper (read from head & write to tail)
/************************************************/
#define LIST_GET_HEAD(lhead, ltail, en)                                        \
  {                                                                            \
    en = lhead;                                                                \
    lhead = lhead->next;                                                       \
    if (lhead)                                                                 \
      lhead->prev = NULL;                                                      \
    else                                                                       \
      ltail = NULL;                                                            \
    en->next = NULL;                                                           \
  }

#define LIST_PUT_TAIL(lhead, ltail, en)                                        \
  {                                                                            \
    en->next = NULL;                                                           \
    en->prev = NULL;                                                           \
    if (ltail) {                                                               \
      en->prev = ltail;                                                        \
      ltail->next = en;                                                        \
      ltail = en;                                                              \
    } else {                                                                   \
      lhead = en;                                                              \
      ltail = en;                                                              \
    }                                                                          \
  }

#define LIST_INSERT_BEFORE(entry, newentry)                                    \
  {                                                                            \
    newentry->next = entry;                                                    \
    newentry->prev = entry->prev;                                              \
    if (entry->prev)                                                           \
      entry->prev->next = newentry;                                            \
    entry->prev = newentry;                                                    \
  }

#define LIST_REMOVE(entry)                                                     \
  {                                                                            \
    if (entry->next)                                                           \
      entry->next->prev = entry->prev;                                         \
    if (entry->prev)                                                           \
      entry->prev->next = entry->next;                                         \
  }

#define LIST_REMOVE_HT(entry, head, tail)                                      \
  {                                                                            \
    if (entry->next)                                                           \
      entry->next->prev = entry->prev;                                         \
    else {                                                                     \
      assert(entry == tail);                                                   \
      tail = entry->prev;                                                      \
    }                                                                          \
    if (entry->prev)                                                           \
      entry->prev->next = entry->next;                                         \
    else {                                                                     \
      assert(entry == head);                                                   \
      head = entry->next;                                                      \
    }                                                                          \
  }

#define LIST_RM(head, tail, en, cnt)                                           \
  {                                                                            \
    if (en->next)                                                              \
      en->next->prev = en->prev;                                               \
    if (en->prev)                                                              \
      en->prev->next = en->next;                                               \
    else if (head == en) {                                                     \
      head = en->next;                                                         \
    }                                                                          \
    if (tail == en) {                                                          \
      tail = en->prev;                                                         \
    }                                                                          \
    cnt--;                                                                     \
  }

#define LIST_RM_SINCE(head, tail, en)                                          \
  {                                                                            \
    if (en->prev)                                                              \
      en->prev->next = NULL;                                                   \
    else if (head == en) {                                                     \
      head = NULL;                                                             \
    }                                                                          \
    tail = en->prev;                                                           \
  }

#define LIST_INSERT_BEFORE_CH(lhead, entry, newentry)                          \
  {                                                                            \
    newentry->next = entry;                                                    \
    newentry->prev = entry->prev;                                              \
    if (entry->prev)                                                           \
      entry->prev->next = newentry;                                            \
    entry->prev = newentry;                                                    \
    if (lhead == entry)                                                        \
      lhead = newentry;                                                        \
  }

/************************************************/
// STATS helper
/************************************************/
#define INC_STATS(tid, name, value)                                            \
  if (STATS_ENABLE)                                                            \
    stats._stats[tid]->name += value;

#define DEC_STATS(tid, name, value)                                            \
  if (STATS_ENABLE)                                                            \
    stats._stats[tid]->name -= value;

#define INC_TMP_STATS(tid, name, value)                                        \
  if (STATS_ENABLE)                                                            \
    stats.tmp_stats[tid]->name += value;

#define INC_GLOB_STATS(name, value)                                            \
  if (STATS_ENABLE)                                                            \
    stats.name += value;

#define UPDATE_STATS(tid, name, value)                                         \
  if (STATS_ENABLE)                                                            \
    stats._stats[tid]->name = max(stats._stats[tid]->name, value);

#define ADD_PER_PRIO_STATS(tid, name, prio, value)                             \
  if (STATS_ENABLE)                                                            \
    stats._stats[tid]->prio_metrics[prio].add_##name(value);

/************************************************/
// malloc helper
/************************************************/
// 为了避免伪共享，任何未共享的读写数组都应修改为只读数组，指向线程本地数据块。

#define ARR_PTR_MULTI(type, name, size, scale)                                 \
  name = new type *[size];                                                     \
  if (g_part_alloc || THREAD_ALLOC) {                                          \
    for (UInt32 i = 0; i < size; i++) {                                        \
      UInt32 padsize = sizeof(type) * (scale);                                 \
      if (g_mem_pad && padsize % CL_SIZE != 0)                                 \
        padsize += CL_SIZE - padsize % CL_SIZE;                                \
      name[i] = (type *)mem_allocator.alloc(padsize, i);                       \
      for (UInt32 j = 0; j < scale; j++)                                       \
        new (&name[i][j]) type();                                              \
    }                                                                          \
  } else {                                                                     \
    for (UInt32 i = 0; i < size; i++)                                          \
      name[i] = new type[scale];                                               \
  }

#define ARR_PTR(type, name, size) ARR_PTR_MULTI(type, name, size, 1)

#define ARR_PTR_INIT(type, name, size, value)                                  \
  name = new type *[size];                                                     \
  if (g_part_alloc) {                                                          \
    for (UInt32 i = 0; i < size; i++) {                                        \
      int padsize = sizeof(type);                                              \
      if (g_mem_pad && padsize % CL_SIZE != 0)                                 \
        padsize += CL_SIZE - padsize % CL_SIZE;                                \
      name[i] = (type *)mem_allocator.alloc(padsize, i);                       \
      new (name[i]) type();                                                    \
    }                                                                          \
  } else                                                                       \
    for (UInt32 i = 0; i < size; i++)                                          \
      name[i] = new type;                                                      \
  for (UInt32 i = 0; i < size; i++)                                            \
    *name[i] = value;

enum Data_type { DT_table, DT_page, DT_row };

// 标识一个事务的request和cid
struct id_data {
  int64_t cid = -1;
  int64_t qid = -1;
  int64_t rid = -1;
};

// 每一行的多版本链数据
struct mv_data {
  // 用于多版本的repair算法
  id_data mv_id;
  char *data;             // 存储row中的char* data
  list<id_data> read_req; // 使用list，读请求链表存储cid和query_id
  bool commited = false;
  bool deleted = false;
};

// item包含row和这一行的版本链
class itemid_t {
public:
  itemid_t(){};
  itemid_t(Data_type type, void *loc) {
    this->type = type;
    this->location = loc;
  };
  // 注意，这里用list会段错误
  my_list<mv_data> mv_list; // 使用my_list，版本链存储在item中，存储版本实体
  mutex mvlist_mutex; // 版本链锁

  // row中的结构,但一定需要先声明,在wl.cpp插入索引时加锁
  vector<pair<uint64_t, access_t>> reader; // 用于读锁
  set<pair<uint64_t, access_t>> waiter;    // 用于加锁算法的等待队列
  std::condition_variable cv;              // 用于row锁等待时的信号量

  Data_type type;
  void *location; // points to the table | page | row
  itemid_t *next;
  bool valid;
  void init_mvlist(); // 版本链，新版本放在链表头
  void init();
  bool operator==(const itemid_t &other) const;
  bool operator!=(const itemid_t &other) const;
  void operator=(const itemid_t &other);
};

int get_thdid_from_txnid(uint64_t txnid);

// key_to_part() is only for ycsb
uint64_t key_to_part(uint64_t key);
uint64_t get_part_id(void *addr);
// TODO can the following two functions be merged?
uint64_t merge_idx_key(uint64_t key_cnt, uint64_t *keys);
uint64_t merge_idx_key(uint64_t key1, uint64_t key2);
uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3);

// 这个是比较准确的
inline uint64_t get_clock() {
  timespec *tp = new timespec;
  clock_gettime(CLOCK_REALTIME, tp);
  uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
  return ret;
}

extern timespec *res;
inline uint64_t get_server_clock() {
#if defined(__i386__)
  uint64_t ret;
  __asm__ __volatile__("rdtsc" : "=A"(ret));
#elif defined(__x86_64__)
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  uint64_t ret = ((uint64_t)lo) | (((uint64_t)hi) << 32);
  ret = (uint64_t)((double)ret / CPU_FREQ);
#else
  timespec *tp = new timespec;
  clock_gettime(CLOCK_REALTIME, tp);
  uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
  return ret;
}

inline uint64_t get_sys_clock() { // 1s=1000000000,9个0
#ifndef NOGRAPHITE
  static volatile uint64_t fake_clock = 0;
  if (warmup_finish)
    return CarbonGetTime(); // in ns
  else {
    return ATOM_ADD_FETCH(fake_clock, 100);
  }
#else
#if TIME_ENABLE
  return get_server_clock();
#else
  return 0;
#endif
#endif
}
class myrand {
public:
  void init(uint64_t seed);
  uint64_t next();

private:
  uint64_t seed;
};

inline void set_affinity(uint64_t thd_id) {
  return;
  /*
  // TOOD. the following mapping only works for swarm
  // which has 4-socket, 10 physical core per socket,
  // 80 threads in total with hyper-threading
  uint64_t a = thd_id % 40;
  uint64_t processor_id = a / 10 + (a % 10) * 4;
  processor_id += (thd_id / 40) * 40;

  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(processor_id, &mask);
  sched_setaffinity(0, sizeof(cpu_set_t), &mask);
  */
}

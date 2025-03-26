#ifndef _MEM_ALLOC_H_
#define _MEM_ALLOC_H_

#include "global.h"
#include <map>
#include <stdint.h>
typedef uint32_t UInt32;

const int SizeNum = 4;
// 不同的Arena的块大小不同
const UInt32 BlockSizes[] = {32, 64, 256, 1024};

typedef struct free_block {
  int size;
  struct free_block *next;
} FreeBlock;

class Arena {
public:
  void init(int arena_id, int size);
  void *alloc();
  void free(void *ptr);

private:
  char *_buffer;       // 内存块的起始地址
  int _size_in_buffer; // 剩余的内存大小
  int _arena_id;
  int _block_size;  // 每个内存块的大小
  FreeBlock *_head; // 采用头插法
  char _pad[128 - sizeof(int) * 3 - sizeof(void *) * 2 - 8];
};

class mem_alloc {
public:
  void init(uint64_t part_cnt, uint64_t bytes_per_part);
  void register_thread(int thd_id);
  void unregister();
  void *alloc(uint64_t size, uint64_t part_id);
  void free(void *block, uint64_t size);
  int get_arena_id();

private:
  void init_thread_arena();
  int get_size_id(UInt32 size);

  // Arena_id用于标识不同线程的内存分配区域，Size_id用于识别不同大小的内存块的池
  Arena **_arenas;
  int _bucket_cnt;
  std::pair<pthread_t, int> *pid_arena; //                     max_arena_id;
  pthread_mutex_t map_lock;             // only used for pid_to_arena update
};

#endif

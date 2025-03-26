#include "para.h"

std::mutex print;
std::set<uint64_t> aborted_set;
mutex aborted_set_mutex;

unordered_map<uint64_t, uint64_t> coroutine;
mutex coroutine_mutex;
vector<uint64_t> coroutine_to_exec;
mutex coroutine_to_exec_mutex;

// 存储交互式事务的request队列
std::queue<std::pair<uint64_t, uint64_t>> req_queue;
std::mutex req_queue_mutex;
std::deque<std::pair<uint64_t, uint64_t>> req_deque;
std::mutex req_deque_mutex;

uint64_t g_client_cnt = 100;
mutex client_query_mutex;
unordered_map<uint64_t, pair<uint64_t, bool>> client_query = [] {
  unordered_map<uint64_t, pair<uint64_t, bool>> m;
  for (uint64_t i = 0; i < g_client_cnt; ++i) {
    m[i] = {i, true};
  }
  return m;
}();

uint64_t query_cnt_per_thd;
// uint64_t txn_id = 0;
uint64_t finished_cnt = 0;
uint64_t query_cnt = 100;
uint64_t thd_cnt = 49;
uint64_t abort_cnt = 0;
uint64_t repair_cnt = 0;
uint64_t io_cnt = 0;
double extra_time = 0;

map<uint64_t, set<uint64_t>> waiter;
map<uint64_t, set<uint64_t>> retired;
map<uint64_t, vector<uint64_t>> reader;

uint64_t g_cid = 0;
uint64_t g_be_dep_cnt = 13;
queue<uint64_t> to_commit_query;
mutex to_commit_query_mutex;
deque<uint64_t> to_commit_deque;
mutex to_commit_deque_mutex;
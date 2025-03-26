#pragma once

#include <deque>
#include <global.h>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <tpcc.h>
#include <unordered_map>
#include <vector>
#include <ycsb.h>

// 输出锁
extern mutex print;
// 存储中止的事务id
extern set<uint64_t> aborted_set;
extern mutex aborted_set_mutex;

struct TaskArgs {
#if WORKLOAD == TPCC
  tpcc_wl *tpccwl;
#elif WORKLOAD == YCSB
  ycsb_wl *ycsbwl;
#endif
};

// 存储交互式事务模型中的request队列，作为I/O队列
extern std::queue<pair<uint64_t, uint64_t>> req_queue;
extern mutex req_queue_mutex;
extern deque<pair<uint64_t, uint64_t>> req_deque;
extern mutex req_deque_mutex;

// 客户端数
extern uint64_t g_client_cnt;
// 分别代表客户端id、事务id、是否能获取事务
extern unordered_map<uint64_t, pair<uint64_t, bool>> client_query;
extern mutex client_query_mutex;

// 每个thread的query数
extern uint64_t query_cnt_per_thd;
// 下一个将要执行的事务id
// extern uint64_t txn_id;
// 已经完成的事务数
extern uint64_t finished_cnt;
// 每个客户端的事务数,乘上客户端数即为总事务数
extern uint64_t query_cnt;
extern uint64_t thd_cnt;
// 中止次数
extern uint64_t abort_cnt;
// 修复次数
extern uint64_t repair_cnt;
// I/O次数
extern uint64_t io_cnt;
// 除了事务执行时间之外的额外时间
extern double extra_time;

/****************************用于repair算法 */ ////////////////////////////////////////
// 用于全局分配cid
extern uint64_t g_cid;
// 临界的被依赖数
extern uint64_t g_be_dep_cnt;
//  队列和双向队列存储request全部完成且依赖事务数为0的可以提交的事务id
extern queue<uint64_t> to_commit_query;
extern mutex to_commit_query_mutex;
extern deque<uint64_t> to_commit_deque;
extern mutex to_commit_deque_mutex;
//////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////*用于ww，wd，bb等加锁算法*//////////////////////////////////////
// 用于加锁算法的row中的锁表的等待队列，适用于wait-die、wound-wait等
// 注意，应该按照request的key存储，而非row的key，因为tpcc中row的key可能重复
extern map<uint64_t, set<uint64_t>> waiter;
extern map<uint64_t, set<uint64_t>> retired;
// 用于支持读写锁，存储每个row的owner的写set，因为row中存储不了
extern map<uint64_t, vector<uint64_t>> reader;
// 用于nw、ww的协程，存储qid和rid
extern unordered_map<uint64_t, uint64_t> coroutine;
extern vector<uint64_t> coroutine_to_exec;
extern mutex coroutine_mutex;
extern mutex coroutine_to_exec_mutex;
////////////////////////////////////////////////////////////////////////////////////////////
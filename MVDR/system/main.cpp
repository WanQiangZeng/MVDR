#include "catalog.h"
#include "global.h"
#include "helper.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "query.h"
#include "row.h"
#include "table.h"
#include "test.h"
#include "tpcc.h"
#include "tpcc_const.h"
#include "tpcc_exec_request.h"
#include "tpcc_interactive_cc.h"
#include "wl.h"
#include "ycsb.h"
#include "ycsb_interactive_cc.h"
#include <para.h>
#include <thread>
#include <time.h>
#include <tpcc_helper.h>
#include <tpcc_query.h>
#include <ycsb_query.h>

using namespace std;

int main(int argc, char *argv[]) {

#ifndef NDEBUG
  uint64_t ts0, ts1;
  ts0 = get_sys_clock();
  sleep(1);
  ts1 = get_sys_clock();
  double ratio = ((double)(ts1 - ts0)) / 1000000000.0;
  if (ratio < 0.99 || ratio > 1.00) {
    fprintf(stderr,
            "FATAL ERROR: CPU freqency might be incorrectly configured: "
            "real_time/cpu_ts_time=%f\n",
            ratio);
    abort();
  }
#endif

  mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
  stats.init();
  printf("mem_allocator initialized!\n");

  workload *m_wl;
  switch (WORKLOAD) {
  case YCSB:
    m_wl = new ycsb_wl;
    break;
  case TPCC:
    m_wl = new tpcc_wl;
    break;
  case TEST:
    m_wl = new TestWorkload;
    ((TestWorkload *)m_wl)->tick();
    break;
  default:
    assert(false);
  }
  int64_t begin = get_clock();
  m_wl->init();
  int64_t end = get_clock();
  cout << "workload initialized, time: " << (end - begin) / 1000000000UL
       << endl;

  pthread_t p_thds[thd_cnt - 1];
  vector<thread> threads;
  query_queue = (Query_queue *)_mm_malloc(sizeof(Query_queue), 64);
  if (WORKLOAD != TEST)
    query_queue->init(m_wl);
  printf("query_queue initialized!\n");

  query_cnt_per_thd = query_queue->all_queries[0]->request_cnt;

#if WORKLOAD == TPCC
  tpcc_wl *tpccwl = static_cast<tpcc_wl *>(m_wl);
  TaskArgs args = {tpccwl};

  auto starttime = get_clock();
  for (uint64_t i = 0; i < thd_cnt - 1; i++) {
    pthread_create(&p_thds[i], NULL, run_tpcc_mvdr_interactive, (void *)&args);
  }
  for (uint64_t i = 0; i < thd_cnt - 1; i++) {
    pthread_join(p_thds[i], NULL);
  }
  auto endtime = get_clock();

  auto exec_time = endtime - starttime;
  double mean_latency = get_mean_latency();
  double mean_commit_wait_time = get_mean_commite_wait_time();
  double mean_lock_wait_time = get_mean_lock_wait_time();
  double mean_net_time = get_mean_net_time();
  double mean_abort_rate = get_mean_abort_rate();
  double mean_abort_time = get_mean_abort_time();
  double mean_useful_time = mean_latency - mean_lock_wait_time -
                            mean_commit_wait_time - mean_net_time -
                            mean_abort_time;

  cout << "finished_cnt: " << finished_cnt << endl;
  cout << "request_cnt per thread: " << query_cnt_per_thd << endl;
  cout << "total abort count: " << abort_cnt << endl;
  cout << "repair count: " << repair_cnt << endl;
  cout << "excuting timeapand: " << exec_time / 1000000UL << "ms" << endl;
  cout << "throughput: "
       << (double)finished_cnt * 1000 / (exec_time / 1000000UL) << " txn/s"
       << endl;
  cout << "mean latency: " << mean_latency << " ms" << endl;
  cout << "mean lock wait time: " << mean_lock_wait_time << " ms, "
       << mean_lock_wait_time / mean_latency * 100 << " % of total time"
       << endl;
  cout << "mean commit wait time: " << mean_commit_wait_time << " ms, "
       << mean_commit_wait_time / mean_latency * 100 << " % of total time"
       << endl;
  cout << "mean net i/o time: " << mean_net_time << " ms, "
       << mean_net_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean abort time: " << mean_abort_time << " ms, "
       << mean_abort_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean useful time: " << mean_useful_time << " ms, "
       << mean_useful_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean abort rate: " << mean_abort_rate * 100 << " %" << endl;

#elif WORKLOAD == YCSB
  ycsb_wl *ycsbwl = static_cast<ycsb_wl *>(m_wl);

  TaskArgs args = {ycsbwl};

  // for (uint64_t i = 0; i < thd_cnt - 1; i++) {
  //   pthread_create(&p_thds[i], NULL, run_ycsb_wound_wait_interactive,
  //                  (void *)&args);
  // }
  // for (uint64_t i = 0; i < thd_cnt - 1; i++) {
  //   pthread_join(p_thds[i], NULL);
  // }

  // cout << "warm up finished" << endl;
  // sleep(1);
  // cout << "start cc alg" << endl;

  auto starttime = get_clock();
  for (uint64_t i = 0; i < thd_cnt - 1; i++) {
    pthread_create(&p_thds[i], NULL, run_ycsb_mvdr_interactive, (void *)&args);
  }
  for (uint64_t i = 0; i < thd_cnt - 1; i++) {
    pthread_join(p_thds[i], NULL);
  }
  // for (auto i = 0; i < thd_cnt - 1; i++) {
  //   threads.push_back(thread(run_ycsb_repair_a_p_interactive, (void
  //   *)&args));
  // }
  // for (auto &t : threads) {
  //   t.join();
  // }
  auto endtime = get_clock();

  auto exec_time = endtime - starttime;
  double mean_latency = get_mean_latency();
  double mean_commit_wait_time = get_mean_commite_wait_time();
  double mean_lock_wait_time = get_mean_lock_wait_time();
  double mean_net_time = get_mean_net_time();
  double mean_abort_rate = get_mean_abort_rate();
  double mean_abort_time = get_mean_abort_time();
  double mean_useful_time = mean_latency - mean_lock_wait_time -
                            mean_commit_wait_time - mean_net_time -
                            mean_abort_time;

  cout << "finished_cnt: " << finished_cnt << endl;
  cout << "request_cnt per thread: " << query_cnt_per_thd << endl;
  cout << "total abort count: " << abort_cnt << endl;
  cout << "repair count: " << repair_cnt << endl;
  cout << "excuting timeapand: " << exec_time / 1000000UL << "ms" << endl;
  cout << "throughput: "
       << (double)finished_cnt * 1000 / (exec_time / 1000000UL) << " txn/s"
       << endl;
  cout << "mean latency: " << mean_latency << " ms" << endl;
  cout << "mean lock wait time: " << mean_lock_wait_time << " ms, "
       << mean_lock_wait_time / mean_latency * 100 << " % of total time"
       << endl;
  cout << "mean commit wait time: " << mean_commit_wait_time << " ms, "
       << mean_commit_wait_time / mean_latency * 100 << " % of total time"
       << endl;
  cout << "mean net i/o time: " << mean_net_time << " ms, "
       << mean_net_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean abort time: " << mean_abort_time << " ms, "
       << mean_abort_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean useful time: " << mean_useful_time << " ms, "
       << mean_useful_time / mean_latency * 100 << " % of total time" << endl;
  cout << "mean abort rate: " << mean_abort_rate * 100 << " %" << endl;

#elif WORKLOAD == TEST

#endif

  //   if (WORKLOAD != TEST) {
  //     printf("PASS! SimTime = %ld\n", endtime - starttime);
  //     if (STATS_ENABLE)
  //       stats.print();
  //   } else {
  //     ((TestWorkload *)m_wl)->summarize();
  //   }
  return 0;
}
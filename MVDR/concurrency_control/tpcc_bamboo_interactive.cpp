// #include "tpcc_interactive_cc.h"

// // 可能存在事务同时处于中止和提交队列中，因此当事务进入提交队列即视为已提交
// // 仍不稳定
// // 可以试着在中止和提交时对整个事务加锁
// #if WORKLOAD == TPCC
// void *run_tpcc_bamboo_interactive(void *args) {
//   TaskArgs *task_args = (TaskArgs *)args;
//   tpcc_wl *wl = (tpcc_wl *)task_args->tpccwl;
//   while (finished_cnt < query_cnt * g_client_cnt) {
//     usleep(50);

//     // 首先处理中止事务
//     uint64_t abort_query_id;
//     bool abort_is_empty;
//     {
//       lock_guard<mutex> lock(aborted_set_mutex);
//       abort_is_empty = aborted_set.empty();
//       if (!abort_is_empty) {
//         abort_query_id = *aborted_set.begin();
//         aborted_set.erase(aborted_set.begin());
//       }
//     }
//     if (!abort_is_empty) {
//       auto query_tmp =
//           &query_queue->all_queries[abort_query_id / query_cnt_per_thd]
//                ->queries[abort_query_id % query_cnt_per_thd];
//       // 处理级联中止
//       for (auto rid = 0; rid < query_tmp->request_cnt; rid++) {
//         auto request_tmp = &query_tmp->requests[rid];
//         row_t *row_tmp = NULL;
//         uint64_t key_tmp = request_tmp->key;
//         uint64_t part_id = wh_to_part(query_tmp->w_id);
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           {
//             unique_lock<mutex> lock(row_tmp->lock_entry.entry_mutex);
//             auto it_tmp = retired[key_tmp].find(abort_query_id);
//             if (it_tmp != retired[key_tmp].end()) {
//               it_tmp = retired[key_tmp].erase(it_tmp);
//               for (; it_tmp != retired[key_tmp].end(); it_tmp++) {
//                 auto next_id = *(it_tmp);
//                 auto next_query =
//                     &query_queue->all_queries[next_id / query_cnt_per_thd]
//                          ->queries[next_id % query_cnt_per_thd];
//                 {
//                   lock_guard<mutex> lock(next_query->query_mutex);
//                   next_query->rerun = true;
//                   if (next_query->able_to_commit &&
//                       !next_query->in_aborted_set && !next_query->commited) {
//                     // {
//                     //   lock_guard<mutex> lock(print);
//                     //   cout << "query: " << abort_query_id << " in
//                     //   aborted_set"
//                     //        << endl;
//                     // }
//                     next_query->in_aborted_set = true;
//                     lock_guard<mutex> lock(aborted_set_mutex);
//                     aborted_set.insert(next_id);
//                   }
//                 }
//               }
//             }
//           }
//         }
//       }
//       {
//         lock_guard<mutex> lock(query_tmp->query_mutex);
//         query_tmp->in_aborted_set = false;
//         query_tmp->rerun = false;
//         query_tmp->dep_cnt = 0;
//         query_tmp->able_to_commit = false;
//         query_tmp->commited = false;
//       }
//       {
//         lock_guard<mutex> lock(req_queue_mutex);
//         req_queue.push(make_pair(abort_query_id, 0));
//       }
//       // {
//       //   lock_guard<mutex> lock(print);
//       //   cout << "query: " << abort_query_id << " aborted" << endl;
//       // }
//       ATOM_ADD(abort_cnt, 1);
//     }

//     // 再处理提交事务
//     uint64_t commit_query_id;
//     bool commit_is_empty;
//     {
//       lock_guard<mutex> lock(to_commit_query_mutex);
//       commit_is_empty = to_commit_query.empty();
//       if (!commit_is_empty) {
//         commit_query_id = to_commit_query.front();
//         to_commit_query.pop();
//       }
//     }
//     if (!commit_is_empty) {
//       usleep(50);

//       auto query_tmp =
//           &query_queue->all_queries[commit_query_id / query_cnt_per_thd]
//                ->queries[commit_query_id % query_cnt_per_thd];
//       if (query_tmp->rerun) {
//         continue;
//       }
//       query_tmp->commited = true;
//       for (auto rid = 0; rid < query_tmp->request_cnt; rid++) {
//         auto request_tmp = &query_tmp->requests[rid];
//         row_t *row_tmp = NULL;
//         uint64_t key_tmp = request_tmp->key;
//         uint64_t part_id = wh_to_part(query_tmp->w_id);
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           {
//             unique_lock<mutex> lock(row_tmp->lock_entry.entry_mutex);
//             auto it_tmp = retired[key_tmp].find(commit_query_id);
//             if (it_tmp != retired[key_tmp].end()) {
//               it_tmp = retired[key_tmp].erase(it_tmp);
//             } else {
//               lock_guard<mutex> lock(print);
//               cout << "ERROR: during commiting query: " << commit_query_id
//                    << " not in retired set" << endl;
//               exit(0);
//             }
//             if (it_tmp == retired[key_tmp].begin() &&
//                 it_tmp != retired[key_tmp].end()) {
//               auto next_id = *(it_tmp);
//               auto next_query =
//                   &query_queue->all_queries[next_id / query_cnt_per_thd]
//                        ->queries[next_id % query_cnt_per_thd];
//               {
//                 lock_guard<mutex> lock(next_query->query_mutex);
//                 next_query->dep_cnt--;
//                 if (next_query->dep_cnt == 0 && !next_query->rerun &&
//                     !next_query->in_aborted_set && next_query->able_to_commit
//                     && !next_query->commited) {
//                   // {
//                   //   lock_guard<mutex> lock(print);
//                   //   cout << "query: " << commit_query_id << " in
//                   //   to_commit_query"
//                   //        << endl;
//                   // }
//                   lock_guard<mutex> lock(to_commit_query_mutex);
//                   to_commit_query.push(next_id);
//                 }
//               }
//             }
//           }
//         }
//       }
//       client_query[commit_query_id % g_client_cnt].first += g_client_cnt;
//       client_query[commit_query_id % g_client_cnt].second = true;
//       ATOM_ADD(finished_cnt, 1);
//       {
//         lock_guard<mutex> lock(print);
//         cout << "query: " << commit_query_id << " finished" << endl;
//       }
//     }

//     tpcc_query *query = NULL;
//     tpcc_request *request = NULL;
//     pair<uint64_t, uint64_t> id;
//     {
//       lock_guard<mutex> lock(req_queue_mutex);
//       if (req_queue.empty()) {
//         for (auto i = 0; i < g_client_cnt; i++) {
//           if (client_query[i].second &&
//               client_query[i].first < query_cnt * g_client_cnt) {
//             req_queue.push(make_pair(client_query[i].first, 0));
//             client_query[i].second = false;
//             // break;
//           }
//         }
//       }
//       if (!req_queue.empty()) {
//         id = req_queue.front();
//         req_queue.pop();
//       } else {
//         continue;
//       }
//     }

//     usleep(50);

//     query = &query_queue->all_queries[id.first / query_cnt_per_thd]
//                  ->queries[id.first % query_cnt_per_thd];
//     request = &query->requests[id.second];

//     row_t *row = NULL;
//     uint64_t key = request->key;
//     uint64_t part_id = wh_to_part(query->w_id);

//     if (request->rtype == RD || request->rtype == WR) {
//       get_row(request->index, request->key, part_id, row);
//     }

//     if (request->rtype == RD || request->rtype == WR) {
//       {
//         unique_lock<mutex> lock(row->lock_entry.entry_mutex);
//         auto write_it = retired[key].insert(id.first).first;
//         if (write_it != retired[key].begin()) {
//           lock_guard<mutex> lock(query->query_mutex);
//           query->dep_cnt++;
//         } else {
//           if (next(write_it) != retired[key].end()) {
//             auto next_id = *(next(write_it));
//             auto next_query =
//                 &query_queue->all_queries[next_id / query_cnt_per_thd]
//                      ->queries[next_id % query_cnt_per_thd];
//             {
//               lock_guard<mutex> lock(next_query->query_mutex);
//               next_query->dep_cnt++;
//             }
//           }
//         }
//         write_it++;
//         //
//         被中止的事务三种情况：1.在队列中等待执行 2.正在被其他线程执行 3.事务执行完成，等待提交
//         for (; write_it != retired[key].end(); write_it++) {
//           auto id_tmp = *write_it;
//           auto query_tmp = &query_queue->all_queries[id_tmp /
//           query_cnt_per_thd]
//                                 ->queries[id_tmp % query_cnt_per_thd];
//           {
//             lock_guard<mutex> lock(query_tmp->query_mutex);
//             query_tmp->rerun = true;
//             if (query_tmp->able_to_commit && !query_tmp->in_aborted_set &&
//                 !query_tmp->commited) {
//               // {
//               //   lock_guard<mutex> lock(print);
//               //   cout << "query: " << id_tmp << " in aborted_set" << endl;
//               // }
//               query_tmp->in_aborted_set = true;
//               lock_guard<mutex> lock(aborted_set_mutex);
//               aborted_set.insert(id_tmp);
//             }
//           }
//         }
//         exec_tpcc_request(id.first, id.second, row, wl);
//       }
//     } else {
//       exec_tpcc_request(id.first, id.second, row, wl);
//     }

//     usleep(50);

//     // 处理级联中止，将事务加入中止set
//     if (query->rerun) {
//       // {
//       //   lock_guard<mutex> lock(print);
//       //   cout << "query: " << id.first << " in aborted_set" << endl;
//       // }
//       {
//         lock_guard<mutex> lock(query->query_mutex);
//         if (!query->in_aborted_set && !query->commited) {
//           query->in_aborted_set = true;
//           lock_guard<mutex> lock(aborted_set_mutex);
//           aborted_set.insert(id.first);
//         }
//       }
//       continue;
//     }

//     if (id.second < query->request_cnt - 1) {
//       lock_guard<mutex> lock(req_queue_mutex);
//       req_queue.push(make_pair(id.first, id.second + 1));
//     } else if (id.second == query->request_cnt - 1) {
//       lock_guard<mutex> lock(query->query_mutex);
//       query->able_to_commit = true;
//       if (query->dep_cnt == 0 && !query->rerun) {
//         // {
//         //   lock_guard<mutex> lock(print);
//         //   cout << "query: " << id.first << " in to_commit_query" << endl;
//         // }
//         lock_guard<mutex> lock(to_commit_query_mutex);
//         to_commit_query.push(id.first);
//       }
//     }
//   }
//   pthread_exit(NULL);
// }
// #endif
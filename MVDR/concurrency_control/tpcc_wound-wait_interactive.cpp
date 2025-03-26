// #include "tpcc_interactive_cc.h"

// // 释放资源包括两个步骤：更改owner，释放锁

// #if WORKLOAD == TPCC

// void *run_tpcc_wound_wait_interactive(void *args) {
//   TaskArgs *task_args = (TaskArgs *)args;
//   tpcc_wl *wl = (tpcc_wl *)task_args->tpccwl;

//   while (finished_cnt < query_cnt * g_client_cnt) {
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
//             break;
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

//     usleep(100);

//     query = &query_queue->all_queries[id.first / query_cnt_per_thd]
//                  ->queries[id.first -
//                            id.first / query_cnt_per_thd * query_cnt_per_thd];
//     request = &query->requests[id.second];

//     row_t *row = NULL;
//     itemid_t *item = NULL;
//     uint64_t part_id = wh_to_part(query->w_id);

//     if (request->rtype == RD || request->rtype == WR) {
//       get_itemid(request->index, request->key, part_id, item);
//       // get_row(request->index, request->key, part_id, row);
//       row = static_cast<row_t *>(item->location);
//     }

//     if (request->rtype == RD || request->rtype == WR) {
//       {
//         unique_lock<mutex> lock(row->lock_entry.entry_mutex);
//         if (row->lock_entry.owner == -1) {
//           row->lock_entry.owner = id.first;
//           exec_tpcc_request(id.first, id.second, row, wl);
//         }
//         // wait
//         else if (row->lock_entry.owner < id.first) {
//           item->waiter.insert(id.first);
//           lock.unlock();
//           while (row->lock_entry.owner != id.first && !query->rerun) {
//             PAUSE
//           }
//           if (!query->rerun) {
//             unique_lock<mutex> lock(row->lock_entry.entry_mutex);
//             exec_tpcc_request(id.first, id.second, row, wl);
//           }
//         }
//         //
//         wound,首先更改owner并抢占锁，2种情况：被终止的事务request处于队列中等待，或者正在被其他线程执行
//         //
//         无论是那种情况，等被终止的事务的在外面的request执行完之后，再释放资源并置于队列重新执行
//         else if (row->lock_entry.owner > id.first) {
//           auto abort_id = row->lock_entry.owner;
//           auto abort_query =
//               &query_queue->all_queries[abort_id / query_cnt_per_thd]
//                    ->queries[abort_id % query_cnt_per_thd];
//           row->lock_entry.owner = id.first;
//           {
//             lock_guard<mutex> lock(abort_query->query_mutex);
//             abort_query->rerun = true;
//             abort_query->num_abort++;
//           }
//           exec_tpcc_request(id.first, id.second, row, wl);
//         }
//       }
//     } else {
//       exec_tpcc_request(id.first, id.second, row, wl);
//     }

//     // usleep(50);

//     // 若本线程执行的request被其他事务中止，将其释放资源并重新加入队列
//     if (query->rerun) {
//       for (auto rid = 0; rid < query->request_cnt; rid++) {
//         auto request_tmp = &query->requests[rid];
//         row_t *row_tmp = NULL;
//         itemid_t *item_tmp = NULL;
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_itemid(request_tmp->index, request_tmp->key, part_id,
//           item_tmp);
//           // get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           row_tmp = static_cast<row_t *>(item_tmp->location);
//           {
//             std::unique_lock<std::mutex>
//             lock(row_tmp->lock_entry.entry_mutex); if
//             (row_tmp->lock_entry.owner == id.first) {
//               if (!item_tmp->waiter.empty()) {
//                 row_tmp->lock_entry.owner = *item_tmp->waiter.begin();
//                 item_tmp->waiter.erase(item_tmp->waiter.begin());
//               } else {
//                 row_tmp->lock_entry.owner = -1;
//               }
//             } else {
//               item_tmp->waiter.erase(id.first);
//             }
//           }
//         }
//       }
//       {
//         std::lock_guard<mutex> lock(query->query_mutex);
//         query->rerun = false;
//       }
//       {
//         lock_guard<mutex> lock(req_queue_mutex);
//         req_queue.push(make_pair(id.first, 0));
//       }
//       ATOM_ADD(abort_cnt, 1);
//       continue;
//     }

//     if (id.second < query->request_cnt - 1) {
//       lock_guard<mutex> lock(req_queue_mutex);
//       req_queue.push(make_pair(id.first, id.second + 1));
//     } else if (id.second == query->request_cnt - 1) {
//       for (auto rid = 0; rid < query->request_cnt; rid++) {
//         auto request_tmp = &query->requests[rid];
//         row_t *row_tmp = NULL;
//         itemid_t *item_tmp = NULL;
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_itemid(request_tmp->index, request_tmp->key, part_id,
//           item_tmp);
//           // get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           row_tmp = static_cast<row_t *>(item_tmp->location);
//           {
//             std::unique_lock<std::mutex>
//             lock(row_tmp->lock_entry.entry_mutex); if
//             (!item_tmp->waiter.empty()) {
//               row_tmp->lock_entry.owner = *item_tmp->waiter.begin();
//               item_tmp->waiter.erase(item_tmp->waiter.begin());
//             } else {
//               row_tmp->lock_entry.owner = -1;
//             }
//           }
//         }
//       }
//       client_query[id.first % g_client_cnt].first += g_client_cnt;
//       client_query[id.first % g_client_cnt].second = true;
//       ATOM_ADD(finished_cnt, 1);
//       {
//         lock_guard<mutex> lock(print);
//         cout << "query: " << id.first << " finished" << endl;
//       }
//     }
//   }
//   pthread_exit(NULL);
// }
// #endif
// #include "ycsb_interactive_cc.h"

// #if WORKLOAD == YCSB
// void *run_ycsb_wait_die_interactive(void *args) {
//   TaskArgs *task_args = (TaskArgs *)args;
//   ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;

//   while (finished_cnt < query_cnt * g_client_cnt) {
//     ycsb_query *query = NULL;
//     ycsb_request *request = NULL;
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

//     usleep(50);

//     query = &query_queue->all_queries[id.first / query_cnt_per_thd]
//                  ->queries[id.first % query_cnt_per_thd];
//     request = &query->requests[id.second];

//     row_t *row = NULL;
//     get_row(wl, request->key, row);
//     assert(row != nullptr);

//     {
//       std::unique_lock<std::mutex> lock(row->lock_entry.entry_mutex);
//       if (row->lock_entry.owner == -1) {
//         row->lock_entry.owner = id.first;
//         if (request->rtype == WR) {
//           write(request, row);
//         } else if (request->rtype == RD) {
//           read(request, row);
//         }
//       }
//       // wait
//       else if (row->lock_entry.owner > id.first) {
//         waiter[request->key].insert(id.first);
//         lock.unlock();
//         while (row->lock_entry.owner != id.first) {
//           PAUSE
//         }
//         std::unique_lock<std::mutex> lock(row->lock_entry.entry_mutex);
//         if (request->rtype == WR) {
//           write(request, row);
//         } else if (request->rtype == RD) {
//           read(request, row);
//         }
//       }
//       // die
//       else if (row->lock_entry.owner < id.first) {
//         lock.unlock();
//         {
//           std::lock_guard<mutex> lock(query->query_mutex);
//           query->num_abort++;
//           query->rerun = true;
//         }
//       }
//     }

//     usleep(50);

//     if (query->rerun) {
//       for (auto rid = 0; rid < query->request_cnt; rid++) {
//         auto request_tmp = &query->requests[rid];
//         row_t *row_tmp = NULL;
//         get_row(wl, request_tmp->key, row_tmp);
//         {
//           std::lock_guard<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
//           if (row_tmp->lock_entry.owner == id.first) {
//             if (!waiter[request_tmp->key].empty()) {
//               row_tmp->lock_entry.owner =
//               *prev(waiter[request_tmp->key].end());
//               waiter[request_tmp->key].erase(
//                   prev(waiter[request_tmp->key].end()));
//             } else {
//               row_tmp->lock_entry.owner = -1;
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
//         row_t *row_tmp;
//         get_row(wl, request_tmp->key, row_tmp);
//         {
//           std::lock_guard<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
//           if (!waiter[request_tmp->key].empty()) {
//             row_tmp->lock_entry.owner =
//             *prev(waiter[request_tmp->key].end());
//             waiter[request_tmp->key].erase(
//                 prev(waiter[request_tmp->key].end()));
//           } else {
//             row_tmp->lock_entry.owner = -1;
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
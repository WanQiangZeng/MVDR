// #include "ycsb_interactive_cc.h"

// #if WORKLOAD == YCSB
// void *run_ycsb_plor_interactive(void *args) {
//   TaskArgs *task_args = (TaskArgs *)args;
//   ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;
//   while (finished_cnt < query_cnt * g_client_cnt) {
//     ycsb_query *query = NULL;
//     ycsb_request *request = NULL;
//     pair<uint64_t, uint64_t> id;
//     bool is_empty = true;

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

//     if (id.second == 0 && query->starttime == 0) {
//       query->starttime = get_clock();
//     }

//     row_t *row;
//     itemid_t *item;
//     get_itemid(wl, request->key, item);
//     row = static_cast<row_t *>(item->location);
//     uint64_t datasize = row->get_tuple_size();

//     // 先对所有读写操作加s锁
//     {
//       unique_lock<mutex> lock(row->lock_entry.entry_mutex);
//       item->reader.push_back(make_pair(id.first, request->rtype));
//       if (request->rtype == WR) {
//         request->data_local = (char *)_mm_malloc(datasize, 64);
//         memcpy(request->data_local, row->get_data(), datasize);
//         for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
//           *(uint64_t *)(&request->data_local[fid * 10]) = 0;
//         }
//         query->write_set.push_back(id.second);
//       } else if (request->rtype == RD) {
//         for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
//           __attribute__((unused)) uint64_t fval =
//               *(uint64_t *)(&row->data[fid * 10]);
//         }
//       }
//     }

//     // usleep(50);

//     if (id.second < query->request_cnt - 1) {
//       {
//         lock_guard<mutex> lock(req_queue_mutex);
//         req_queue.push(make_pair(id.first, id.second + 1));
//       }
//     }
//     // 验证，采用wd避免死锁
//     else if (id.second == query->request_cnt - 1) {
//       // 获取写锁
//       auto to_get_lock = query->write_set.size();
//       for (auto rid : query->write_set) {
//         auto v_request = &query->requests[rid];
//         row_t *v_row = NULL;
//         itemid_t *v_item;
//         get_itemid(wl, v_request->key, v_item);
//         v_row = static_cast<row_t *>(v_item->location);
//         {
//           unique_lock<mutex> lock(v_row->lock_entry.entry_mutex);
//           if (v_item->reader.begin()->first == id.first) {
//             v_row->lock_entry.owner = id.first;
//           } else {
//             auto v_it = v_item->reader.begin();
//             for (; v_it->first > id.first; v_it++) {
//             }
//             // wait
//             if (v_it->first == id.first) {
//               lock.unlock();
//               while (v_row->lock_entry.owner != id.first) {
//                 PAUSE;
//               }
//             }
//             // die
//             else if (v_it->first < id.first) {
//               lock.unlock();
//               query->rerun = true;
//               query->num_abort++;
//               break;
//             }
//           }
//         }
//       }

//       query->write_set.clear();
//       for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
//         auto v_request = &query->requests[rid];
//         row_t *v_row = NULL;
//         itemid_t *v_item;
//         get_itemid(wl, v_request->key, v_item);
//         v_row = static_cast<row_t *>(v_item->location);
//         {
//           unique_lock<mutex> lock(v_row->lock_entry.entry_mutex);
//           if (v_row->lock_entry.owner == id.first) {
//             v_row->lock_entry.owner = -1;
//           }
//           auto v_it = find(v_item->reader.begin(), v_item->reader.end(),
//                            make_pair(id.first, v_request->rtype));
//           if (v_it != v_item->reader.end()) {
//             v_it = v_item->reader.erase(v_it);
//             if (v_it != v_item->reader.end() &&
//                 v_it == v_item->reader.begin()) {
//               v_row->lock_entry.owner = v_it->first;
//             }
//           }
//         }
//       }
//       if (query->rerun) {
//         {
//           std::lock_guard<mutex> lock(query->query_mutex);
//           query->rerun = false;
//         }
//         {
//           lock_guard<mutex> lock(req_queue_mutex);
//           req_queue.push(make_pair(id.first, 0));
//         }
//         ATOM_ADD(abort_cnt, 1);
//       } else if (!query->rerun) {
//         for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
//           auto v_request = &query->requests[rid];
//           if (v_request->rtype == WR) {
//             row_t *v_row = NULL;
//             get_row(wl, v_request->key, v_row);
//             {
//               lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
//               memcpy(v_row->get_data(), v_request->data_local,
//               v_row->datasize);
//             }
//           }
//         }
//         query->endtime = get_clock();
//         query->timespan = query->endtime - query->starttime;
//         client_query[id.first % g_client_cnt].first += g_client_cnt;
//         client_query[id.first % g_client_cnt].second = true;
//         ATOM_ADD(finished_cnt, 1);
//         {
//           lock_guard<mutex> lock(print);
//           cout << "query: " << id.first << " finished" << endl;
//         }
//       }
//     }
//   }
//   pthread_exit(NULL);
// }
// #endif
// #include "tpcc_interactive_cc.h"

// // 加入客户端的改变：1.while的终止条件 2.req_queue的加入 3.提交阶段
// #if WORKLOAD == TPCC
// void *run_tpcc_no_wait_interactive(void *args) {
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
//                  ->queries[id.first -
//                            id.first / query_cnt_per_thd * query_cnt_per_thd];
//     request = &query->requests[id.second];

//     row_t *row = NULL;
//     uint64_t part_id = wh_to_part(query->w_id);

//     if (request->rtype == RD || request->rtype == WR) {
//       get_row(request->index, request->key, part_id, row);
//     }

//     if (request->rtype == RD || request->rtype == WR) {
//       if (row->lock_entry.owner == -1) {
//         lock_guard<mutex> lock(row->lock_entry.entry_mutex);
//         row->lock_entry.owner = id.first;
//         exec_tpcc_request(id.first, id.second, row, wl);
//       } else {
//         for (auto rid = 0; rid < query->request_cnt; rid++) {
//           auto request_tmp = &query->requests[rid];
//           row_t *row_tmp;
//           if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//             get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//             lock_guard<mutex> lock(row_tmp->lock_entry.entry_mutex);
//             if (row_tmp->lock_entry.owner == id.first) {
//               row_tmp->lock_entry.owner = -1;
//             }
//           }
//         }
//         ATOM_ADD(abort_cnt, 1);
//         {
//           lock_guard<mutex> lock(query->query_mutex);
//           query->num_abort++;
//         }
//         {
//           lock_guard<mutex> lock(req_queue_mutex);
//           req_queue.push(make_pair(id.first, 0));
//         }
//         continue;
//       }
//     } else {
//       exec_tpcc_request(id.first, id.second, row, wl);
//     }

//     usleep(50);

//     if (id.second < query->request_cnt - 1) {
//       lock_guard<mutex> lock(req_queue_mutex);
//       req_queue.push(make_pair(id.first, id.second + 1));
//     } else if (id.second == query->request_cnt - 1) {
//       for (auto rid = 0; rid < query->request_cnt; rid++) {
//         auto request_tmp = &query->requests[rid];
//         row_t *row_tmp;
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           lock_guard<mutex> lock(row_tmp->lock_entry.entry_mutex);
//           row_tmp->lock_entry.owner = -1;
//           pthread_mutex_unlock(&row_tmp->lock_entry.row_mutex);
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
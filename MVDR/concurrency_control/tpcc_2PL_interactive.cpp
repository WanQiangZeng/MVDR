// #include "tpcc_interactive_cc.h"

// #if WORKLOAD == TPCC
// void *run_tpcc_2PL_interactive(void *args) {
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
//       pthread_mutex_lock(&row->lock_entry.row_mutex);
//       exec_tpcc_request(id.first, id.second, row, wl);
//     } else {
//       exec_tpcc_request(id.first, id.second, row, wl);
//     }

//     if (id.second < query->request_cnt - 1) {
//       lock_guard<mutex> lock(req_queue_mutex);
//       req_queue.push(make_pair(id.first, id.second + 1));
//     } else if (id.second == query->request_cnt - 1) {
//       for (auto rid = 0; rid < query->request_cnt; rid++) {
//         auto request_tmp = &query->requests[rid];
//         row_t *row_tmp = NULL;
//         if (request_tmp->rtype == RD || request_tmp->rtype == WR) {
//           get_row(request_tmp->index, request_tmp->key, part_id, row_tmp);
//           pthread_mutex_unlock(&row_tmp->lock_entry.row_mutex);
//         }
//       }
//       client_query[id.first % g_client_cnt].first += g_client_cnt;
//       client_query[id.first % g_client_cnt].second = true;
//       ATOM_ADD(finished_cnt, 1);
//       // {
//       //   lock_guard<mutex> lock(print);
//       //   cout << "query: " << id.first << " finished" << endl;
//       // }
//     }
//   }
//   pthread_exit(NULL);
// }
// #endif
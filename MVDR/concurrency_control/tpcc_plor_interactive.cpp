#include "tpcc_interactive_cc.h"

// wait时加入了超时机制
#if WORKLOAD == TPCC
void *run_tpcc_plor_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  tpcc_wl *wl = (tpcc_wl *)task_args->tpccwl;
  while (finished_cnt < query_cnt * g_client_cnt) {
    tpcc_query *query = NULL;
    tpcc_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    bool is_empty = true;

    {
      lock_guard<mutex> lock(req_queue_mutex);
      if (req_queue.empty()) {
        for (uint64_t i = 0; i < g_client_cnt; i++) {
          if (client_query[i].second &&
              client_query[i].first < query_cnt * g_client_cnt) {
            req_queue.push(make_pair(client_query[i].first, 0));
            client_query[i].second = false;
            // break;
          }
        }
      }
      if (!req_queue.empty()) {
        id = req_queue.front();
        req_queue.pop();
      } else {
        continue;
      }
    }

    usleep(50);

    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first % query_cnt_per_thd];
    request = &query->requests[id.second];

    ++query->net_times;

    if (id.second == 0 && query->starttime == 0) {
      lock_guard<mutex> lock(query->query_mutex);
      query->starttime = get_clock();
    }

    row_t *row = NULL;
    itemid_t *item = NULL;
    uint64_t part_id = wh_to_part(query->w_id);
    uint64_t datasize = 0;

    if (request->rtype == RD || request->rtype == WR) {
      get_itemid(request->index, request->key, part_id, item);
      row = static_cast<row_t *>(item->location);
      datasize = row->get_tuple_size();
    }

    // 先对所有读写操作加s锁
    {
      if (request->rtype == WR) {
        unique_lock<mutex> lock(row->lock_entry.entry_mutex);
        item->reader.push_back(make_pair(id.first, request->rtype));

        auto row_tmp = row;
        exec_tpcc_request(id.first, id.second, row_tmp, wl);
        request->data_local = (char *)_mm_malloc(sizeof(char) * datasize, 64);
        memcpy(request->data_local, row_tmp->data, row->datasize);

        lock.unlock();
        query->write_set.push_back(id.second);
      } else if (request->rtype == RD) {
        unique_lock<mutex> lock(row->lock_entry.entry_mutex);
        item->reader.push_back(make_pair(id.first, request->rtype));

        exec_tpcc_request(id.first, id.second, row, wl);
      } else if (request->rtype == IS) {
        exec_tpcc_request(id.first, id.second, row, wl);
      }
    }

    // usleep(50);
    query->net_times++;

    if (id.second < query->request_cnt - 1) {
      {
        lock_guard<mutex> lock(req_queue_mutex);
        req_queue.push(make_pair(id.first, id.second + 1));
      }
    }
    // 验证，采用wd避免死锁
    else if (id.second == query->request_cnt - 1) {
      // 获取写锁
      for (auto rid : query->write_set) {
        auto v_request = &query->requests[rid];
        row_t *v_row = NULL;
        itemid_t *v_item;
        get_itemid(v_request->index, v_request->key, part_id, v_item);
        v_row = static_cast<row_t *>(v_item->location);
        {
          unique_lock<mutex> lock(v_row->lock_entry.entry_mutex);
          if (v_item->reader.begin()->first == id.first) {
            v_row->lock_entry.owner = id.first;
          } else {
            auto v_it = v_item->reader.begin();
            for (; v_it->first > id.first; v_it++) {
            }
            // wait+超时机制
            if (v_it->first == id.first) {
              query->lock_wait_starttime = get_clock();
              auto timeout =
                  std::chrono::milliseconds(10); // 设置超时时间为10毫秒
              // auto timeout = std::chrono::microseconds(3000);
              // v_item->cv.wait(
              //     lock, [&]() { return v_row->lock_entry.owner == id.first;
              //     });
              bool acquired = v_item->cv.wait_for(lock, timeout, [&]() {
                return v_row->lock_entry.owner == id.first;
              });
              if (!acquired) {
                // 超时处理逻辑
                query->rerun = true;
                query->num_abort++;
                query->lock_wait_starttime = 0;
                break;
              }
              if (query->lock_wait_starttime != 0) {
                query->lock_wait_endtime = get_clock();
                query->lock_wait_time += (double)(query->lock_wait_endtime -
                                                  query->lock_wait_starttime) /
                                         1000000UL;
              }
            }
            // die
            else if (v_it->first < id.first) {
              lock.unlock();
              query->rerun = true;
              query->num_abort++;
              break;
            }
          }
        }
      }

      query->write_set.clear();
      for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
        auto v_request = &query->requests[rid];
        row_t *v_row = NULL;
        itemid_t *v_item;
        if (v_request->rtype == RD || v_request->rtype == WR) {
          get_itemid(v_request->index, v_request->key, part_id, v_item);
          v_row = static_cast<row_t *>(v_item->location);
          {
            unique_lock<mutex> lock(v_row->lock_entry.entry_mutex);
            if (v_row->lock_entry.owner == id.first) {
              v_row->lock_entry.owner = -1;
            }
            auto v_it = v_item->reader.begin();
            for (; v_it != v_item->reader.end() && v_it->first != id.first;
                 v_it++)
              ;
            // auto v_it = find(v_item->reader.begin(), v_item->reader.end(),
            //                  make_pair(id.first, v_request->rtype));
            if (v_it != v_item->reader.end()) {
              v_it = v_item->reader.erase(v_it);
              if (v_it != v_item->reader.end() &&
                  v_it == v_item->reader.begin() && v_it->second == WR) {
                v_row->lock_entry.owner = v_it->first;
                v_item->cv.notify_all();
              }
            }
          }
        }
      }

      if (query->rerun) {
        {
          std::lock_guard<mutex> lock(query->query_mutex);
          query->rerun = false;
          query->lock_wait_starttime = 0;
          query->lock_wait_endtime = 0;
        }
        {
          lock_guard<mutex> lock(req_queue_mutex);
          req_queue.push(make_pair(id.first, 0));
        }
        ATOM_ADD(abort_cnt, 1);
      } else if (!query->rerun) {
        for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
          auto v_request = &query->requests[rid];
          if (v_request->rtype == WR) {
            row_t *v_row = NULL;
            get_row(v_request->index, v_request->key, part_id, v_row);
            {
              lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
              memcpy(v_row->get_data(), v_request->data_local, v_row->datasize);
            }
          }
        }
        {
          lock_guard<mutex> lock(query->query_mutex);
          query->endtime = get_clock();
          query->timespan =
              (double)(query->endtime - query->starttime) / 1000000UL;
        }
        client_query[id.first % g_client_cnt].first += g_client_cnt;
        client_query[id.first % g_client_cnt].second = true;
        ATOM_ADD(finished_cnt, 1);
        {
          lock_guard<mutex> lock(print);
          cout << "query: " << id.first << " finished" << endl;
        }
      }
    }
  }
  pthread_exit(NULL);
}
#endif
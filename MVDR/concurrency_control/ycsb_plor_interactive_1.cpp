#include "ycsb_interactive_cc.h"

// wait时加入了超时机制
#if WORKLOAD == YCSB
void *run_ycsb_plor_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;
  while (finished_cnt < query_cnt * g_client_cnt) {
    ycsb_query *query = NULL;
    ycsb_request *request = NULL;
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

    row_t *row;
    itemid_t *item;
    get_itemid(wl, request->key, item);
    row = static_cast<row_t *>(item->location);
    uint64_t datasize = row->get_tuple_size();

    // 先对所有读写操作加s锁
    {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);
      item->reader.push_back(make_pair(id.first, request->rtype));
      if (request->rtype == WR) {
        request->data_local = (char *)_mm_malloc(datasize, 64);
        memcpy(request->data_local, row->get_data(), datasize);
        lock.unlock();
        for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
          *(uint64_t *)(&request->data_local[fid * 10]) = 0;
        }
        query->write_set.push_back(id.second);
      } else if (request->rtype == RD) {
        lock.unlock();
        for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
          __attribute__((unused)) uint64_t fval =
              *(uint64_t *)(&row->data[fid * 10]);
        }
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
        get_itemid(wl, v_request->key, v_item);
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
              // 争用低时超时时长设低一点，争用高时设高一点
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
        get_itemid(wl, v_request->key, v_item);
        v_row = static_cast<row_t *>(v_item->location);
        {
          unique_lock<mutex> lock(v_row->lock_entry.entry_mutex);
          if (v_row->lock_entry.owner == id.first) {
            v_row->lock_entry.owner = -1;
          }
          auto v_it = find(v_item->reader.begin(), v_item->reader.end(),
                           make_pair(id.first, v_request->rtype));
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

      if (query->rerun) {
        // {
        //   lock_guard<mutex> lock(print);
        //   cout << "query: " << id.first << " rerun" << endl;
        // }
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
            get_row(wl, v_request->key, v_row);
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
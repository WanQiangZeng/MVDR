#include "tpcc_interactive_cc.h"

// // MVTO + cid分配，尽可能减少中止
// request执行包括三个步骤：分配cid，写操作，读操作
// 1. 分配cid包括两个操作：更改mv_data的cid，更改query的cid
// 2.写操作包括两个步骤：对本地的版本写入，将本地版本插入合适的位置
// 3.读操作包括两个步骤：找到合适读取的版本读取，将本地的read_id插入到版本的读集中
// 与MVTO不同的是，写操作只关注未提交的版本
// //  记录事务的依赖项和被依赖项，当依赖项为空时可以提交
// //  使用多版本作为读写缓存，类似wound-wait进行优先级控制以避免循环依赖
// //  WR、RW只需要修复读操作，WW需要修复所有操作，可以在线程中直接修复
// //  当id->second ==
// //  query->request_cnt-1且事务依赖项为空时可以提交，代表有资格提交

// 已实现2+3的一部分
#if WORKLOAD == TPCC
void *run_tpcc_mvdr_no_valid_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  tpcc_wl *wl = (tpcc_wl *)task_args->tpccwl;
  while (finished_cnt < query_cnt * g_client_cnt) {
    // 若提交队列不为空，取出一个事务提交,此事务所有request都已经执行完且无依赖项
    uint64_t query_id;
    bool queue_empty;
    {
      lock_guard<mutex> lock(to_commit_deque_mutex);
      queue_empty = to_commit_deque.empty();
      if (!queue_empty) {
        query_id = *to_commit_deque.begin();
        to_commit_deque.erase(to_commit_deque.begin());
      }
    }
    if (!queue_empty) {
      auto query_tmp = &query_queue->all_queries[query_id / query_cnt_per_thd]
                            ->queries[query_id % query_cnt_per_thd];
      query_tmp->commited = true;
      query_tmp->commit_wait_endtime = get_clock();
      for (auto rid = 0; rid < query_tmp->request_cnt; rid++) {
        auto request_tmp = &query_tmp->requests[rid];
        if (request_tmp->rtype == WR) {
          itemid_t *item_tmp = nullptr;
          row_t *row_tmp = NULL;
          uint64_t part_id = wh_to_part(query_tmp->w_id);
          get_itemid(request_tmp->index, request_tmp->key, part_id, item_tmp);
          row_tmp = static_cast<row_t *>(item_tmp->location);
          {
            lock_guard<mutex> lock(item_tmp->mvlist_mutex);
            auto it_tmp = item_tmp->mv_list.head;
            for (; it_tmp->data.mv_id.qid != query_id; it_tmp = it_tmp->next) {
            }
            // 将版本改为已提交
            it_tmp->data.commited = true;
            // 将数据写入到row中
            char *new_data = it_tmp->data.data;
            memcpy(row_tmp->data, new_data, row_tmp->datasize);
            if (it_tmp->next != NULL) {
              // 下一个节点的dep_cnt-1
              auto next_mvnode = &it_tmp->next->data;
              auto next_query_id = next_mvnode->mv_id.qid;
              auto next_query =
                  &query_queue->all_queries[next_query_id / query_cnt_per_thd]
                       ->queries[next_query_id % query_cnt_per_thd];
              {
                lock_guard<mutex> lock(next_query->query_mutex);
                next_query->dep_cnt--;
                // 若next_query的依赖数为0且request全部执行完且未提交，加入提交队列
                if (next_query->dep_cnt == 0 && next_query->able_to_commit &&
                    !next_query->commited &&
                    next_query->next_rid == next_query->request_cnt) {
                  next_query->in_commited_query = true;
                  {
                    lock_guard<mutex> lock(to_commit_deque_mutex);
                    if (next_query->be_dep_cnt > g_be_dep_cnt) {
                      to_commit_deque.push_front(next_query_id);
                    } else {
                      to_commit_deque.push_back(next_query_id);
                    }
                  }
                }
              }
            }
          }
        }
      }
      {
        lock_guard<mutex> lock(query_tmp->query_mutex);
        query_tmp->endtime = get_clock();
        query_tmp->timespan =
            (double)(query_tmp->endtime - query_tmp->starttime) / 1000000UL;
      }
      client_query[query_id % g_client_cnt].first += g_client_cnt;
      client_query[query_id % g_client_cnt].second = true;
      ATOM_ADD(finished_cnt, 1);
      {
        lock_guard<mutex> lock(print);
        cout << "query: " << query_id << " finished" << endl;
      }
    }

    tpcc_query *query = NULL;
    tpcc_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    // 从客户端中获取一个request
    {
      lock_guard<mutex> lock(req_deque_mutex);
      if (req_deque.empty()) {
        for (auto i = 0; i < g_client_cnt; i++) {
          if (client_query[i].second &&
              client_query[i].first < query_cnt * g_client_cnt) {
            auto id_tmp = client_query[i].first;
            auto query_tmp =
                &query_queue->all_queries[id_tmp / query_cnt_per_thd]
                     ->queries[id_tmp % query_cnt_per_thd];
            req_deque.push_back(make_pair(client_query[i].first, 0));
            query_tmp->next_rid++;
            client_query[i].second = false;
            // break;
          }
        }
      }
      if (!req_deque.empty()) {
        id = *req_deque.begin();
        req_deque.erase(req_deque.begin());
      } else {
        continue;
      }
    }

    usleep(50);

    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first % query_cnt_per_thd];
    request = &query->requests[id.second];

    query->rerun = false;
    ++query->net_times;

    if (id.second == 0 && query->starttime == 0) {
      // lock_guard<mutex> lock(query->query_mutex);
      query->starttime = get_clock();
    }

    itemid_t *item = nullptr;
    row_t *row = nullptr;
    uint64_t part_id = wh_to_part(query->w_id);
    uint64_t data_size = 0;

    if (request->rtype == RD || request->rtype == WR) {
      get_itemid(request->index, request->key, part_id, item);
      get_row(request->index, request->key, part_id, row);
      data_size = row->datasize;
    }

    if (request->rtype == RD || request->rtype == WR) {
      { // 对整个版本链加锁
        lock_guard<mutex> lock(item->mvlist_mutex);
        if (item->mv_list.empty()) {
          item->init_mvlist();
        }
        // 最新的未提交节点或null
        auto commited_it = item->mv_list.head;
        for (; commited_it != NULL && commited_it->data.commited;
             commited_it = commited_it->next) {
        }
        // 版本链的最新版本，即尾部版本
        auto mv_tail = &item->mv_list.back();

        // 问题出在写入操作的data复制上
        if (request->rtype == WR) {
          mv_data mv_newnode;
          mv_newnode.mv_id = {query->cid, id.first, id.second};
          mv_newnode.commited = false;
          // 可能创建的新版本的data复制
          mv_newnode.data = (char *)_mm_malloc(data_size, 64);

          // 接着执行写request，committed_id指向最新的未提交节点或null，last_mvnode指向插入点的前一个版本
          //    write_it指向插入点的后一个节点或null
          auto write_it = commited_it;
          for (; write_it != NULL; write_it = write_it->next) {
            if (write_it->data.mv_id.qid >= mv_newnode.mv_id.qid) {
              break;
            }
          }
          // last_mvnode指向插入点的前一个版本，检测RW冲突
          mv_data *last_mvnode;
          if (write_it == NULL) {
            last_mvnode = &item->mv_list.back();
          } else {
            last_mvnode = &write_it->prev->data;
          }

          auto row_mv = row;
          memcpy(row_mv->data, last_mvnode->data, data_size);
          exec_tpcc_request(id.first, id.second, row_mv, wl);
          memcpy(mv_newnode.data, row_mv->data, data_size);

          if (write_it == NULL ||
              write_it->data.mv_id.qid > mv_newnode.mv_id.qid) {
            if (!last_mvnode->commited) {
              auto id_tmp = last_mvnode->mv_id.qid;
              auto query_tmp =
                  &query_queue->all_queries[id_tmp / query_cnt_per_thd]
                       ->queries[id_tmp % query_cnt_per_thd];
              {
                lock_guard<mutex> lock(query->query_mutex);
                query->dep_cnt++;
              }
              {
                lock_guard<mutex> lock(query_tmp->query_mutex);
                query_tmp->be_dep_cnt++;
              }
            }

            // for (auto it = last_mvnode->read_req.begin();
            //      it != last_mvnode->read_req.end(); it++) {
            //   // 修复RW冲突
            //   if ((*it).qid > mv_newnode.mv_id.qid) {

            //     // repair RW conflict
            //     repair_deque((*it).qid, (*it).rid);

            //     it = last_mvnode->read_req.erase(it);
            //     continue;
            //   }
            // }

            // 只需要推入到版本链末尾
            if (write_it == NULL) {
              item->mv_list.push_back(mv_newnode);
            }
            // 需要修复WW冲突
            else if (write_it != NULL) {
              if (last_mvnode->commited) {
                auto next_query_id = write_it->data.mv_id.qid;
                auto next_query =
                    &query_queue->all_queries[next_query_id / query_cnt_per_thd]
                         ->queries[next_query_id % query_cnt_per_thd];
                {
                  lock_guard<mutex> lock(next_query->query_mutex);
                  next_query->dep_cnt++;
                }
                {
                  lock_guard<mutex> lock(query->query_mutex);
                  query->be_dep_cnt++;
                }
              }
              item->mv_list.insert_before(write_it, mv_newnode);
              // for (; write_it != NULL; write_it = write_it->next) {

              //   // repair WW conflict
              //   repair_deque(write_it->data.mv_id.qid,
              //                write_it->data.mv_id.rid);
              //   // ATOM_ADD(repair_cnt, 1);

              //   for (auto it = write_it->data.read_req.begin();
              //        it != write_it->data.read_req.end(); it++) {

              //     // repair WR conflict
              //     repair_deque((*it).qid, (*it).rid);

              //     it = write_it->data.read_req.erase(it);
              //     continue;
              //   }
              // }
            }
          }
        }

        // 若request为读
        else if (request->rtype == RD) {
          id_data read_id = {query->cid, id.first, id.second};

          // 其次，执行读request,寻找合适的读取版本，修复WR冲突
          // read_it指向合适的读取版本的下一个节点
          auto read_it = commited_it;
          for (; read_it != NULL; read_it = read_it->next) {
            if (read_it->data.mv_id.qid > read_id.qid) {
              break;
            }
          }
          // 适合读取的版本，修复WR冲突
          mv_data *read_mvnode;
          if (read_it == NULL) {
            read_mvnode = &item->mv_list.back();
          } else {
            read_mvnode = &read_it->prev->data;
          }

          auto row_mv = row;
          memcpy(row_mv->data, read_mvnode->data, data_size);
          exec_tpcc_request(id.first, id.second, row_mv, wl);

          // 若不在其中则设rerun为false
          bool in_read_it = false;
          for (auto id_tmp : read_mvnode->read_req) {
            if (id_tmp.qid == id.first) {
              in_read_it = true;
              break;
            }
          }
          if (!in_read_it) {
            read_mvnode->read_req.push_back(read_id);
          }
        }
      }
    } else {
      exec_tpcc_request(id.first, id.second, row, wl);
    }

    if (query->rerun) {
      for (auto rid = query->next_rid; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        if (request_tmp->rtype == WR) {
          itemid_t *item_tmp = nullptr;
          row_t *row_tmp = NULL;
          uint64_t part_id = wh_to_part(query->w_id);
          get_itemid(request_tmp->index, request_tmp->key, part_id, item_tmp);
          row_tmp = static_cast<row_t *>(item_tmp->location);
          {
            lock_guard<mutex> lock(item_tmp->mvlist_mutex);
            auto it_tmp = item_tmp->mv_list.head;
            for (; it_tmp != NULL && it_tmp->data.mv_id.qid != id.first;
                 it_tmp = it_tmp->next) {
            }
            if (it_tmp != NULL) {
              it_tmp = it_tmp->next;
              for (; it_tmp != NULL; it_tmp = it_tmp->next) {

                // repair WW conflict
                repair_deque(it_tmp->data.mv_id.qid, it_tmp->data.mv_id.rid);
                // repair(it_tmp->data.mv_id.qid, it_tmp->data.mv_id.rid + 1);
                // query->net_times++;
                // ATOM_ADD(repair_cnt, 1);

                for (auto it = it_tmp->data.read_req.begin();
                     it != it_tmp->data.read_req.end(); it++) {

                  // repair WR conflict
                  repair_deque((*it).qid, (*it).rid);

                  it = it_tmp->data.read_req.erase(it);
                  continue;
                }
              }
            } else {
              break;
            }
          }
        }
      }
    }

    query->net_times++;
    // usleep(50);

    {
      unique_lock<mutex> lock(query->query_mutex);
      if (id.second < query->request_cnt - 1) {
        insert_request_prio(id.first);
        query->next_rid++;
        // query->rerun = false;
      } else if (id.second == query->request_cnt - 1) {
        if (query->next_rid == query->request_cnt) {
          // 代表request都已经执行完，有资格提交
          query->able_to_commit = true;
          lock.unlock();

          for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
            auto request_tmp = &query->requests[rid];
            if (request_tmp->rtype == WR) {
              itemid_t *item_tmp = nullptr;
              get_itemid(request_tmp->index, request_tmp->key, part_id,
                         item_tmp);
              {
                lock_guard<mutex> lock(item_tmp->mvlist_mutex);
                auto it_tmp = item_tmp->mv_list.head;
                for (; it_tmp->data.mv_id.qid != id.first;
                     it_tmp = it_tmp->next) {
                }
                auto last_mvnode = &it_tmp->data;
                for (auto it = last_mvnode->read_req.begin();
                     it != last_mvnode->read_req.end(); it++) {
                  // 修复RW冲突
                  if ((*it).qid > id.first) {

                    // repair RW conflict
                    repair_deque((*it).qid, (*it).rid);

                    it = last_mvnode->read_req.erase(it);
                    continue;
                  }
                }
                auto write_it = it_tmp->next;
                for (; write_it != NULL; write_it = write_it->next) {

                  // repair WW conflict
                  repair_deque(write_it->data.mv_id.qid,
                               write_it->data.mv_id.rid);

                  for (auto it = write_it->data.read_req.begin();
                       it != write_it->data.read_req.end(); it++) {

                    // repair WR conflict
                    repair_deque((*it).qid, (*it).rid);

                    it = write_it->data.read_req.erase(it);
                    continue;
                  }
                }
              }
            }
          }

          unique_lock<mutex> lock(query->query_mutex);
          query->commit_wait_starttime = get_clock();
          // 无依赖项则可以提交，加入提交队列
          if (query->dep_cnt == 0 && query->next_rid == query->request_cnt) {
            query->in_commited_query = true;
            {
              lock_guard<mutex> lock(to_commit_deque_mutex);
              if (query->be_dep_cnt > g_be_dep_cnt) {
                to_commit_deque.push_front(id.first);
              } else {
                to_commit_deque.push_back(id.first);
              }
            }
          } else if (query->dep_cnt < 0) {
            cout << "ERROR: query: " << id.first
                 << " has dep_cnt: " << query->dep_cnt << endl;
            exit(0);
          }
        } else if (query->next_rid < query->request_cnt) {
          insert_request_prio(id.first);
          query->next_rid++;
          // query->rerun = false;
        }
      }
    }
  }
  pthread_exit(NULL);
}
#endif
// 一、加锁时可能的上下文切换：尽可能的使用lock_guard()等方式加锁，显式加锁如lock（），unlock（）可能会因I/O、时间片耗尽等原因进行上下文切换，造成锁定丢失，其他线程可能在交界区修改的过程中修改交界区，造成段错误
// 上下文切换是操作系统调度程序在多任务环境中切换CPU执行的线程或进程的过程。上下文切换可能在以下情况下发生：

// 1.
// **时间片用尽**：操作系统使用时间片轮转调度算法，每个线程或进程在其时间片用尽时会被切换出去。
// 2.
// **I/O操作**：线程或进程在执行I/O操作时会被阻塞，操作系统会切换到其他可运行的线程或进程。
// 3. **系统调用**：线程或进程执行系统调用时，可能会导致上下文切换。
// 4.
// **优先级调度**：操作系统可能会根据线程或进程的优先级进行调度，高优先级的线程或进程可能会抢占低优先级的线程或进程。
// 5.
// **多核处理器**：在多核处理器系统中，操作系统可能会将线程或进程调度到不同的CPU核心上运行。

// 在你的代码中，[`queue_mutex.lock()`]
// "Go to definition") 和
// [`queue_mutex.unlock()`]

// 1. **时间片用尽**：如果线程在执行
// [`req_queue.pop()`]
// 2. **I/O操作**：如果在
// [`req_queue.pop()`]
// "Go to definition")
// 之后执行了某些I/O操作（例如打印日志），可能会导致线程被阻塞，操作系统会切换到其他线程。
// 3. **系统调用**：如果在
// [`req_queue.pop()`]
// "Go to definition")
// 之后执行了某些系统调用（例如内存分配），可能会导致上下文切换。

// 为了避免在
// [`queue_mutex.lock()`]
// [`queue_mutex.unlock()`]之间发生上下文切换
// 可以使用[`lock_guard`]
// 避免显式使用[`lock()`]和
// `unlock()`。这样可以确保在持有锁的情况下对交界区进行访问，避免数据竞争和段错误的发生。
// 示例如下：
// 原代码：可能因上下文切换导致锁丢失，造成段错误
pair<uint64_t, uint64_t> id;
if (!req_queue.empty()) {
  queue_mutex.lock();
  id = req_queue.front();
  req_queue.pop();
  queue_mutex.unlock();
} else {
  continue;
}
// 修改后的代码：
pair<uint64_t, uint64_t> id;
{
  lock_guard<mutex> lock(queue_mutex);
  if (!req_queue.empty()) {
    id = req_queue.front();
    req_queue.pop();
  } else {
    continue;
  }
}

// 二、char*
// data的处理：尽量按照下面方式处理row中的char*类型变量data，否则会出现错误
row_t *row = (row_t *)location;
char *row_data = row->get_data();
uint64_t data_size = row->get_tuple_size();
initial_node.data = (char *)_mm_malloc(data_size, 64);
memcpy(initial_node.data, row_data, data_size);

// 三、对一些无关紧要东西的处理尽量不要放在前面，可能造成线程间的信息差和时间差，如ATOM_ADD(abort_cnt,1)

// 四、在tpcc的事务的request执行过程中，本程序采用row_tmp替换其中的data再执行，
// 在data的memcpy中一定要注意datasize的大小，不同表的row大小会存在差异，不控制会出现数组越界，段错误，损毁数据
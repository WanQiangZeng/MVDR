#ifndef _MCS_SPINLOCK
#define _MCS_SPINLOCK

#include "amd64.h"
#include <atomic>

class mcslock {

public:
  mcslock() : tail(nullptr){};

  struct mcs_node {
    volatile bool locked;
    uint8_t pad0[64 - sizeof(bool)];
    volatile mcs_node *volatile next;
    uint8_t pad1[64 - sizeof(mcs_node *)];
    mcs_node() : locked(true), next(nullptr) {}
  };

  void acquire(mcs_node *me) {
    auto prior_node = tail.exchange(me, std::memory_order_acquire);
    if (prior_node != nullptr) {
      memory_barrier();
      me->locked = true;
      prior_node->next = me;
      memory_barrier();
      while (me->locked) {
        memory_barrier();
        nop_pause();
      }
      assert(!me->locked);
    }
  };

  void release(mcs_node *me) {
    if (me->next == nullptr) {
      mcs_node *expected = me;
      if (tail.compare_exchange_strong(expected, nullptr,
                                       std::memory_order_release,
                                       std::memory_order_relaxed)) {
        return;
      }
      while (me->next == nullptr) {
      };
    }
    memory_barrier();
    me->next->locked = false;
    me->next = nullptr;
  };

private:
  std::atomic<mcs_node *> tail;
};

#endif

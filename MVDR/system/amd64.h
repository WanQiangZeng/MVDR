#pragma once
#ifndef _AMD64_H_
#define _AMD64_H_

#include <stdint.h>
// 定义AMD64架构下的多线程处理优化
#define ALWAYS_INLINE __attribute__((always_inline))

inline ALWAYS_INLINE void nop_pause() { __asm volatile("pause" : :); }

inline void memory_barrier() { asm volatile("mfence" : : : "memory"); }

#endif /* _AMD64_H_ */

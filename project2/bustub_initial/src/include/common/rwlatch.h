//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>  // NOLINT
#include <shared_mutex>

#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
 public:
  /**
   * Acquire a write latch.
   */
  void WLock() {
    mutex_.lock();
    is_locked_ = true;
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    mutex_.unlock();
    is_locked_ = false;
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    mutex_.lock_shared();
    is_locked_ = true;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    mutex_.unlock_shared();
    is_locked_ = false;
  }

  auto IsLocked() const -> bool { return is_locked_; }

 private:
  std::shared_mutex mutex_;
  bool is_locked_ = false;
};

}  // namespace bustub

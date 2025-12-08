//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page.h
//
// Identification: src/include/storage/page/page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <iostream>

#include "common/config.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */
class Page {
  // There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
  friend class BufferPoolManagerInstance;

 public:
  /** Constructor. Zeros out the page data. */
  Page() { ResetMemory(); }

  /** Default destructor. */
  ~Page() = default;

  /** @return the actual data contained within this page */
  inline auto GetData() -> char * { return data_; }

  /** @return the page id of this page */
  inline auto GetPageId() -> page_id_t { return page_id_; }

  /** @return the pin count of this page */
  inline auto GetPinCount() -> int { return pin_count_; }

  /** @return true if the page in memory has been modified from the page on disk, false otherwise */
  inline auto IsDirty() -> bool { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }

  /**
   * LSN:
   * A log sequence number (LSN) represents the offset, in bytes, of a log record from the beginning of a database log
   * file. It identifies the location within the log file of a specific log file record. LSNs are used by many
   * components throughout the DB2®product to maintain database consistency and integrity. The play a critical role in,
   * among other things, commit and rollback operations, crash recovery, and the synchronization of database operations
   * in partitioned database environments. The rate of growth of LSNs in the log files is directly associated with
   * database activity. That is, as transactions take place, and entries are written to log files, LSNs continually
   * increase. When there is more activity in the database, LSNs grow more quickly.
   */
  /** @return the page LSN. */
  inline auto GetLSN() -> lsn_t { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /** Sets the page LSN. */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

  /**
   * Checks if the page is unlocked.
   * @return true if the page is unlocked, false otherwise.
   */
  inline auto IsUnlocked() const -> bool { return !rwlatch_.IsLocked(); }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 4;

 private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, BUSTUB_PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char data_[BUSTUB_PAGE_SIZE]{};
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page.
   * the "pin count" of a page refers to the number of clients (such as queries or transactions)
   * currently using or "pinning" that page in memory.
   */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
  /** Dirty Page
   * A "dirty page" in the context of a database system refers to a page (a unit of data storage)
   * that has been modified in memory but has not yet been written back to disk.
   */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
};

}  // namespace bustub

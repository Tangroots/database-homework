//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once
#include <pthread.h>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "container/hash/hash_table.h"
namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size) {
    global_depth_ = 0;
    bucket_size_ = bucket_size;
    num_buckets_ = 1;
    dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
    pthread_mutex_init(&dir_exclusive_mutex_, nullptr);
    pthread_rwlock_init(&dir_read_write_lock_, nullptr);
    pthread_cond_init(&dir_selective_cond_, nullptr);
    pthread_mutex_init(&dir_selective_mutex_, nullptr);
  }

  ~ExtendibleHashTable() override {
    pthread_mutex_destroy(&dir_exclusive_mutex_);
    pthread_rwlock_destroy(&dir_read_write_lock_);
    pthread_cond_destroy(&dir_selective_cond_);
    pthread_mutex_destroy(&dir_selective_mutex_);
  }

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  // Read Lock
  void DirRLock() { pthread_rwlock_rdlock(&dir_read_write_lock_); }

  void DirRUnlock() { pthread_rwlock_unlock(&dir_read_write_lock_); }

  // Exclusive Lock
  void DirWLock() { pthread_mutex_lock(&dir_exclusive_mutex_); }

  void DirWUnlock() { pthread_mutex_unlock(&dir_exclusive_mutex_); }

  // Selective Lock
  void DirSLock() {
    pthread_mutex_lock(&dir_selective_mutex_);
    while (dir_selective_lock_active_) {
      pthread_cond_wait(&dir_selective_cond_, &dir_selective_mutex_);
    }
    dir_selective_lock_active_ = true;
    pthread_rwlock_rdlock(&dir_read_write_lock_);
    pthread_mutex_unlock(&dir_selective_mutex_);
  }

  void DirSUnlock() {
    pthread_rwlock_unlock(&dir_read_write_lock_);
    pthread_mutex_lock(&dir_selective_mutex_);
    dir_selective_lock_active_ = false;
    pthread_cond_broadcast(&dir_selective_cond_);
    pthread_mutex_unlock(&dir_selective_mutex_);
  }

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override {
    size_t index = IndexOf(key);
    dir_[index]->RLock();
    int current_mask = dir_[index]->GetLocalMask();
    std::shared_ptr<Bucket> current_ptr = dir_[index];
    if (current_ptr->GetDepth() == 0) {
      if (current_ptr->Find(key, value)) {
        current_ptr->RUnlock();
        return true;
      }
      current_ptr->RUnlock();
      return false;
    }
    while ((current_mask & std::hash<K>()(key)) != current_ptr->GetCommonBits()) {
      auto next_ptr = current_ptr->GetNextBucket();
      if (!next_ptr) {
        current_ptr->RUnlock();  // Make sure to unlock before returning
        return false;            // No next bucket, key not found
      }
      current_ptr->GetNextBucket()->RLock();
      current_mask = dir_[index]->GetLocalMask();
      current_ptr->RUnlock();
      current_ptr = current_ptr->GetNextBucket();
    }
    if (current_ptr->Find(key, value)) {
      current_ptr->RUnlock();
      return true;
    }
    current_ptr->RUnlock();
    return false;
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override {
    while (true) {
      DirSLock();
      std::shared_ptr<Bucket> current_ptr = dir_[IndexOf(key)];
      current_ptr->SLock();
      if (!(current_ptr->IsFull())) {  // bucket is not full
        DirSUnlock();
        if (current_ptr->Insert(key, value)) {
          current_ptr->SUnlock();
          return;
        }
      } else {  // bucket is full
        if (current_ptr->GetDepth() == global_depth_) {
          /**Double the directory*/
          int original_size = dir_.size();
          // Double the size of the directory and update pointers
          for (int i = 0; i < original_size; ++i) {
            dir_.push_back(dir_[i]);
          }
          global_depth_++;  // Increment global depth after fully updating the directory
          /**Spilt the Bucket*/
          num_buckets_ = num_buckets_ + 1;
          int original_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto original_bucket_underline = IndexOf(key) & original_mask;  // 原始桶的下标
          dir_[IndexOf(key)]->IncrementDepth();
          // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
          int new_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto new_bucket_underline =
              original_bucket_underline + (1 << (dir_[IndexOf(key)]->GetDepth() - 1));  // 新桶的下标
          dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
          /** reexamine the original bucket
           * Be cautious with iterator invalidation. When you remove an item from the list
           * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
           * it invalidates the iterator. This can lead to undefined behavior.
           */
          // get the original bucket's items
          std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
          for (auto original_bucket_iterator = original_bucket_list.begin();
               original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
            if ((IndexOf(original_bucket_iterator->first) & new_mask) == new_bucket_underline) {
              dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
              dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
            }
          }
          // update the next pointer
          dir_[new_bucket_underline]->SetNext(dir_[original_bucket_underline]->GetNextBucket());
          dir_[original_bucket_underline]->SetNext(dir_[new_bucket_underline]);
          // update the common bits
          dir_[original_bucket_underline]->UpdateCommonBits(original_bucket_underline);
          dir_[new_bucket_underline]->UpdateCommonBits(new_bucket_underline);
          // rehash the directory
          for (size_t i = 0; i < dir_.size(); i++) {
            if ((i & new_mask) == new_bucket_underline) {
              dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
            }
          }
          current_ptr->SUnlock();
          DirSUnlock();
        }
        if (current_ptr->GetDepth() < global_depth_) {
          /**Spilt the Bucket*/
          num_buckets_ = num_buckets_ + 1;
          int original_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto original_bucket_underline = IndexOf(key) & original_mask;  // 原始桶的下标
          dir_[IndexOf(key)]->IncrementDepth();
          // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
          int new_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto new_bucket_underline =
              original_bucket_underline + (1 << (dir_[IndexOf(key)]->GetDepth() - 1));  // 新桶的下标
          dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
          /** reexamine the original bucket
           * Be cautious with iterator invalidation. When you remove an item from the list
           * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
           * it invalidates the iterator. This can lead to undefined behavior.
           */
          // get the original bucket's items
          std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
          for (auto original_bucket_iterator = original_bucket_list.begin();
               original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
            if ((IndexOf(original_bucket_iterator->first) & new_mask) == new_bucket_underline) {
              dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
              dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
            }
          }
          // update the next pointer
          dir_[new_bucket_underline]->SetNext(dir_[original_bucket_underline]->GetNextBucket());
          dir_[original_bucket_underline]->SetNext(dir_[new_bucket_underline]);
          // update the common bits
          dir_[original_bucket_underline]->UpdateCommonBits(original_bucket_underline);
          dir_[new_bucket_underline]->UpdateCommonBits(new_bucket_underline);
          // rehash the directory
          for (size_t i = 0; i < dir_.size(); i++) {
            if ((i & new_mask) == new_bucket_underline) {
              dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
            }
          }
          current_ptr->SUnlock();
          DirSUnlock();
        }
      }
    }
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override {
    size_t index = IndexOf(key);
    dir_[index]->WLock();
    int current_mask = dir_[index]->GetLocalMask();
    std::shared_ptr<Bucket> current_ptr = dir_[index];
    while ((current_mask & std::hash<K>()(key)) != current_ptr->GetCommonBits()) {
      auto next_ptr = current_ptr->GetNextBucket();
      if (!next_ptr) {
        current_ptr->WUnlock();  // Make sure to unlock before returning
        return false;            // No next bucket, key not found
      }
      next_ptr->WLock();
      current_mask = dir_[index]->GetLocalMask();
      current_ptr->WUnlock();
      current_ptr = current_ptr->GetNextBucket();
    }
    if (current_ptr->Remove(key)) {
      current_ptr->WUnlock();
      return true;
    }
    current_ptr->WUnlock();
    return false;
  };

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0) {
      size_ = size;
      depth_ = depth;
      next_bucket_ = nullptr;
      pthread_mutex_init(&bucket_exclusive_mutex_, nullptr);
      pthread_rwlock_init(&bucket_read_write_lock_, nullptr);
      pthread_cond_init(&bucket_selective_cond_, nullptr);
      pthread_mutex_init(&bucket_selective_mutex_, nullptr);
    }

    ~Bucket() {
      pthread_mutex_destroy(&bucket_exclusive_mutex_);
      pthread_rwlock_destroy(&bucket_read_write_lock_);
      pthread_cond_destroy(&bucket_selective_cond_);
      pthread_mutex_destroy(&bucket_selective_mutex_);
    }

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    // Read Lock
    void RLock() { pthread_rwlock_rdlock(&bucket_read_write_lock_); }

    void RUnlock() { pthread_rwlock_unlock(&bucket_read_write_lock_); }

    // Exclusive Lock
    void WLock() { pthread_mutex_lock(&bucket_exclusive_mutex_); }

    void WUnlock() { pthread_mutex_unlock(&bucket_exclusive_mutex_); }

    // Selective Lock
    void SLock() {
      pthread_mutex_lock(&bucket_selective_mutex_);
      while (bucket_selective_lock_active_) {
        pthread_cond_wait(&bucket_selective_cond_, &bucket_selective_mutex_);
      }
      bucket_selective_lock_active_ = true;
      pthread_rwlock_rdlock(&bucket_read_write_lock_);
      pthread_mutex_unlock(&bucket_selective_mutex_);
    }

    void SUnlock() {
      pthread_rwlock_unlock(&bucket_read_write_lock_);
      pthread_mutex_lock(&bucket_selective_mutex_);
      bucket_selective_lock_active_ = false;
      pthread_cond_broadcast(&bucket_selective_cond_);
      pthread_mutex_unlock(&bucket_selective_mutex_);
    }

    /**Get the status of the Selective lock*/
    auto GetSelectiveLockStatus() -> bool { return bucket_selective_lock_active_; }

    /**Get the local mask of the Bucket*/
    auto GetLocalMask() -> int { return (1 << depth_) - 1; }

    /**Get the common bits*/
    auto GetCommonBits() -> std::optional<int> {
      if (depth_ == 0) {
        return std::nullopt;  // Indicates no value
      }
      return common_bits_;
    }

    /**Update the common bits*/
    void UpdateCommonBits(int new_common_bits) { common_bits_ = new_common_bits; }

    /**Get the pointer of the next Bucket*/
    auto GetNextBucket() -> std::shared_ptr<Bucket> { return next_bucket_; }

    /**Set the pointer of the next Bucket*/
    void SetNext(std::shared_ptr<Bucket> next_) { next_bucket_ = next_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool {
      if (list_.empty()) {
        return false;
      }
      for (auto bucket_find_iterator = list_.begin(); bucket_find_iterator != list_.end(); bucket_find_iterator++) {
        if (bucket_find_iterator->first == key) {
          value = bucket_find_iterator->second;
          return true;
        }
      }
      return false;
    };

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool {
      if (list_.empty()) {
        return false;
      }
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if (it->first == key) {
          list_.erase(it);
          return true;
        }
      }
      return false;
    };

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool {
      if (IsFull()) {
        return false;
      }
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if (it->first == key) {
          it->second = value;
          return true;
        }
      }
      list_.push_back(std::make_pair(key, value));
      return true;
    };

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;
    int depth_;
    int common_bits_ = -1;  // -1 indicates no value
    std::list<std::pair<K, V>> list_;
    std::shared_ptr<Bucket> next_bucket_;  // next bucket pointer
    pthread_mutex_t bucket_exclusive_mutex_;
    pthread_rwlock_t bucket_read_write_lock_;
    pthread_cond_t bucket_selective_cond_;
    pthread_mutex_t bucket_selective_mutex_;
    bool bucket_selective_lock_active_ = false;
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table
  pthread_mutex_t dir_exclusive_mutex_;       // The exclusive lock for the directory
  pthread_rwlock_t dir_read_write_lock_;      // The read-write lock for the directory
  pthread_cond_t dir_selective_cond_;         // The conditional variable for the selective lock
  pthread_mutex_t dir_selective_mutex_;       // The mutex for the selective lock
  bool dir_selective_lock_active_ = false;    // The status of the selective lock

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub

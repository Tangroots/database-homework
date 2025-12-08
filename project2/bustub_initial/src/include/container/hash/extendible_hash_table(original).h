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

#include <list>
#include <memory>
#include <mutex>  // NOLINT
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
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

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
      for (typename std::list<std::pair<K, V>>::iterator bucket_find_iterator = list_.begin();
           bucket_find_iterator != list_.end(); bucket_find_iterator++) {
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
      for (typename std::list<std::pair<K, V>>::iterator bucket_remove_iterator = list_.begin();
           bucket_remove_iterator != list_.end(); bucket_remove_iterator++) {
        if (bucket_remove_iterator->first == key) {
          list_.erase(bucket_remove_iterator);
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
      for (typename std::list<std::pair<K, V>>::iterator bucket_insert_iterator = list_.begin();
           bucket_insert_iterator != list_.end(); bucket_insert_iterator++) {
        if (bucket_insert_iterator->first == key) {
          bucket_insert_iterator->second = value;
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
    std::list<std::pair<K, V>> list_;
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  /**
   * explicit:
   * The keyword explicit in C++ is used in the context of constructors to prevent implicit conversions or
   * copy-initialization. When you declare a constructor with the explicit keyword, it means that the constructor cannot
   * be used for implicit conversions and copy-initialization. This is particularly important for type safety and to
   * avoid unintended conversions that might lead to bugs.
   */
  explicit ExtendibleHashTable(size_t bucket_size) {
    global_depth_ = 0;
    bucket_size_ = bucket_size;
    num_buckets_ = 1;
    dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
  };

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

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   * Find(K, V) : For the given key K, check to see whether it exists in the hash table.
   * If it does, then store the pointer to its corresponding value in V and return true.
   * If the key does not exist, then return false
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override {
    size_t index = IndexOf(key);
    return dir_[index]->Find(key, value);
  };

  /**
   *
   * TODO(P1): Add implementation
   * Insert(K, V) : Insert the key/value pair into the hash table.
   * If the key K already exists, overwrite its value with the new value V and return true.
   * If the key/value pair couldn't be inserted into the bucket
   * (because the bucket is full and the key is not updating an existing pair),
   * do the following steps before retrying:
   * If the local depth of the bucket is equal to the global depth,
   * increment the global depth and double the size of the directory.
   * Increment the local depth of the bucket.
   * Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   * Some implementations split the bucket if the bucket becomes full right after an insertion.
   * But for this project please detect if the bucket is overflowing and perform the split before an insertion.
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
      if (dir_[IndexOf(key)]->Insert(key, value)) {
        return;
      }
      if (dir_[IndexOf(key)]->GetDepth() == global_depth_) {
        // increment the global depth and double the size of the directory.
        global_depth_++;
        // set the new directory's increased pointer to the old directory pointer
        for (size_t i = 0; i < dir_.size(); i++) {
          dir_.push_back(dir_[i]);
        }
        num_buckets_ = num_buckets_ + 1;
        dir_[IndexOf(key)]->IncrementDepth();
        // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
        auto original_bucket_underline = IndexOf(key);                          // 原始桶的下标
        auto new_bucket_underline = IndexOf(key) | (1 << (global_depth_ - 1));  // 新桶的下标
        dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
        /** reexamine the original bucket
         * Be cautious with iterator invalidation. When you remove an item from the list
         * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
         * it invalidates the iterator. This can lead to undefined behavior.
         */
        // get the original bucket's items
        std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
        for (typename std::list<std::pair<K, V>>::iterator original_bucket_iterator = original_bucket_list.begin();
             original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
          if (IndexOf(original_bucket_iterator->first) == new_bucket_underline) {
            dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
            dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
          }
        };
      }
      if (dir_[IndexOf(key)]->GetDepth() < global_depth_) {
        dir_[IndexOf(key)]->IncrementDepth();
        // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
        auto original_bucket_underline = IndexOf(key);                          // 原始桶的下标
        auto new_bucket_underline = IndexOf(key) | (1 << (global_depth_ - 1));  // 新桶的下标
        dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
        /** reexamine the original bucket
         * Be cautious with iterator invalidation. When you remove an item from the list
         * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
         * it invalidates the iterator. This can lead to undefined behavior.
         */
        // get the original bucket's items
        std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
        for (typename std::list<std::pair<K, V>>::iterator original_bucket_iterator = original_bucket_list.begin();
             original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
          if (IndexOf(original_bucket_iterator->first) == new_bucket_underline) {
            dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
            dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
          }
        };
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
    if (dir_[IndexOf(key)]->Remove(key)) {
      return true;
    }
    return false;
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

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

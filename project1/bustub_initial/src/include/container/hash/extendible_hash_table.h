#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <vector>

namespace bustub {

template <typename K, typename V>
class ExtendibleHashTable {
 public:
  // 桶类定义
  class Bucket {
   public:
    explicit Bucket(size_t depth, size_t max_size = 4);
    auto Find(const K &key, V &value) -> bool;
    auto Remove(const K &key) -> bool;
    auto Insert(const K &key, const V &value) -> bool;
    auto IsFull() const -> bool;
    auto IsEmpty() const -> bool;
    auto GetDepth() const -> size_t;
    void IncreaseDepth();
    auto GetItems() const -> const std::list<std::pair<K, V>> &;

   private:
    size_t depth_;
    size_t max_size_;
    std::list<std::pair<K, V>> items_;
    mutable std::mutex latch_;
  };

  explicit ExtendibleHashTable(size_t bucket_size = 4);
  auto Find(const K &key, V &value) -> bool;
  auto Remove(const K &key) -> bool;
  auto Insert(const K &key, const V &value) -> bool;

  auto GetGlobalDepth() const -> size_t;
  auto GetLocalDepth(int directory_index) const -> size_t;
  auto GetNumBuckets() const -> size_t;

 private:
  auto IndexOf(const K &key) -> size_t;
  void SplitBucket(size_t directory_index);

  size_t global_depth_;
  size_t bucket_size_;
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> directory_;
};

}  // namespace bustub


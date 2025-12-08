//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../../include/container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <iostream>

namespace bustub {

    // Bucket 类实现
    template <typename K, typename V>
    ExtendibleHashTable<K, V>::Bucket::Bucket(size_t depth, size_t max_size) : depth_(depth), max_size_(max_size) {}

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Find(const K& key, V& value) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        for (const auto& item : items_) {
            if (item.first == key) {
                value = item.second;
                return true;
            }
        }
        return false;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Remove(const K& key) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        for (auto it = items_.begin(); it != items_.end(); ++it) {
            if (it->first == key) {
                items_.erase(it);
                return true;
            }
        }
        return false;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Insert(const K& key, const V& value) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);

        // 检查键是否已存在
        for (auto& item : items_) {
            if (item.first == key) {
                item.second = value;
                return true;
            }
        }

        // 如果桶已满，返回false
        if (items_.size() >= max_size_) {
            return false;
        }

        // 插入新键值对
        items_.emplace_back(key, value);
        return true;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::IsFull() const -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        return items_.size() >= max_size_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::IsEmpty() const -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        return items_.empty();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::GetDepth() const -> size_t {
        return depth_;
    }

    template <typename K, typename V>
    void ExtendibleHashTable<K, V>::Bucket::IncreaseDepth() {
        depth_++;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::GetItems() const -> const std::list<std::pair<K, V>>& {
        return items_;
    }

    // ExtendibleHashTable 类实现 
    template <typename K, typename V>
    ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
        : global_depth_(0), bucket_size_(bucket_size), directory_(1 << global_depth_) {
        for (size_t i = 0; i < directory_.size(); i++) {
            directory_[i] = std::make_shared<Bucket>(global_depth_, bucket_size_);
        }
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::IndexOf(const K& key) -> size_t {
        int mask = (1 << global_depth_) - 1;
        return std::hash<K>()(key) & mask;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Find(const K& key, V& value) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        size_t directory_index = IndexOf(key);
        auto bucket = directory_[directory_index];
        return bucket->Find(key, value);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Remove(const K& key) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        size_t directory_index = IndexOf(key);
        auto bucket = directory_[directory_index];
        return bucket->Remove(key);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Insert(const K& key, const V& value) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);

        while (true) {
            size_t directory_index = IndexOf(key);
            auto bucket = directory_[directory_index];

            // 尝试插入
            if (bucket->Insert(key, value)) {
                return true;
            }

            // 插入失败，桶已满，需要分裂
            if (bucket->GetDepth() == global_depth_) {
                // 需要扩展目录
                size_t old_size = directory_.size();
                directory_.resize(old_size * 2);

                // 复制指针到新的目录位置
                for (size_t i = 0; i < old_size; i++) {
                    directory_[old_size + i] = directory_[i];
                }
                global_depth_++;
            }

            // 分裂桶
            SplitBucket(directory_index);
        }
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> size_t {
        std::scoped_lock<std::mutex> lock(latch_);
        return global_depth_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetLocalDepth(int directory_index) const -> size_t {
        std::scoped_lock<std::mutex> lock(latch_);
        if (directory_index < 0 || static_cast<size_t>(directory_index) >= directory_.size()) {
            return 0;
        }
        return directory_[directory_index]->GetDepth();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> size_t {
        std::scoped_lock<std::mutex> lock(latch_);
        std::vector<std::shared_ptr<Bucket>> unique_buckets;
        for (const auto& bucket_ptr : directory_) {
            bool found = false;
            for (const auto& unique_bucket : unique_buckets) {
                if (unique_bucket == bucket_ptr) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                unique_buckets.push_back(bucket_ptr);
            }
        }
        return unique_buckets.size();
    }

    template <typename K, typename V>
    void ExtendibleHashTable<K, V>::SplitBucket(size_t directory_index) {
        auto old_bucket = directory_[directory_index];
        size_t local_depth = old_bucket->GetDepth();

        // 创建两个新桶
        auto new_bucket0 = std::make_shared<Bucket>(local_depth + 1, bucket_size_);
        auto new_bucket1 = std::make_shared<Bucket>(local_depth + 1, bucket_size_);

        // 重新分配旧桶中的项目
        const auto& items = old_bucket->GetItems();
        size_t mask = 1 << local_depth;

        for (const auto& item : items) {
            size_t hash_value = std::hash<K>()(item.first);
            if ((hash_value & mask) == 0) {
                new_bucket0->Insert(item.first, item.second);
            }
            else {
                new_bucket1->Insert(item.first, item.second);
            }
        }

        // 更新目录指针
        for (size_t i = 0; i < directory_.size(); i++) {
            if (directory_[i] == old_bucket) {
                if ((i & mask) == 0) {
                    directory_[i] = new_bucket0;
                }
                else {
                    directory_[i] = new_bucket1;
                }
            }
        }
    }

    // 模板实例化
    template class ExtendibleHashTable<int, int>;
    template class ExtendibleHashTable<int, std::string>;

}  // namespace bustub



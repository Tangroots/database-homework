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
        for (typename std::list<std::pair<K, V>>::iterator original_bucket_iterator = original_bucket_list.begin();
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
        current_ptr->SUnlock();  // test
        // rehash the directory
        for (size_t i = 0; i < dir_.size(); i++) {
          if ((i & new_mask) == new_bucket_underline) {
            dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
          }
        }
        // current_ptr->SUnlock();
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
        for (typename std::list<std::pair<K, V>>::iterator original_bucket_iterator = original_bucket_list.begin();
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
        current_ptr->SUnlock();  // test
        // rehash the directory
        for (size_t i = 0; i < dir_.size(); i++) {
          if ((i & new_mask) == new_bucket_underline) {
            dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
          }
        }
        // current_ptr->SUnlock();
        DirSUnlock();
      }
    }
  }
};
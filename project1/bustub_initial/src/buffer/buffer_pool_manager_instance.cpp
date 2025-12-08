//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_manager_instance.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // 分配连续的内存空间
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // 初始时所有帧都在空闲列表
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  // 注意：这里不再调用FlushAllPgsImp()，因为disk_manager_可能已经被销毁
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);
  
  frame_id_t frame_id;
  
  // 首先尝试从空闲列表获取帧
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 如果没有空闲帧，尝试从替换器获取受害者帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr; // 没有可替换的帧
    }
    
    // 如果受害者帧是脏的，写回磁盘
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    
    // 从页表中移除
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  
  // 分配新页ID
  *page_id = AllocatePage();
  
  // 重置页内存和元数据
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  
  // 添加到页表
  page_table_->Insert(*page_id, frame_id);
  
  // 记录访问并设置为不可驱逐
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);
  
  frame_id_t frame_id;
  
  // 检查页是否已在缓冲池中
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  
  // 页不在缓冲池中，需要从磁盘读取
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr; // 没有可替换的帧
    }
    
    // 如果受害者帧是脏的，写回磁盘
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  
  // 重置页并读取磁盘数据
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  
  // 添加到页表
  page_table_->Insert(page_id, frame_id);
  
  // 记录访问并设置为不可驱逐
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false; // 页不在缓冲池中
  }
  
  if (pages_[frame_id].pin_count_ == 0) {
    return false; // pin计数已经为0
  }
  
  pages_[frame_id].pin_count_--;
  
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false; // 页不在缓冲池中
  }
  
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);
  
  // 安全地刷新所有脏页
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      frame_id_t frame_id;
      if (page_table_->Find(pages_[i].GetPageId(), frame_id) && 
          pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
        pages_[i].is_dirty_ = false;
      }
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    DeallocatePage(page_id);
    return true; // 页不存在
  }
  
  if (pages_[frame_id].GetPinCount() > 0) {
    return false; // 页被pin住，不能删除
  }
  
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}  
}// namespace bustub

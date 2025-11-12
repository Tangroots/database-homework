//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  if (k == 0) {
    throw bustub::Exception("K must be greater than 0");
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  frame_id_t candidate = -1;
  size_t candidate_distance = 0;
  size_t candidate_earliest_time = std::numeric_limits<size_t>::max();
  bool found_inf_candidate = false;

  for (const auto &[fid, history] : frame_table_) {
    if (!history.is_evictable) {
      continue;
    }

    const auto &timestamps = history.access_timestamps;
    
    if (timestamps.size() < k_) {
      // 访问次数不足K次，优先级：最早访问时间
      size_t earliest_time = timestamps.empty() ? 0 : timestamps.front();
      
      if (!found_inf_candidate) {
        candidate = fid;
        candidate_earliest_time = earliest_time;
        found_inf_candidate = true;
      } else if (earliest_time < candidate_earliest_time) {
        candidate = fid;
        candidate_earliest_time = earliest_time;
      }
    } else {
      // 访问次数>=K次，优先级：最大k-distance
      size_t kth_previous_time = timestamps.front();
      size_t distance = current_timestamp_ - kth_previous_time;
      
      if (!found_inf_candidate) {
        if (candidate == -1 || distance > candidate_distance) {
          candidate = fid;
          candidate_distance = distance;
          candidate_earliest_time = kth_previous_time;
        } else if (distance == candidate_distance && kth_previous_time < candidate_earliest_time) {
          candidate = fid;
          candidate_earliest_time = kth_previous_time;
        }
      }
    }
  }

  if (candidate == -1) {
    return false;
  }

  frame_table_.erase(candidate);
  curr_size_--;
  *frame_id = candidate;
  
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw bustub::Exception("Invalid frame id");
  }

  current_timestamp_++;

  // 显式处理新帧插入，确保is_evictable默认值为false
  auto [it, inserted] = frame_table_.try_emplace(frame_id);
  auto &history = it->second;
  auto &timestamps = history.access_timestamps;

  // 新帧初始化：默认不可淘汰
  if (inserted) {
    history.is_evictable = false;
  }

  // 记录访问时间戳，保持最近k次访问
  timestamps.push_back(current_timestamp_);
  if (timestamps.size() > k_) {
    timestamps.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw bustub::Exception("Invalid frame id");
  }

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  auto &history = it->second;
  bool currently_evictable = history.is_evictable;

  if (currently_evictable == set_evictable) {
    return; // 状态未变，无需操作
  }

  // 更新可淘汰状态并调整计数
  history.is_evictable = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw bustub::Exception("Invalid frame id");
  }

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  if (!it->second.is_evictable) {
    throw bustub::Exception("Cannot remove non-evictable frame");
  }

  frame_table_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub

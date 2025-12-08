//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */

/**
 * Maximum Size of LRUKReplacer = Size of Buffer Pool
 * The LRUKReplacer needs to have information about every frame in the buffer pool because any of these frames could be
 * a candidate for replacement. If the LRUKReplacer's size was smaller than the buffer pool, it might not have
 * information about all the pages in the pool, leading to inefficient or incorrect replacement decisions. If it was
 * larger, it would hold information about pages that are no longer in the pool, which would be unnecessary and a waste
 * of resources.
 */
class LRUKReplacer {
 public:
  class Frame {
   public:
    explicit Frame(frame_id_t frame_id) : frame_id_(frame_id) {}
    ~Frame() = default;
    auto GetFrameId() -> frame_id_t { return frame_id_; }
    auto GetAccessHistory() -> std::vector<size_t> { return access_history_; }
    auto GetEvictable() -> bool { return evictable_; }
    auto SetEvictable(bool set_evictable) -> void { evictable_ = set_evictable; }
    auto SetAccessHistory(size_t timestamp) -> void { access_history_.push_back(timestamp); }
    auto Settimestamp(size_t time) -> void {
      if (timestamp_ > time) {
        timestamp_ = time;
      }
    }
    auto Gettimestamp() -> size_t { return timestamp_; }
    auto GetBackwardKDistance(size_t current_timestamp, size_t k) -> size_t {
      if (access_history_.size() < k) {
        return std::numeric_limits<size_t>::max();
      }
      return current_timestamp - access_history_[access_history_.size() - k];
    }

   private:
    frame_id_t frame_id_;                                    // frame id
    std::vector<size_t> access_history_;                     // access history of the frame
    bool evictable_ = false;                                 // whether the frame is evictable or not
    size_t timestamp_ = std::numeric_limits<size_t>::max();  // timestamp of the frame
  };
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k) {
    replacer_size_ = num_frames;
    k_ = k;
    curr_size_ = 0;
    current_timestamp_ = 0;
  }

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Evict(frame_id_t*) : Evict the frame with largest backward k-distance compared to
   * all other evictable frames being tracked by the Replacer.
   * Store the frame id in the output parameter and return True. If there are no evictable frames return False.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    if (curr_size_ == 0) {
      return false;
    }
    size_t max_backward_k_distance = 0;
    frame_id_t max_backward_k_distance_frame_id = 0;
    size_t max_backward_k_distance_timestamp = current_timestamp_;
    std::shared_ptr<Frame> max_backward_k_distance_frame_ptr = nullptr;
    for (auto &backward_frame_ptr : cash_list_) {
      if (backward_frame_ptr->GetEvictable()) {
        size_t backward_frame_k_distance = backward_frame_ptr->GetBackwardKDistance(current_timestamp_, k_);
        frame_id_t backward_frame_id = backward_frame_ptr->GetFrameId();
        size_t backward_frame_timestamp = backward_frame_ptr->Gettimestamp();
        if (backward_frame_k_distance > max_backward_k_distance) {
          max_backward_k_distance_frame_ptr = backward_frame_ptr;
          max_backward_k_distance = backward_frame_k_distance;
          max_backward_k_distance_frame_id = backward_frame_id;
          max_backward_k_distance_timestamp = backward_frame_timestamp;
        } else if (backward_frame_k_distance == max_backward_k_distance) {
          if (backward_frame_timestamp < max_backward_k_distance_timestamp) {
            max_backward_k_distance_frame_ptr = backward_frame_ptr;
            max_backward_k_distance = backward_frame_k_distance;
            max_backward_k_distance_frame_id = backward_frame_id;
            max_backward_k_distance_timestamp = backward_frame_timestamp;
          }
        }
      }
    }
    *frame_id = max_backward_k_distance_frame_id;
    auto it = std::find(cash_list_.begin(), cash_list_.end(), max_backward_k_distance_frame_ptr);
    if (it != cash_list_.end()) {
      cash_list_.erase(it);
    }
    // cash_list_.erase(max_backward_k_distance_frame_id);
    frame_map_.erase(max_backward_k_distance_frame_id);
    curr_size_--;
    return true;
  };

  /**
   * TODO(P1): Add implementation
   *
   * RecordAccess(frame_id_t) : Record that given frame id is accessed at current timestamp.
   * This method should be called after a page is pinned in the BufferPoolManager
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id > static_cast<int>(replacer_size_)) {
      throw std::exception();
    }
    current_timestamp_++;
    if (frame_map_.find(frame_id) != frame_map_.end()) {
      frame_map_[frame_id]->SetAccessHistory(current_timestamp_);
      frame_map_[frame_id]->Settimestamp(current_timestamp_);
      return;
    }
    frame_map_[frame_id] = std::make_shared<Frame>(frame_id);
    frame_map_[frame_id]->SetAccessHistory(current_timestamp_);
    frame_map_[frame_id]->Settimestamp(current_timestamp_);
    cash_list_.push_back(frame_map_[frame_id]);
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * SetEvictable(frame_id_t, bool set_evictable) :
   * This method controls whether a frame is evictable or not. It also controls LRUKReplacer's size.
   * You'll know when to call this function when you implement the BufferPoolManager.
   * To be specific, when pin count of a page reaches 0,
   * its corresponding frame is marked evictable and replacer's size is incremented.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    if ((frame_id > static_cast<int>(replacer_size_))) {
      throw std::exception();
    }
    if (frame_map_.find(frame_id) == frame_map_.end()) {
      return;
    }
    if (set_evictable && (!frame_map_[frame_id]->GetEvictable())) {
      curr_size_++;
      frame_map_[frame_id]->SetEvictable(true);
    } else if (!set_evictable && frame_map_[frame_id]->GetEvictable()) {
      curr_size_--;
      frame_map_[frame_id]->SetEvictable(false);
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * Remove(frame_id_t) : Clear all access history associated with a frame.
   * This method should be called only when a page is deleted in the BufferPoolManager.
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (frame_map_.find(frame_id) == frame_map_.end()) {
      return;
    }
    if (!frame_map_[frame_id]->GetEvictable()) {
      throw std::exception();
    }
    // cash_list_.erase(frame_map_[frame_id]);
    auto it = std::find(cash_list_.begin(), cash_list_.end(), frame_map_[frame_id]);
    if (it != cash_list_.end()) {
      cash_list_.erase(it);
    }
    frame_map_.erase(frame_id);
    curr_size_--;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_;
  };

  /**
   * @brief Return the std::shared_ptr of the frame with the specified frame_id.
   *
   * @return std::shared_ptr<Frame>
   */
  auto GetFrame(frame_id_t frame_id) -> std::shared_ptr<Frame> { return frame_map_[frame_id]; };

  /**
   * @brief Return the cash list of frames.
   *
   * @return std::list<std::shared_ptr<Frame>>
   */
  auto GetCashList() -> std::list<frame_id_t> {
    std::list<frame_id_t> cash_list;
    for (auto &it : cash_list_) {
      cash_list.push_front(it->GetFrameId());
    }
    return cash_list;
  };

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};  // current timestamp
  size_t curr_size_{0};          // current size of the replacer
  size_t replacer_size_;         // maximum size of the replacer
  size_t k_;                     // k value for LRU-k
  std::mutex latch_;
  std::list<std::shared_ptr<Frame>> cash_list_;                       // list of cash
  std::unordered_map<frame_id_t, std::shared_ptr<Frame>> frame_map_;  // map of frame id to frame
};

}  // namespace bustub

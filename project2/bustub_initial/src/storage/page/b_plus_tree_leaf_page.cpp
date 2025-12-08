//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

/*
 * Helper method to find and return the value associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPair(int index) -> const MappingType & { return array_[index]; }

/*
 * Helper method to find key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = 0;
  while (comparator(array_[index].first, key) < 0 && index < GetSize()) {
    index++;
  }
  if (index == GetSize()) {
    return false;
  }
  return comparator(array_[index].first, key) == 0;
}

/*
 * Helper method to find the index of a key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int index = 0;
  while (comparator(array_[index].first, key) < 0 && index < GetSize()) {
    index++;
  }
  if (index == GetSize()) {
    return -1;
  }
  if (comparator(array_[index].first, key) == 0) {
    return index;
  }
  return -1;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, ValueType &value, const KeyComparator &comparator)
    -> bool {
  int index = 0;
  while (comparator(array_[index].first, key) < 0 && index < GetSize()) {
    index++;
  }
  if (index == GetSize()) {
    return false;
  }
  if (comparator(array_[index].first, key) == 0) {
    value = array_[index].second;
    return true;
  }
  return false;
}

/**
 * Helper method to Insert a key & value pair into current leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertInLeaf(const KeyType &key, const ValueType &value,
                                              const KeyComparator &comparator) -> bool {
  int index = 0;
  while (comparator(array_[index].first, key) < 0 && index < GetSize()) {
    index++;
  }
  if (index == GetSize()) {
    array_[index] = {key, value};
    IncreaseSize(1);
    return true;
  }
  if (comparator(array_[index].first, key) == 0 && array_[index].second == value) {
    return false;
  }
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
  return true;
}

/**
 * Helper method to Remove a key & value pair from current leaf page
 * Remember to delete the pair from array, and shift the remaining pairs.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteInLeaf(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = 0;
  while (comparator(array_[index].first, key) < 0 && index < GetSize()) {
    index++;
  }
  if (index == GetSize()) {
    return false;
  }
  if (comparator(array_[index].first, key) == 0) {
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
    IncreaseSize(-1);
    return true;
  }
  return false;
}

/**
 * Helper method to Move all key & value pairs from current page to destination
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFrom(array_, GetSize(), buffer_pool_manager);
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/**
 * Helper method to Move half of key & value pairs from current page to
 * destination
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastHalfTo(BPlusTreeLeafPage *recipient, int spiltindex,
                                                BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  recipient->CopyFrom(array_ + spiltindex, size - spiltindex, buffer_pool_manager);
  recipient->SetNextPageId(GetNextPageId());
  IncreaseSize(-1 * (size - spiltindex));
}

/**
 * Helper method to copy key & value pairs from current page to destination
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // Copies all elements in the range [first, last)
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(MappingType *items) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = *items;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(MappingType *items) {
  array_[GetSize()] = *items;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, KeyType middlekey,
                                                   BufferPoolManager *buffer_pool_manager) {
  auto last_item = array_[GetSize() - 1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(&last_item);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, KeyType middlekey,
                                                  BufferPoolManager *buffer_pool_manager) {
  auto first_item = array_[0];
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
  recipient->CopyLastFrom(&first_item);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

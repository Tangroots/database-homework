//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/**
 * Helper method to set value associated with input "index"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}

/**
 * Helper method to get the value and key pair associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ElemntAt(int index) const -> MappingType { return array_[index]; }

/**
 * Helper method to find the index of the value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}

/**
 * Helper method to find the smallest number i such that array_[i].first >= key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndexAboveKey(const KeyType &key, const KeyComparator &comparator) const
    -> int {
  auto it = std::find_if(array_ + 1, array_ + GetSize(),
                         [&key, &comparator](const auto &pair) { return comparator(pair.first, key) >= 0; });
  auto index = std::distance(array_, it);
  return index - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  if (comparator(array_[1].first, key) > 0) {
    return 0;
  }
  if (comparator(key, array_[GetSize() - 1].first) >= 0) {
    return GetSize() - 1;
  }
  int index = 1;
  while (comparator(key, array_[index].first) >= 0) {
    index++;
  }
  return index - 1;
}

/**
 * Helper method to set the new root with two children
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetNewRoot(ValueType &old_value, const KeyType &new_key,
                                                const ValueType &new_value) -> void {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/**
 * Helper method to insert node after specific "index"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> void {
  int new_value_index = ValueIndex(old_value) + 1;
  std::move_backward(array_ + new_value_index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[new_value_index].first = new_key;
  array_[new_value_index].second = new_value;
  // array_[index].second = old_value;
  IncreaseSize(1);
}

/**
 * Helper method to delete node
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteNode(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = FindKeyIndex(key, comparator);
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
  return true;
}

/**
 * Helper method to move all the key-value pairs to the right sibling page to
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array_ + GetSize());

  for (int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i + GetSize()));
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }

  IncreaseSize(size);
}

/**
 * Helper method to move half of key & value pairs from current page to destination
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastHalfTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, int spiltindex,
                                                    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFrom(array_ + spiltindex, GetSize() - spiltindex, buffer_pool_manager);
  IncreaseSize(-1 * (GetSize() - spiltindex));
}

/**
 * Helper method to copy the key-value pairs to the right sibling page to
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array_ + GetSize());
  for (int i = 0; i < size; i++) {
    Page *page = buffer_pool_manager->FetchPage(items[i].second);
    auto *child = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(items[i].second, true);
  }
  IncreaseSize(size);
}

/**
 * Helper method to move the last key-value pair to the front of current page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, KeyType k,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  // recipient->SetKeyAt(0, k);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient, KeyType middlekey,
    bustub::BufferPoolManager *buffer_pool_manager) {
  // SetKeyAt(0, middlekey);
  auto first_item = array_[0];
  recipient->CopyLastFrom(first_item, buffer_pool_manager);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}

/**
 * Helper method to copy the first key-value pair to the front of current page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = item;
  IncreaseSize(1);
  Page *page = buffer_pool_manager->FetchPage(item.second);
  auto *child = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/**
 * Helper method to copy the last key-value pair to the end of current page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = item;
  IncreaseSize(1);
  Page *page = buffer_pool_manager->FetchPage(item.second);
  auto *child = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

// value type for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

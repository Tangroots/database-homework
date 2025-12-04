//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(INVALID_PAGE_ID);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to get/set next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/**
 * Helper method to get/set the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

// 模板实例化
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
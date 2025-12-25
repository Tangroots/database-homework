//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"
#include <stdexcept>
#include <string>

namespace bustub {

/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(INVALID_PAGE_ID);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::KeyAt(int index) const -> KeyType {
  // 添加边界检查以帮助调试
  if (index < 0 || index >= GetSize()) {
    throw std::out_of_range("KeyAt: index " + std::to_string(index) + " out of range, size=" + std::to_string(GetSize()));
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get/set the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  // 添加边界检查以帮助调试
  if (index < 0 || index >= GetSize()) {
    throw std::out_of_range("ValueAt: index " + std::to_string(index) + " out of range, size=" + std::to_string(GetSize()));
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  // 添加边界检查以帮助调试
  if (index < 0 || index >= GetSize()) {
    throw std::out_of_range("SetValueAt: index " + std::to_string(index) + " out of range, size=" + std::to_string(GetSize()));
  }
  array_[index].second = value;
}

// 模板实例化
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

}  // namespace bustub
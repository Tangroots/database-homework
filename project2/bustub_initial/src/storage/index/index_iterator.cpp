/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leftmost_leaf, int idx, BufferPoolManager *buffer_pool_manager) {
  leaf_ = leftmost_leaf;
  idx_ = idx;
  buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    auto *page = reinterpret_cast<Page *>(leaf_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && idx_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw std::out_of_range("IndexIterator out of range");
  }
  return leaf_->GetPair(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    throw std::out_of_range("IndexIterator out of range");
  }
  idx_++;
  if (idx_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_pid = leaf_->GetNextPageId();
    Page *next_page = buffer_pool_manager_->FetchPage(next_pid);  // pined page
    next_page->RLatch();
    reinterpret_cast<Page *>(leaf_)->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    auto *next_node = reinterpret_cast<LeafPage *>(next_page->GetData());
    leaf_ = next_node;
    idx_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const INDEXITERATOR_TYPE &itr) const -> bool {
  return leaf_ == itr.leaf_ && idx_ == itr.idx_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const INDEXITERATOR_TYPE &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

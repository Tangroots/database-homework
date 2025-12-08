#include <string>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();
  auto *pagec = FindLeafPageCon(key, Operation::SEARCH, transaction);
  ValueType v;
  auto *pagecnode = reinterpret_cast<LeafPage *>(pagec->GetData());
  if (pagec == nullptr) {
    return false;
  }
  bool res = pagecnode->FindValue(key, v, comparator_);
  if (!res) {
    pagec->RUnlatch();
    buffer_pool_manager_->UnpinPage(pagec->GetPageId(), false);
    return false;
  }
  result->push_back(v);
  pagec->RUnlatch();
  buffer_pool_manager_->UnpinPage(pagec->GetPageId(), false);
  return true;
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page * {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto pagec = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(pagec->GetData());
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    // find the index i = smallest number that key<=K(i)
    child_page_id = internal_node->ValueAt(internal_node->FindKeyIndex(key, comparator_));
    // Fetch the child page and update the node pointer.
    pagec = buffer_pool_manager_->FetchPage(child_page_id);
    node = reinterpret_cast<BPlusTreePage *>(pagec->GetData());
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
  }
  return pagec;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageCon(const KeyType &key, Operation op, Transaction *transaction) -> Page * {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto pagec = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(pagec->GetData());
  if (op == Operation::SEARCH) {
    root_page_id_latch_.RUnlock();
    pagec->RLatch();
  } else {
    pagec->WLatch();
    if (op == Operation::DELETE && node->GetSize() > 2) {
      ReleaseLatchFromQueue(transaction);
    }
    if (op == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      ReleaseLatchFromQueue(transaction);
    }
    if (op == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      ReleaseLatchFromQueue(transaction);
    }
  }
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    // find the index i = smallest number that key<=K(i)
    child_page_id = internal_node->ValueAt(internal_node->FindKeyIndex(key, comparator_));
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    // Fetch the child page and update the node pointer.
    if (op == Operation::SEARCH) {
      pagec->RUnlatch();
      child_page->RLatch();
      buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
    }
    if (op == Operation::INSERT) {
      child_page->WLatch();
      transaction->AddIntoPageSet(pagec);
      // check child node is safe
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseLatchFromQueue(transaction);
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }
    if (op == Operation::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(pagec);
      // check child node is safe
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }
    pagec = child_page;
    node = child_node;
  }
  return pagec;
}

// Release the latch of ancestor pages
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto pages = transaction->GetPageSet();
  for (auto page : *pages) {
    if (page == nullptr) {
      this->root_page_id_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
  pages->clear();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value, transaction);
    ReleaseLatchFromQueue(transaction);
    return true;
  }  // Insert into Leaf Page
  auto *pagec = FindLeafPageCon(key, Operation::INSERT, transaction);
  auto *pagecnode = reinterpret_cast<LeafPage *>(pagec->GetData());
  if (pagec == nullptr) {
    ReleaseLatchFromQueue(transaction);
    return false;
  }
  if (pagecnode->GetSize() < pagecnode->GetMaxSize() - 1) {  // leaf page has space
    bool insert_success = pagecnode->InsertInLeaf(key, value, comparator_);
    if (!insert_success) {
      ReleaseLatchFromQueue(transaction);
      pagec->WUnlatch();
      buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
      return false;
    }
    ReleaseLatchFromQueue(transaction);
    pagec->WUnlatch();
    buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
    return true;
  }  // leaf page has n-1 key value already, spilt it
  bool insert_success = pagecnode->InsertInLeaf(key, value, comparator_);
  if (!insert_success) {
    ReleaseLatchFromQueue(transaction);
    pagec->WUnlatch();
    buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
    return false;
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *newpage = Split(pagecnode);
  newpage->SetNextPageId(pagecnode->GetNextPageId());
  pagecnode->SetNextPageId(newpage->GetPageId());
  InsertIntoParent(pagecnode, newpage->KeyAt(0), newpage, transaction);
  ReleaseLatchFromQueue(transaction);
  pagec->WUnlatch();
  buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(newpage->GetPageId(), true);
  return true;
}

/*
 * Insert leaf page into internal page after split
 * User needs to first find the parent page of given node.
 * If parent page has enough space, simply insert entry into it, otherwise
 * recursively split parent page and insert entry into parent's parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) -> void {
  page_id_t new_page_id = INVALID_PAGE_ID;
  page_id_t old_page_id = old_node->GetPageId();
  if (old_node->IsRootPage()) {
    Page *rootpage = buffer_pool_manager_->NewPage(&new_page_id);
    if (rootpage == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "root page is null");
    }
    auto root = reinterpret_cast<InternalPage *>(rootpage->GetData());
    root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    root->SetNewRoot(old_page_id, key, new_node->GetPageId());
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(0);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parentpage = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent = reinterpret_cast<InternalPage *>(parentpage->GetData());
  if (parent->GetSize() < parent->GetMaxSize()) {  // parent page has space
    old_page_id = old_node->GetPageId();
    parent->InsertNodeAfter(old_page_id, key, new_node->GetPageId());
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  } else {
    old_page_id = old_node->GetPageId();
    parent->InsertNodeAfter(old_page_id, key, new_node->GetPageId());
    auto *newpage = Split(parent);
    InsertIntoParent(parent, newpage->KeyAt(0), newpage, transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(newpage->GetPageId(), true);
  }
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first create a root page and then insert entry into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page *rootpage = buffer_pool_manager_->NewPage(&root_page_id);
  if (rootpage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "root page is null");
  }
  auto *root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(rootpage->GetData());
  root->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  root->InsertInLeaf(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *newpage = buffer_pool_manager_->NewPage(&new_page_id);
  if (newpage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "new page is null");
  }
  auto *newnode = reinterpret_cast<N *>(newpage->GetData());
  newnode->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  int spilt_index = node->GetMinSize();
  node->MoveLastHalfTo(newnode, spilt_index, buffer_pool_manager_);
  // node->SetSize(spilt_index);
  // std::copy(node->array_ + spilt_index, node->array_ + node->GetSize(), newnode->array_);
  // newnode->SetSize(node->GetSize() - spilt_index);
  return newnode;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (root_page_id_ == INVALID_PAGE_ID) {
    ReleaseLatchFromQueue(transaction);
    return;
  }
  auto *pagec = FindLeafPageCon(key, Operation::DELETE, transaction);
  auto *pagecnode = reinterpret_cast<LeafPage *>(pagec->GetData());
  if (pagecnode == nullptr) {
    ReleaseLatchFromQueue(transaction);
    return;
  }
  std::vector<page_id_t> page_delete;
  DeleteEntry(pagecnode, key, comparator_, transaction);
  pagec->WUnlatch();
  buffer_pool_manager_->UnpinPage(pagecnode->GetPageId(), true);
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
  transaction->GetDeletedPageSet()->clear();
}

/**
 * Delete entry from the B+ tree, return false if key is not in the tree
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::DeleteEntry(N *node, const KeyType &key, const KeyComparator &comparator, Transaction *transaction)
    -> bool {
  auto *pagec = reinterpret_cast<BPlusTreePage *>(node);
  if (pagec == nullptr) {
    return false;
  }
  bool deletion_result = false;
  if (pagec->IsLeafPage()) {
    auto *pagecnode = static_cast<LeafPage *>(pagec);
    deletion_result = pagecnode->DeleteInLeaf(key, comparator);
  } else {  // It's an internal node.
    auto *pagecnode = static_cast<InternalPage *>(pagec);
    deletion_result = pagecnode->DeleteNode(key, comparator);
  }
  // After deletion, check for underflow or if it's a special root case.
  bool needs_rebalancing = pagec->GetSize() < pagec->GetMinSize() && !pagec->IsRootPage();
  bool is_root_and_single_entry = pagec->IsRootPage() && pagec->GetSize() == 1 && !pagec->IsLeafPage();
  if (pagec->IsRootPage()) {
    if (pagec->GetSize() > 1) {
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
      return deletion_result;
    }
    if (pagec->IsLeafPage() && pagec->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      transaction->AddIntoDeletedPageSet(pagec->GetPageId());
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
      // buffer_pool_manager_->DeletePage(page_id);
      return true;
    }
  }
  if (is_root_and_single_entry) {
    // Special root handling logic here.
    // Note: The root handling seems incorrect, typically you'd want to adjust the root pointer in the tree.
    // Assuming deletion of old root and setting a new root.
    auto *root = reinterpret_cast<InternalPage *>(pagec);
    auto *newroot = buffer_pool_manager_->FetchPage(root->ValueAt(0));
    auto *newrootnode = reinterpret_cast<BPlusTreePage *>(newroot->GetData());
    newrootnode->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = newrootnode->GetPageId();
    UpdateRootPageId(0);
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(newrootnode->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true);
    transaction->AddIntoDeletedPageSet(pagec->GetPageId());
    // buffer_pool_manager_->DeletePage(pagec->GetPageId());
    return true;
  }
  if (needs_rebalancing) {
    // Handle underflow which may include merging or redistributing.
    auto *parentpage = reinterpret_cast<InternalPage *>(FetchPage(pagec->GetParentPageId()));
    int index = parentpage->ValueIndex(pagec->GetPageId());
    // if index>0 then sibling is on the left, else it's on the right.
    auto siblingindex = index > 0 ? index - 1 : index + 1;
    auto *siblingoriginalpage = buffer_pool_manager_->FetchPage(parentpage->ValueAt(siblingindex));
    auto *siblingpage = reinterpret_cast<BPlusTreePage *>(siblingoriginalpage);
    siblingoriginalpage->WLatch();
    // KeyType key_at = index>0?parentpage->KeyAt(index-1):parentpage->KeyAt(index);
    KeyType key_at = index > 0 ? parentpage->KeyAt(index) : parentpage->KeyAt(index + 1);
    bool is_merge;
    if (pagec->IsLeafPage()) {
      is_merge = (siblingpage->GetSize() + pagec->GetSize()) <= (pagec->GetMaxSize() - 1);
    } else {
      is_merge = (siblingpage->GetSize() + pagec->GetSize()) <= pagec->GetMaxSize();
    }
    if (is_merge) {
      Coalesce(pagec, siblingpage, parentpage, index, key_at, transaction);
    } else {
      Redistribute(pagec, siblingpage, parentpage, index, transaction);
    }
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parentpage->GetPageId(), true);
    siblingoriginalpage->WUnlatch();
    buffer_pool_manager_->UnpinPage(siblingpage->GetPageId(), true);
  }

  // Ensure pages are always unpinned.
  ReleaseLatchFromQueue(transaction);
  // buffer_pool_manager_->UnpinPage(pagec->GetPageId(), true); // Consider marking dirty as appropriate.
  return deletion_result;
}

/**
 * Coalesce two pages and redistribute keys & values evenly
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *node, N *sibling, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                              int index, KeyType k, Transaction *transaction) {
  if (index == 0) {
    std::swap(node, sibling);
    index = 1;
  }
  if (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *internal_sibling = reinterpret_cast<InternalPage *>(sibling);
    internal_node->MoveAllTo(internal_sibling, k, buffer_pool_manager_);
    // sibling->MoveAllTo(node, k, buffer_pool_manager_);
  } else {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *leaf_sibling = reinterpret_cast<LeafPage *>(sibling);
    leaf_node->MoveAllTo(leaf_sibling, k, buffer_pool_manager_);
    // sibling->MoveAllTo(node,k, buffer_pool_manager_);
  }
  DeleteEntry(parent, k, comparator_, transaction);
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // After deletion, check for underflow or if it's a special root case.
  bool needs_rebalancing = node->GetSize() < node->GetMinSize() && !node->IsRootPage();
  bool is_root_and_single_entry = node->IsRootPage() && node->GetSize() == 1 && !node->IsLeafPage();
  if (node->IsRootPage()) {
    if (node->GetSize() > 1) {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
    if (node->IsLeafPage() && node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(node->GetPageId());
      return true;
    }
  }
  if (is_root_and_single_entry) {
    // Special root handling logic here.
    // Note: The root handling seems incorrect, typically you'd want to adjust the root pointer in the tree.
    // Assuming deletion of old root and setting a new root.
    auto *root = reinterpret_cast<InternalPage *>(node);
    auto *newroot = buffer_pool_manager_->FetchPage(root->ValueAt(0));
    auto *newrootnode = reinterpret_cast<BPlusTreePage *>(newroot->GetData());
    newrootnode->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = newrootnode->GetPageId();
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    // buffer_pool_manager_->DeletePage(pagec->GetPageId());
    return true;
  }
  if (needs_rebalancing) {
    // Handle underflow which may include merging or redistributing.
    auto *parentpage = reinterpret_cast<InternalPage *>(FetchPage(node->GetParentPageId()));
    int index = parentpage->ValueIndex(node->GetPageId());
    // if index>0 then sibling is on the left, else it's on the right.
    auto siblingindex = index > 0 ? index - 1 : index + 1;
    auto *siblingpage = reinterpret_cast<BPlusTreePage *>(FetchPage(parentpage->ValueAt(siblingindex)));
    // KeyType key_at = index>0?parentpage->KeyAt(index-1):parentpage->KeyAt(index);
    KeyType key_at = index > 0 ? parentpage->KeyAt(index) : parentpage->KeyAt(index + 1);
    bool is_merge = (siblingpage->GetSize() + node->GetSize()) <= node->GetMaxSize();
    auto nodepage = reinterpret_cast<BPlusTreePage *>(node);
    if (is_merge) {
      Coalesce(nodepage, siblingpage, parentpage, index, key_at, transaction);
    } else {
      Redistribute(nodepage, siblingpage, parentpage, index, transaction);
    }
    buffer_pool_manager_->UnpinPage(parentpage->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(siblingpage->GetPageId(), true);
  }
  // Ensure pages are always unpinned.
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);  // Consider marking dirty as appropriate.
  return true;
}

/**
 * Redistribute keys & values between two pages
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *node, N *sibling, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                                  int index, Transaction *transaction) {
  if (index > 0) {  // node is on the right of sibling,
    if (!node->IsLeafPage()) {
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      auto *internal_sibling = reinterpret_cast<InternalPage *>(sibling);
      internal_sibling->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    } else {
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      auto *leaf_sibling = reinterpret_cast<LeafPage *>(sibling);
      leaf_sibling->MoveLastToFrontOf(leaf_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    if (!node->IsLeafPage()) {
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      auto *internal_sibling = reinterpret_cast<InternalPage *>(sibling);
      internal_sibling->MoveFirstToEndOf(internal_node, parent->KeyAt(index + 1), buffer_pool_manager_);
      parent->SetKeyAt(index + 1, internal_sibling->KeyAt(0));
    } else {
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      auto *leaf_sibling = reinterpret_cast<LeafPage *>(sibling);
      leaf_sibling->MoveFirstToEndOf(leaf_node, parent->KeyAt(index + 1), buffer_pool_manager_);
      parent->SetKeyAt(index + 1, leaf_sibling->KeyAt(0));
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  root_page_id_latch_.RLock();
  auto pagec = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(pagec->GetData());
  root_page_id_latch_.RUnlock();
  pagec->RLatch();
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    // find the index i = smallest number that key<=K(i)
    child_page_id = internal_node->ValueAt(0);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    // Fetch the child page and update the node pointer.
    pagec->RUnlatch();
    child_page->RLatch();
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
    pagec = child_page;
    node = child_node;
  }
  return INDEXITERATOR_TYPE(reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  root_page_id_latch_.RLock();
  Page *pagec = FindLeafPageCon(key, Operation::SEARCH, nullptr);
  if (pagec == nullptr) {
    throw Exception(ExceptionType::INVALID, "page is null");
  }
  auto node = reinterpret_cast<LeafPage *>(pagec->GetData());
  int index = node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(node, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  root_page_id_latch_.RLock();
  Page *pagec = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(pagec->GetData());
  root_page_id_latch_.RUnlock();
  pagec->RLatch();
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    // find the index i = smallest number that key<=K(i)
    child_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    // Fetch the child page and update the node pointer.
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    pagec->RUnlatch();
    child_page->RLatch();
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
    pagec = child_page;
    node = child_node;
  }
  auto *leaf_node = reinterpret_cast<LeafPage *>(pagec->GetData());
  return INDEXITERATOR_TYPE(leaf_node, leaf_node->GetSize(), buffer_pool_manager_);
}

/*
 * Get the number of whole pairs
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSize() -> int {
  int size = 0;
  for (auto iterator = Begin(); iterator != End(); ++iterator) {
    size = size + 1;
  }
  return size;
}

/**
 * Fetch page from disk
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  auto bplustreepage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  return bplustreepage;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**********************************************************************************************************
 * ********************************************************************************************************
 * Test and check functions
 *******************************************************************************************************
 * ********************************************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsBalanced(page_id_t pid) -> int{
  if (root_page_id_ == INVALID_PAGE_ID) {return -1;}
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception(ExceptionType::INVALID, "all page are pinned while IsBalanced");
  }
  int ret = 0;
  if (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    int last = -2;
    for (int i = 0; i < page->GetSize(); i++) {
      int cur = IsBalanced(page->ValueAt(i));
      if (cur >= 0 && last == -2) {
        last = cur;
        ret = last + 1;
      } else if (last != cur) {
        ret = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {return true;}
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception(ExceptionType::INVALID, "all page are pinned while IsPageCorr");
  }
  bool ret = true;
  if (node->IsLeafPage()) {  // Leaf Node Check
    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = page->GetSize();
    if (node->IsRootPage()) {
      ret = ret && size <= node->GetMaxSize();
    } else {
      ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    }
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
        ret = false;
        break;
      }
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  } else {  // Internal Node Check
    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int size = page->GetSize();
    if (node->IsRootPage()) {
      ret = ret && (size >= 2 && size <= node->GetMaxSize());
    } else {
      ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    }
    std::pair<KeyType, KeyType> left;
    std::pair<KeyType, KeyType> right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        ret = ret && IsPageCorr(page->ValueAt(0), left);
      }
      ret = ret && IsPageCorr(page->ValueAt(i), right);
      ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
      ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
      if (!ret) {break;}
      left = right;
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsUnlocked(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {return true;}
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception(ExceptionType::INVALID, "all page are pinned while IsPageCorr");
  }
  bool ret = true;
  if (node->IsLeafPage()) {  // Leaf Node Check
    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = page->GetSize();
    if (node->IsRootPage()) {
      ret = ret && reinterpret_cast<Page *>(node)->IsUnlocked();
    }
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
        ret = false;
        break;
      }
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  } else {  // Internal Node Check
    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int size = page->GetSize();
    if (node->IsRootPage()) {
      ret = ret && reinterpret_cast<Page *>(node)->IsUnlocked();
    }
    std::pair<KeyType, KeyType> left;
    std::pair<KeyType, KeyType> right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        ret = ret && IsUnlocked(page->ValueAt(0), left);
      }
      ret = ret && IsUnlocked(page->ValueAt(i), right);
      ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
      ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
      if (!ret) {break;}
      left = right;
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Check(bool forceCheck) -> bool{
  if (!forceCheck && !open_check_) {
    return true;
  }
  std::pair<KeyType, KeyType> in;
  bool is_page_in_order_and_size_corr = IsPageCorr(root_page_id_, in);
  std::pair<KeyType, KeyType> out;
  bool is_unlocked = IsUnlocked(root_page_id_, out);
  bool is_bal = (IsBalanced(root_page_id_) >= 0);
  auto *instance = dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_);
  bool is_all_unpin = instance->CheckAllUnpined();
  if (!is_page_in_order_and_size_corr) {std::cout << "problem in page order or page size" << std::endl;}
  if (!is_bal) {std::cout << "problem in balance" << std::endl;}
  if (!is_all_unpin) {std::cout << "problem in page unpin" << std::endl;}
  if (!is_unlocked) {std::cout << "problem in page lock" << std::endl;}
  return is_page_in_order_and_size_corr && is_bal && is_all_unpin && is_unlocked;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

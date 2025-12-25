#include "storage/index/b_plus_tree.h"
#include <string>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
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
      internal_max_size_(internal_max_size) {
  // 如果传入-1，使用默认值
  if (leaf_max_size_ == -1) {
    leaf_max_size_ = (BUSTUB_PAGE_SIZE - 28) / sizeof(std::pair<KeyType, ValueType>);
  }
  if (internal_max_size_ == -1) {
    internal_max_size_ = (BUSTUB_PAGE_SIZE - 24) / sizeof(std::pair<KeyType, page_id_t>);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  // 如果根节点ID是无效的，说明树是空的
  return root_page_id_ == INVALID_PAGE_ID; 
}
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
  // 如果树是空的，直接返回false
  if (IsEmpty()) {
    return false;
  }
  
  // 找到可能包含这个key的叶子节点
  auto leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return false;
  }
  
  // 将Page转换为叶子节点类型
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 在叶子节点中查找key
  int size = leaf_node->GetSize();
  for (int i = 0; i < size; i++) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {
      // 找到了！将对应的value加入结果
      result->push_back(leaf_node->ValueAt(i));
      // 释放页面
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
  }
  
  // 没找到，释放页面
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
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
  // 如果树是空的，创建新树
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  
  // 否则，插入到叶子节点
  return InsertIntoLeaf(key, value, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 如果树是空的，直接返回
  if (IsEmpty()) {
    return;
  }
  
  // 找到包含这个key的叶子节点
  auto leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return;
  }
  
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 尝试从叶子节点删除键
  bool deleted = DeleteFromLeaf(leaf_node, key);
  
  if (deleted) {
    // 如果删除成功，检查是否需要合并或重新分配
    CoalesceOrRedistribute(leaf_node, transaction);
  }
  
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), deleted);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  return root_page_id_; 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
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

/*****************************************************************************
 * HELPER FUNCTIONS
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> Page * {
  // 如果树是空的，返回nullptr
  if (IsEmpty()) {
    return nullptr;
  }
  
  // 从根节点开始
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    return nullptr;
  }
  
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
  // 一直向下查找直到叶子节点
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    
    page_id_t child_page_id;
    if (leftMost) {
      // 如果要找最左边的叶子，总是选择第一个子节点
      child_page_id = internal_node->ValueAt(0);
    } else {
      // 否则，使用二分查找找到合适的子节点
      int size = internal_node->GetSize();
      int child_index = 1;  // 从1开始，因为第0个key是无效的
      
      // 找到第一个大于key的位置
      while (child_index < size && comparator_(internal_node->KeyAt(child_index), key) <= 0) {
        child_index++;
      }
      
      // 选择前一个子节点
      child_page_id = internal_node->ValueAt(child_index - 1);
    }
    
    // 释放当前页面，获取子页面
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    if (page == nullptr) {
      return nullptr;
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  
  return page;
}

/*
 * Create a new tree and insert first key & value pair into it
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> void {
  // 创建新的根节点（叶子节点）
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  
  // 初始化叶子节点
  auto root = reinterpret_cast<LeafPage *>(new_page->GetData());
  root->Init(leaf_max_size_);
  root->SetPageId(new_page_id);
  
  // 插入第一个键值对
  root->SetKeyAt(0, key);
  root->SetValueAt(0, value);
  root->SetSize(1);
  
  // 更新根节点ID
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);  // 1表示插入新记录
  
  // 释放页面
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert key & value pair into leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 找到应该插入的叶子节点
  auto leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return false;
  }
  
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int size = leaf_node->GetSize();
  
  // 检查是否已存在（我们只支持唯一键）
  for (int i = 0; i < size; i++) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {
      // 键已存在，释放页面并返回false
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  }
  
  // 找到插入位置（保持有序）
  int insert_pos = size;
  for (int i = 0; i < size; i++) {
    if (comparator_(key, leaf_node->KeyAt(i)) < 0) {
      insert_pos = i;
      break;
    }
  }
  
  // 如果节点未满，直接插入
  if (size < leaf_node->GetMaxSize()) {
    // 移动后面的元素
    for (int i = size; i > insert_pos; i--) {
      leaf_node->SetKeyAt(i, leaf_node->KeyAt(i - 1));
      leaf_node->SetValueAt(i, leaf_node->ValueAt(i - 1));
    }
    
    // 插入新元素
    leaf_node->SetKeyAt(insert_pos, key);
    leaf_node->SetValueAt(insert_pos, value);
    leaf_node->IncreaseSize(1);
    
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  
  // 节点已满，需要分裂
  // 创建一个临时数组来存储所有元素（包括新元素）
  int new_size = size + 1;
  auto temp_keys = new KeyType[new_size];
  auto temp_values = new ValueType[new_size];
  
  // 复制现有元素并插入新元素到正确位置
  int j = 0;
  for (int i = 0; i < new_size; i++) {
    if (i == insert_pos) {
      temp_keys[i] = key;
      temp_values[i] = value;
    } else {
      temp_keys[i] = leaf_node->KeyAt(j);
      temp_values[i] = leaf_node->ValueAt(j);
      j++;
    }
  }
  
  // 执行分裂
  auto new_leaf_node = Split(leaf_node);
  
  // 重新分配元素
  int split_index = (new_size + 1) / 2;
  
  // 前半部分留在原节点
  for (int i = 0; i < split_index; i++) {
    leaf_node->SetKeyAt(i, temp_keys[i]);
    leaf_node->SetValueAt(i, temp_values[i]);
  }
  leaf_node->SetSize(split_index);
  
  // 后半部分移到新节点
  for (int i = split_index, k = 0; i < new_size; i++, k++) {
    new_leaf_node->SetKeyAt(k, temp_keys[i]);
    new_leaf_node->SetValueAt(k, temp_values[i]);
  }
  new_leaf_node->SetSize(new_size - split_index);
  
  // 清理临时数组
  delete[] temp_keys;
  delete[] temp_values;
  
  // 将中间键插入到父节点
  auto middle_key = new_leaf_node->KeyAt(0);
  InsertIntoParent(leaf_node, middle_key, new_leaf_node, transaction);
  
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  
  return true;
}

/*
 * Split leaf page and return new page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage *node) -> LeafPage * {
  // 创建新页面
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  
  // 初始化新叶子节点
  auto new_leaf_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_node->Init(leaf_max_size_);
  new_leaf_node->SetPageId(new_page_id);
  new_leaf_node->SetParentPageId(node->GetParentPageId());
  
  // 更新叶子节点的链表指针
  new_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  
  return new_leaf_node;
}

/*
 * Insert key & pointer pair into parent after split
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, 
                                      BPlusTreePage *new_node, Transaction *transaction) -> void {
  // 如果old_node是根节点，需要创建新的根节点
  if (old_node->IsRootPage()) {
    // 创建新的根节点
    page_id_t new_root_id;
    auto new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    if (new_root_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new root page");
    }
    
    auto new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(internal_max_size_);
    new_root->SetPageId(new_root_id);
    
    // 先设置size，因为SetValueAt会检查边界
    new_root->SetSize(2);
    
    // 设置新根节点的第一个指针指向old_node
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, new_node->GetPageId());
    
    // 更新子节点的父指针
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    
    // 更新根节点ID
    root_page_id_ = new_root_id;
    UpdateRootPageId(0);
    
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }
  
  // 否则，插入到父节点
  page_id_t parent_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::INVALID, "Cannot fetch parent page");
  }
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 找到old_node在父节点中的位置
  int insert_index = parent_node->GetSize();
  for (int i = 0; i < parent_node->GetSize(); i++) {
    if (parent_node->ValueAt(i) == old_node->GetPageId()) {
      insert_index = i + 1;
      break;
    }
  }
  
  // 如果父节点未满，直接插入
  // 注意：内部节点的第0个位置只有Value没有Key，所以实际能存储的键值对数量是MaxSize
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    // 先增加size
    parent_node->IncreaseSize(1);
    
    // 移动后面的元素
    for (int i = parent_node->GetSize() - 1; i > insert_index; i--) {
      parent_node->SetKeyAt(i, parent_node->KeyAt(i - 1));
      parent_node->SetValueAt(i, parent_node->ValueAt(i - 1));
    }
    
    // 插入新元素
    parent_node->SetKeyAt(insert_index, key);
    parent_node->SetValueAt(insert_index, new_node->GetPageId());
    
    // 更新新节点的父指针
    new_node->SetParentPageId(parent_id);
    
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }
  
  // 父节点已满，需要分裂
  // 创建临时数组存储所有元素
  int parent_size = parent_node->GetSize();
  
  auto temp_keys = new KeyType[parent_size + 1];
  auto temp_values = new page_id_t[parent_size + 1];
  
  // 复制现有元素并插入新元素
  // 注意：我们需要将原有的parent_size个元素加上1个新元素，总共parent_size+1个元素
  int j = 0;  // j是原数组的索引
  for (int i = 0; i <= parent_size; i++) {  // i是新数组的索引
    if (i == insert_index) {
      // 在这个位置插入新元素
      temp_keys[i] = key;
      temp_values[i] = new_node->GetPageId();
    } else {
      // 从原数组复制元素
      // j必须小于parent_size，因为原数组只有parent_size个元素
      if (j < parent_size) {
        temp_keys[i] = parent_node->KeyAt(j);
        temp_values[i] = parent_node->ValueAt(j);
        j++;
      }
    }
  }
  
  // 分裂内部节点
  auto new_internal_node = Split(parent_node);
  
  // 计算分裂点
  int split_index = (parent_size + 1) / 2;
  
  // 前半部分留在原节点
  for (int i = 0; i < split_index; i++) {
    parent_node->SetKeyAt(i, temp_keys[i]);
    parent_node->SetValueAt(i, temp_values[i]);
  }
  parent_node->SetSize(split_index);
  
  // 后半部分移到新节点（注意：内部节点的第一个key是无效的）
  new_internal_node->SetValueAt(0, temp_values[split_index]);
  for (int i = split_index + 1, k = 1; i <= parent_size; i++, k++) {
    new_internal_node->SetKeyAt(k, temp_keys[i]);
    new_internal_node->SetValueAt(k, temp_values[i]);
  }
  new_internal_node->SetSize(parent_size + 1 - split_index);
  
  // 更新子节点的父指针
  for (int i = 0; i < new_internal_node->GetSize(); i++) {
    auto child_page_id = new_internal_node->ValueAt(i);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(new_internal_node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
  
  // 清理临时数组
  delete[] temp_keys;
  delete[] temp_values;
  
  // 将中间键提升到更上层的父节点
  auto push_up_key = temp_keys[split_index];
  InsertIntoParent(parent_node, push_up_key, new_internal_node, transaction);
  
  buffer_pool_manager_->UnpinPage(parent_id, true);
  buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true);
}

/*
 * Split internal page and return new page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(InternalPage *node) -> InternalPage * {
  // 创建新页面
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  
  // 初始化新内部节点
  auto new_internal_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_node->Init(internal_max_size_);
  new_internal_node->SetPageId(new_page_id);
  new_internal_node->SetParentPageId(node->GetParentPageId());
  
  return new_internal_node;
}

/*
 * Delete key from leaf node
 * Return true if deletion is successful
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteFromLeaf(LeafPage *node, const KeyType &key) -> bool {
  int size = node->GetSize();
  
  // 找到要删除的键的位置
  int delete_index = -1;
  for (int i = 0; i < size; i++) {
    if (comparator_(node->KeyAt(i), key) == 0) {
      delete_index = i;
      break;
    }
  }
  
  // 如果没找到键，返回false
  if (delete_index == -1) {
    return false;
  }
  
  // 移动后面的元素来覆盖被删除的元素
  for (int i = delete_index; i < size - 1; i++) {
    node->SetKeyAt(i, node->KeyAt(i + 1));
    node->SetValueAt(i, node->ValueAt(i + 1));
  }
  
  // 减少size
  node->IncreaseSize(-1);
  
  return true;
}

/*
 * Handle redistribution or merge after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, Transaction *transaction) -> bool {
  // 如果是根节点，特殊处理
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  
  // 检查是否需要合并或重新分配（节点大小小于最小值）
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  
  // 获取父节点
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 找到当前节点在父节点中的索引
  int index = 0;
  for (int i = 0; i < parent_node->GetSize(); i++) {
    if (parent_node->ValueAt(i) == node->GetPageId()) {
      index = i;
      break;
    }
  }
  
  // 尝试从兄弟节点借位或合并
  bool should_delete = false;
  if (index > 0) {
    // 有左兄弟
    auto sibling_page_id = parent_node->ValueAt(index - 1);
    auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    auto sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
    
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      // 从左兄弟借位
      Redistribute(sibling_node, node, parent_node, index, true);
    } else {
      // 与左兄弟合并
      should_delete = Coalesce(sibling_node, node, parent_node, index, transaction);
    }
    
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
  } else if (index < parent_node->GetSize() - 1) {
    // 有右兄弟
    auto sibling_page_id = parent_node->ValueAt(index + 1);
    auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    auto sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
    
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      // 从右兄弟借位
      Redistribute(sibling_node, node, parent_node, index + 1, false);
    } else {
      // 与右兄弟合并
      should_delete = Coalesce(node, sibling_node, parent_node, index + 1, transaction);
    }
    
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
  }
  
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  
  return should_delete;
}

/*
 * Adjust root after deletion
 * Return true if root is deleted
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node) -> bool {
  // 情况1：根节点不是叶子节点且只有一个子节点
  if (!node->IsLeafPage() && node->GetSize() == 1) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto child_page_id = internal_node->ValueAt(0);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    
    // 让子节点成为新的根节点
    child_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    
    buffer_pool_manager_->UnpinPage(child_page_id, true);
    
    // 删除旧的根节点
    buffer_pool_manager_->DeletePage(node->GetPageId());
    return true;
  }
  
  // 情况2：根节点是叶子节点且为空
  if (node->IsLeafPage() && node->GetSize() == 0) {
    // 树变为空
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    
    // 删除根节点
    buffer_pool_manager_->DeletePage(node->GetPageId());
    return true;
  }
  
  return false;
}

/*
 * Redistribute key & value pairs from neighbor to node
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(BPlusTreePage *neighbor_node, BPlusTreePage *node, 
                                  InternalPage *parent, int index, bool from_left) -> void {
  if (node->IsLeafPage()) {
    // 处理叶子节点的重新分配
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    
    if (from_left) {
      // 从左兄弟借最后一个元素
      int neighbor_size = neighbor_leaf->GetSize();
      
      // 在当前节点的开头插入空间
      leaf_node->IncreaseSize(1);
      for (int i = leaf_node->GetSize() - 1; i > 0; i--) {
        leaf_node->SetKeyAt(i, leaf_node->KeyAt(i - 1));
        leaf_node->SetValueAt(i, leaf_node->ValueAt(i - 1));
      }
      
      // 从左兄弟移动最后一个元素
      leaf_node->SetKeyAt(0, neighbor_leaf->KeyAt(neighbor_size - 1));
      leaf_node->SetValueAt(0, neighbor_leaf->ValueAt(neighbor_size - 1));
      neighbor_leaf->IncreaseSize(-1);
      
      // 更新父节点的键
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    } else {
      // 从右兄弟借第一个元素
      int node_size = leaf_node->GetSize();
      
      // 从右兄弟移动第一个元素到当前节点末尾
      leaf_node->SetKeyAt(node_size, neighbor_leaf->KeyAt(0));
      leaf_node->SetValueAt(node_size, neighbor_leaf->ValueAt(0));
      leaf_node->IncreaseSize(1);
      
      // 在右兄弟中删除第一个元素
      int neighbor_size = neighbor_leaf->GetSize();
      for (int i = 0; i < neighbor_size - 1; i++) {
        neighbor_leaf->SetKeyAt(i, neighbor_leaf->KeyAt(i + 1));
        neighbor_leaf->SetValueAt(i, neighbor_leaf->ValueAt(i + 1));
      }
      neighbor_leaf->IncreaseSize(-1);
      
      // 更新父节点的键
      parent->SetKeyAt(index, neighbor_leaf->KeyAt(0));
    }
  } else {
    // 处理内部节点的重新分配
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    
    if (from_left) {
      // 从左兄弟借最后一个元素
      int neighbor_size = neighbor_internal->GetSize();
      
      // 在当前节点的开头插入空间
      internal_node->IncreaseSize(1);
      for (int i = internal_node->GetSize() - 1; i > 0; i--) {
        internal_node->SetKeyAt(i, internal_node->KeyAt(i - 1));
        internal_node->SetValueAt(i, internal_node->ValueAt(i - 1));
      }
      
      // 将父节点的键下移到当前节点
      internal_node->SetKeyAt(1, parent->KeyAt(index));
      internal_node->SetValueAt(0, neighbor_internal->ValueAt(neighbor_size - 1));
      
      // 更新子节点的父指针
      auto child_page_id = internal_node->ValueAt(0);
      auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
      auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(internal_node->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page_id, true);
      
      // 将左兄弟的最后一个键上移到父节点
      parent->SetKeyAt(index, neighbor_internal->KeyAt(neighbor_size - 1));
      neighbor_internal->IncreaseSize(-1);
    } else {
      // 从右兄弟借第一个元素
      int node_size = internal_node->GetSize();
      
      // 将父节点的键下移到当前节点末尾
      internal_node->SetKeyAt(node_size, parent->KeyAt(index));
      internal_node->SetValueAt(node_size, neighbor_internal->ValueAt(0));
      internal_node->IncreaseSize(1);
      
      // 更新子节点的父指针
      auto child_page_id = neighbor_internal->ValueAt(0);
      auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
      auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(internal_node->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page_id, true);
      
      // 将右兄弟的第一个键上移到父节点
      parent->SetKeyAt(index, neighbor_internal->KeyAt(1));
      
      // 在右兄弟中删除第一个元素
      int neighbor_size = neighbor_internal->GetSize();
      for (int i = 0; i < neighbor_size - 1; i++) {
        if (i > 0) {
          neighbor_internal->SetKeyAt(i, neighbor_internal->KeyAt(i + 1));
        }
        neighbor_internal->SetValueAt(i, neighbor_internal->ValueAt(i + 1));
      }
      neighbor_internal->IncreaseSize(-1);
    }
  }
}

/*
 * Coalesce (merge) two nodes
 * Return true if parent node should be deleted
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Coalesce(BPlusTreePage *left_node, BPlusTreePage *right_node, 
                              InternalPage *parent, int index, Transaction *transaction) -> bool {
  if (left_node->IsLeafPage()) {
    // 处理叶子节点的合并
    auto left_leaf = reinterpret_cast<LeafPage *>(left_node);
    auto right_leaf = reinterpret_cast<LeafPage *>(right_node);
    
    // 将右节点的所有元素移动到左节点
    int left_size = left_leaf->GetSize();
    int right_size = right_leaf->GetSize();
    
    for (int i = 0; i < right_size; i++) {
      left_leaf->SetKeyAt(left_size + i, right_leaf->KeyAt(i));
      left_leaf->SetValueAt(left_size + i, right_leaf->ValueAt(i));
    }
    left_leaf->IncreaseSize(right_size);
    
    // 更新叶子节点的链表指针
    left_leaf->SetNextPageId(right_leaf->GetNextPageId());
    
    // 删除右节点
    buffer_pool_manager_->DeletePage(right_node->GetPageId());
  } else {
    // 处理内部节点的合并
    auto left_internal = reinterpret_cast<InternalPage *>(left_node);
    auto right_internal = reinterpret_cast<InternalPage *>(right_node);
    
    // 将父节点的键下移到左节点
    int left_size = left_internal->GetSize();
    left_internal->SetKeyAt(left_size, parent->KeyAt(index));
    left_internal->SetValueAt(left_size, right_internal->ValueAt(0));
    left_internal->IncreaseSize(1);
    
    // 更新第一个子节点的父指针
    auto child_page_id = right_internal->ValueAt(0);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(left_internal->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
    
    // 将右节点的所有元素移动到左节点
    int right_size = right_internal->GetSize();
    for (int i = 1; i < right_size; i++) {
      left_internal->SetKeyAt(left_size + i, right_internal->KeyAt(i));
      left_internal->SetValueAt(left_size + i, right_internal->ValueAt(i));
      
      // 更新子节点的父指针
      child_page_id = right_internal->ValueAt(i);
      child_page = buffer_pool_manager_->FetchPage(child_page_id);
      child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(left_internal->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page_id, true);
    }
    left_internal->IncreaseSize(right_size - 1);
    
    // 删除右节点
    buffer_pool_manager_->DeletePage(right_node->GetPageId());
  }
  
  // 从父节点删除键
  DeleteFromInternal(parent, index);
  
  // 检查父节点是否需要合并或重新分配
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Delete key from internal node
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteFromInternal(InternalPage *node, int index) -> void {
  int size = node->GetSize();
  
  // 移动后面的元素
  for (int i = index; i < size - 1; i++) {
    node->SetKeyAt(i, node->KeyAt(i + 1));
    node->SetValueAt(i, node->ValueAt(i + 1));
  }
  
  // 减少size
  node->IncreaseSize(-1);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

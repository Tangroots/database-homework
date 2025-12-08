//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  /**
   * explicit:
   * In C++, the explicit keyword is used in the declaration of a constructor to prevent implicit conversions or
   * copy-initialization.
   */
  explicit TrieNode(char key_char) {
    key_char_ = key_char;
    is_end_ = false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  /**
   * noexcept:
   * noexcept is a keyword in C++ that is used to indicate that a function will not throw any exception.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_ = std::move(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return (children_.find(key_char) != children_.end()); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return (!children_.empty()); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  /**
   * unique_ptr:
   * std::unique_ptr is a smart pointer that owns and manages another object through a pointer
   * and disposes of that object when the unique_ptr goes out of scope.
   * unique_ptr不能拷贝，但可以移动
   * unique_ptr保存一个指针，当对象被销毁时，它使用delete表达式来释放关联的对象
   */
  /**
   * Lvalue
   * Definition:
   * The term "lvalue" stands for "locator value."
   * An lvalue refers to an object that occupies some identifiable location in memory (i.e., it has an address).
   * Characteristics:
   * Can be on the left-hand side of an assignment.
   * Typically represents objects that persist beyond a single expression. For example, variables.
   * You can take the address of an lvalue using the & operator.
   * Rvalue
   * Definition:
   * The term "rvalue" is often thought to stand for "right value."
   * Rvalues are temporary values that do not persist beyond the expression that uses them.
   * Characteristics:
   * Can be on the right-hand side of an assignment.
   * Cannot have a persistent address in memory (not directly addressable).
   * Includes literals (like 5, 'a'), non-lvalue expressions (like 2 + 2), and temporary objects.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (children_.find(key_char) != children_.end()) {
      return nullptr;
    }
    if (child->key_char_ != key_char) {
      return nullptr;
    }
    children_[key_char] = std::move(child);
    /**
     * Why Return a Pointer to a unique_ptr?
     * Returning a pointer to a unique_ptr (rather than the unique_ptr itself) can be useful for several reasons:
     * 1.No Ownership Transfer: It allows the function to return a reference to the node without transferring ownership
     * out of the children_ map. The unique_ptr in the map still owns the TrieNode, but the caller can access the node
     * through the pointer. 2.Error Handling: It enables the function to return nullptr in case of an error (like if the
     * key already exists or if there's a key mismatch), which is a common pattern in C++ for indicating failure in
     * functions that otherwise return pointers.
     */
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (children_.find(key_char) != children_.end()) {
      return &children_[key_char];
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.find(key_char) == children_.end()) {
      return;
    }
    children_.erase(key_char);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  /*

  */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)) {
    value_ = value;
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    value_ = value;
    is_end_ = true;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  /**
   * directly use std::unique_ptr<TrieNode>
   * root_ = std::unique_ptr<TrieNode>(new TrieNode('\0'))
   * use std::make_unique
   * root_ = std::make_unique<TrieNode>('\0');
   * Why std::make_unique is Preferred
   * Exception Safety:
   * std::make_unique provides stronger exception safety.
   * If an exception is thrown during the allocation or construction of the object, std::make_unique ensures that there
   * are no memory leaks. This is particularly important in more complex expressions where multiple allocations might
   * occur. Simplicity and Readability: std::make_unique results in shorter, more readable code. It also avoids the need
   * to use the new keyword directly, which aligns with modern C++ practices of avoiding direct memory management.
   */
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    latch_.WLock();
    if (key.empty()) {
      latch_.WUnlock();
      return false;
    }
    auto length = key.length();
    auto test_node = root_.get();
    // Traverse the trie until the second last character
    for (size_t i = 0; i < length - 1; i++) {
      if (test_node->HasChild(key[i])) {
        test_node = test_node->GetChildNode(key[i])->get();
      } else {
        test_node = test_node->InsertChildNode(key[i], std::make_unique<TrieNode>(key[i]))->get();
      }
    }
    // Check the last character
    if (test_node->HasChild(key[length - 1])) {
      // 这里，last_node_ptr 是一个引用，指向 test_node 的子节点中与 key[length - 1] 对应的 std::unique_ptr<TrieNode>。
      auto last_node_ptr = test_node->GetChildNode(key[length - 1]);
      if ((*last_node_ptr)->IsEndNode()) {
        // If the last node is TrieNodeWithValue, return false
        latch_.WUnlock();
        return false;
      }
      // Convert TrieNode to TrieNodeWithValue
      auto temp_node = std::move(*last_node_ptr);
      test_node->RemoveChildNode(key[length - 1]);
      auto temp = std::make_unique<TrieNodeWithValue<T>>(std::move(*temp_node), value);
      test_node->InsertChildNode(key[length - 1], std::move(temp));
      latch_.WUnlock();
      return true;
    }
    test_node->InsertChildNode(key[length - 1], std::make_unique<TrieNodeWithValue<T>>(key[length - 1], value));
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    latch_.WLock();
    if (key.empty()) {
      latch_.WUnlock();
      return false;
    }
    auto length = key.length();
    auto test_node = root_.get();
    std::stack<TrieNode *> my_stack;
    my_stack.push(test_node);
    // Traverse the trie until the last character
    for (size_t i = 0; i < length; i++) {
      if (test_node->HasChild(key[i])) {
        test_node = test_node->GetChildNode(key[i])->get();
        my_stack.push(test_node);
      } else {
        latch_.WUnlock();
        return false;
      }
    }
    // 最后一个节点是否是终止节点。如果不是终止节点，意味着该键不存在于 Trie 中.
    if (!test_node->IsEndNode()) {
      latch_.WUnlock();
      return false;
    }
    // Set the end node to false
    test_node->SetEndNode(false);
    // 删除所有孤立的节点
    /*for (size_t j = length - 1; j >= 0 ; j--) {
      //TrieNode *current_node = my_stack.top();
      my_stack.pop();
      if ((!my_stack.top()->HasChildren()) && (!my_stack.top()->IsEndNode())) {
        if (!my_stack.empty()) {
          my_stack.top()->RemoveChildNode(key[j]);
        }
      } else {
        break;
      }
    }*/
    for (size_t j = length - 1; j >= 0; j--) {
      // TrieNode *current_node = my_stack.top();
      // my_stack.pop();
      if ((!my_stack.top()->HasChildren()) && (!my_stack.top()->IsEndNode())) {
        my_stack.pop();
        if (my_stack.empty()) {
          break;
        }
        my_stack.top()->RemoveChildNode(key[j]);
      } else {
        break;
      }
    }
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  /**
   *GetValue function should acquire a read lock over root node (by calling RwLatch's RLock method)
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    latch_.RLock();
    // If key is empty, set success to false.
    if (key.empty()) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    // If key does not exist in trie, set success to false.
    auto length = key.length();
    auto test_node = root_.get();
    // key does not match
    for (size_t i = 0; i < length; i++) {
      if (test_node->HasChild(key[i])) {
        test_node = test_node->GetChildNode(key[i])->get();
      } else {
        *success = false;
        latch_.RUnlock();
        return {};
      }
    }
    // the last char in key in the Trie is not the end node
    if (!test_node->IsEndNode()) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    // If the given type T is not the same as the value type stored in TrieNodeWithValue
    if (dynamic_cast<TrieNodeWithValue<T> *>(test_node) == nullptr) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    *success = true;
    latch_.RUnlock();
    return dynamic_cast<TrieNodeWithValue<T> *>(test_node)->GetValue();
  }
};
}  // namespace bustub

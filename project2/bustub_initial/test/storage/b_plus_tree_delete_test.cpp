//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <numeric>
#include <random>
#include <string>
#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    auto ridssize = rids.size();
    EXPECT_EQ(ridssize, 1);
    int64_t value = key & 0xFFFFFFFF;
    auto ridslot = rids[0].GetSlotNum();
    EXPECT_EQ(ridslot, value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      auto find = std::find(remove_keys.begin(), remove_keys.end(), key);
      auto end = remove_keys.end();
      EXPECT_NE(find, end);
    } else {
      auto ridssize = rids.size();
      EXPECT_EQ(ridssize, 1);
      auto ridspageid = rids[0].GetPageId();
      EXPECT_EQ(ridspageid, 0);
      auto ridsslotnum = rids[0].GetSlotNum();
      EXPECT_EQ(ridsslotnum, key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    auto ridssize = rids.size();
    EXPECT_EQ(ridssize, 1);

    int64_t value = key & 0xFFFFFFFF;
    auto ridslot = rids[0].GetSlotNum();
    EXPECT_EQ(ridslot, value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      auto find = std::find(remove_keys.begin(), remove_keys.end(), key);
      auto end = remove_keys.end();
      EXPECT_NE(find, end);
    } else {
      auto ridssize = rids.size();
      EXPECT_EQ(ridssize, 1);
      auto ridspageid = rids[0].GetPageId();
      EXPECT_EQ(ridspageid, 0);
      auto ridsslotnum = rids[0].GetSlotNum();
      EXPECT_EQ(ridsslotnum, key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest4) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteTest5) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  auto key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  bool checkallpin;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    checkallpin = bpm->CheckAllUnpined();
    ASSERT_TRUE(checkallpin);
  }

  checkallpin = bpm->CheckAllUnpined();
  ASSERT_TRUE(checkallpin);
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    auto ridssize = rids.size();
    EXPECT_EQ(ridssize, 1);

    int64_t value = key & 0xFFFFFFFF;
    auto ridslot = rids[0].GetSlotNum();
    EXPECT_EQ(ridslot, value);
  }
  checkallpin = bpm->CheckAllUnpined();
  ASSERT_TRUE(checkallpin);
  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  ASSERT_TRUE(bpm->CheckAllUnpined());
  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);
  ASSERT_TRUE(bpm->CheckAllUnpined());
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteTest6) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);
  ASSERT_TRUE(bpm->CheckAllUnpined());
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeDeleteTests, DeleteBasic7) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  auto key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/DeleteBasic7Insert.dot");
  auto check = tree.Check(true);
  ASSERT_TRUE(check);
  // std::cout<<"insert done"<<std::endl;

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);
  // std::cout<<"check done"<<std::endl;

  int64_t start_key = keys[0];
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    ++current_key;
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);
  EXPECT_EQ(current_key, keys.size() + start_key);
  // std::cout<<"iterator done"<<std::endl;

  std::vector<int64_t> remove_keys = {2, 5, 3, 1, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    // std::cout<<"remove :"<<index_key<<std::endl;
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);
  // all gone
  rids.clear();
  for (auto key : remove_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 0);
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteScale8) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<64> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(10, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<64>, RID, GenericComparator<64>> tree("foo_pk", bpm, comparator);
  GenericKey<64> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int scale = 100;
  std::vector<int64_t> keys;
  for (int i = 0; i < scale; ++i) {
    keys.push_back(i + 1);
  }

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    std::cout << "insert :" << key << std::endl;
    tree.Insert(index_key, rid, transaction);
    // std::cout<<tree.ToString()<<endl;
  }

  // check all value is in the tree
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  // range query
  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  // delete all
  for (auto key : keys) {
    index_key.SetFromInteger(key);
    // std::cout<<"remove :"<<index_key<<std::endl;
    tree.Remove(index_key, transaction);
    // std::cout<<tree.ToString()<<endl;
  }

  // check all value is in the tree
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 0);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeDeleteTests, DeleteRandom9) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<64> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<64>, RID, GenericComparator<64>> tree("foo_pk", bpm, comparator);
  GenericKey<64> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  tree.open_check_ = false;
  std::vector<int64_t> keys;
  int scale = 10000;
  for (int i = 0; i < scale; ++i) {
    keys.push_back(i + 1);
  }
  // Create a random number engine seeded with a random value or a fixed seed
  std::random_device
      rd;  // Non-deterministic random number generator to obtain a seed (use a fixed seed for reproducible results)
  std::default_random_engine rng(rd());  // Random number engine seeded with rd()

  // Shuffle the elements using std::shuffle
  std::shuffle(keys.begin(), keys.end(), rng);

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  //  ASSERT_TRUE(tree.Check(true));

  // Shuffle the elements using std::shuffle
  std::shuffle(keys.begin(), keys.end(), rng);
  // delete all
  for (auto key : keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  ASSERT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteTest10) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 4);
  GenericKey<8> index_key{};
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // DONE(wxx)  修改这里测试
  int size = 12;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  int cnt = 0;
  keys = {2, 4, 15, 3, 7, 16, 18, 22, 20, 25, 11, 13};
  for (auto key : keys) {
    cnt++;
    std::cout << "insert" << ' ' << key << ' ' << "at index" << ' ' << cnt << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  bool check = tree.Check(true);
  ASSERT_TRUE(check);
  std::vector<RID> rids;

  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);

  // remove key 15,16
  std::vector<int64_t> removes(2);
  removes = {15, 16};
  for (auto key : removes) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
    check = tree.Check(true);
    ASSERT_TRUE(check);
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);

  // insert key 8,26
  std::vector<int64_t> adds(2);
  adds = {8, 26};
  for (auto key : adds) {
    cnt++;
    std::cout << "insert" << ' ' << key << ' ' << "at index" << ' ' << cnt << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);

  /*
   * Example: Deletion 1
   * delete “4” from	the index
   * Removing “4” doesn’t cause	node to	be under-full, so we’re	done.
   * */
  std::vector<int64_t> dots(1);
  dots = {4};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);

  /*
   * Example: Deletion 2
   * Delete at Leaf: Redistribute
   * delete “20” from the index
   * The leaf-node is now under-full!
   * Move an entry from	sibling	into the under-full node:
   * Parent-node entry is clearly wrong	now
   * Given a pair of sibling leaf-nodes	N and N’:
   * N is the immediate	predecessor to N'
   * Redistributing values between N and N’
   * Either moving a value from	N to N’, or from N’ to N
   * In	parent,	replace	key between N and N’ with N’.K0
   * */
  dots = {20};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  check = tree.Check(true);
  ASSERT_TRUE(check);

  /*
   * Example: Deletion 3
   * Delete at Leaf: Coalesce
   * delete “7” from the index
   * The leaf-node is now under-full!
   * Coalesce the under-full node with its sibling
   * Clearly need to modify parent-node	contents
   * No	longer need “7”	entry, or pointer to deleted node
   * */
  dots = {7};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  ASSERT_TRUE(tree.Check(true));
  std::vector<int64_t> final(10);
  final = {2, 8, 3, 26, 18, 22, 25, 11, 13};
  std::shuffle(final.begin(), final.end(), g);
  for (auto key : final) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }

  EXPECT_EQ(true, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteTest11) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 4);
  GenericKey<8> index_key{};
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // DONE(wxx)  修改这里测试
  int size = 12;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  int cnt = 0;
  keys = {2, 4, 15, 3, 7, 16, 18, 22, 20, 25, 11, 12};
  for (auto key : keys) {
    cnt++;
    std::cout << "insert" << ' ' << key << ' ' << "at index" << ' ' << cnt << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  std::vector<RID> rids;
  EXPECT_EQ(false, tree.IsEmpty());
  bool check = tree.Check(true);
  ASSERT_TRUE(check);
  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  // remove key 22,25
  std::vector<int64_t> removes(2);
  removes = {22, 25};
  for (auto key : removes) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  // insert key 13,14,8
  std::vector<int64_t> adds(3);
  adds = {13, 14, 8};
  for (auto key : adds) {
    cnt++;
    std::cout << "insert" << ' ' << key << ' ' << "at index" << ' ' << cnt << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  // remove key 4
  std::vector<int64_t> removels(1);
  removels = {4};
  for (auto key : removels) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  /*
   * Example: Deletion 4
   * Deletes at	Internal Nodes
   * delete “16” from the index
   * Leaf node becomes too empty, but it has a sibling
   * Can’t redistribute	values:	 sibling doesn’t have enough
   * Coalesce node with	its sibling (we	know how to do that)
   * After deleting “16”, coalescing leaf-nodes, and removing intervening key and pointer:
   * Problem: now internal node	is too empty For n = 4,	internal nodes must have at least 2 pointers
   * Can’t coalesce in this situation:
   * Left sibling already has 4	pointers
   * Can only redistribute values
   * In	this situation,	would like right sibling to point to both “13/14” leaf,
   * and “15/18/20” leaf
   * Can move rightmost	pointer	in left	node to	right node
   * Can’t move	“13” straight across to right sibling!
   * Right sibling should get “15” as	its key
   * Move “13”	to parent node,	“15” to right sibling
   * General principle:
   * When redistributing pointers between internal nodes,
   * keys must be rotated through the parent node
   * */
  std::vector<int64_t> dots(1);
  dots = {16};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  /*
   * Example: Deletion 5
   * Coalesce at Internal Nodes
   * After also	deleting “18”:
   * Next, delete “14”
   * Causes leaf node to become	too empty…
   * Need to coalesce leaf nodes; handle as usual
   * Could redistribute	from left sibling as before, but this time we can coalesce the two internal nodes together
   * As	before,	the two	internal nodes being coalesced are separated by	a key in the parent node
   * When coalescing internal nodes’ contents, use key from parent to separate	the combined contents
   * Also as before, remove pointer to deleted node, and also remove the key that separated them:
   * Finally, if the root node only has	one pointer, we	don’t need it anymore Node pointed to by old root’s lone
   * pointer becomes the new root
   * */
  dots = {18};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }
  dots = {14};
  for (auto key : dots) {
    ++cnt;
    index_key.SetFromInteger(key);
    std::cout << "Removing" << ' ' << key << std::endl;
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "/home/zks/cmu15445/bustub/draw/" + std::to_string(cnt) + "key" + std::to_string(key) + ".dot");
  }

  EXPECT_EQ(false, tree.IsEmpty());
  check = tree.Check(true);
  ASSERT_TRUE(check);
  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub

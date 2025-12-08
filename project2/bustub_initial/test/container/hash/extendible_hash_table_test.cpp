/**
 * extendible_hash_test.cpp
 */

#include "container/hash/extendible_hash_table.h"
#include <memory>
#include <random>
#include <thread>  // NOLINT
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, GetNumBucketsTest1) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);

  table->Insert(0b0000100, 1);  // 04
  table->Insert(0b0001100, 2);  // 12
  table->Insert(0b0010000, 3);  // 16
  EXPECT_EQ(4, table->GetNumBuckets());

  table->Insert(0b1000000, 4);  // 64
  table->Insert(0b0011111, 5);  // 31
  table->Insert(0b0001010, 6);  // 10
  table->Insert(0b0110011, 7);  // 51
  EXPECT_EQ(4, table->GetNumBuckets());

  table->Insert(0b0001111, 8);   // 15
  table->Insert(0b0010010, 9);   // 18
  table->Insert(0b0010100, 10);  // 20
  EXPECT_EQ(7, table->GetNumBuckets());

  table->Insert(0b0000111, 11);  // 07
  table->Insert(0b0010111, 12);  // 23
  EXPECT_EQ(8, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, GetNumBucketsTest2) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(0b0000100, "01");  // 04
  table->Insert(0b0001100, "02");  // 12
  table->Insert(0b0010000, "03");  // 16
  EXPECT_EQ(4, table->GetNumBuckets());

  table->Insert(0b1000000, "04");  // 64
  table->Insert(0b0011111, "05");  // 31
  table->Insert(0b0001010, "06");  // 10
  table->Insert(0b0110011, "07");  // 51
  EXPECT_EQ(4, table->GetNumBuckets());

  table->Insert(0b0001111, "08");  // 15
  table->Insert(0b0010010, "09");  // 18
  table->Insert(0b0010100, "10");  // 20
  EXPECT_EQ(7, table->GetNumBuckets());

  table->Insert(0b0000111, "11");  // 07
  table->Insert(0b0010111, "12");  // 23
  EXPECT_EQ(8, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, RepeatedInsertsAndUpdates3) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);

  // Insert initial values
  table->Insert(1, 10);
  table->Insert(2, 20);
  EXPECT_EQ(1, table->GetNumBuckets());

  // Update existing values
  table->Insert(1, 100);
  table->Insert(2, 200);

  int value;
  EXPECT_TRUE(table->Find(1, value));
  EXPECT_EQ(100, value);
  EXPECT_TRUE(table->Find(2, value));
  EXPECT_EQ(200, value);
}

TEST(ExtendibleHashTableTest, RemovalsAndBucketCount4) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);

  table->Insert(1, 10);
  table->Insert(2, 20);
  table->Insert(3, 30);
  EXPECT_EQ(2, table->GetNumBuckets());  // Assuming split occurs

  // Remove a key and check the bucket count
  EXPECT_TRUE(table->Remove(1));
  EXPECT_EQ(2, table->GetNumBuckets());  // Bucket count should remain the same
}

TEST(ExtendibleHashTableTest, CollisionHandling5) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);

  // Insert values that cause collisions
  table->Insert(1, 10);
  table->Insert(17, 170);  // Assuming hash collision with key 1

  int value;
  EXPECT_TRUE(table->Find(1, value));
  EXPECT_EQ(10, value);
  EXPECT_TRUE(table->Find(17, value));
  EXPECT_EQ(170, value);
}

TEST(ExtendibleHashTableTest, SampleTest6) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTest, SampleTest7) {
  auto test = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  // insert several key/value pairs
  test->Insert(1, "a");
  test->Insert(2, "b");
  test->Insert(3, "c");
  test->Insert(4, "d");
  test->Insert(5, "e");
  test->Insert(6, "f");
  test->Insert(7, "g");
  test->Insert(8, "h");
  test->Insert(9, "i");
  EXPECT_EQ(2, test->GetLocalDepth(0));
  EXPECT_EQ(3, test->GetLocalDepth(1));
  EXPECT_EQ(2, test->GetLocalDepth(2));
  EXPECT_EQ(2, test->GetLocalDepth(3));

  // find test
  std::string result;
  test->Find(9, result);
  EXPECT_EQ("i", result);
  test->Find(8, result);
  EXPECT_EQ("h", result);
  test->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_EQ(0, test->Find(10, result));

  // delete test
  EXPECT_EQ(1, test->Remove(8));
  EXPECT_EQ(1, test->Remove(4));
  EXPECT_EQ(1, test->Remove(1));
  EXPECT_EQ(0, test->Remove(20));
}

TEST(ExtendibleHashTest, SampleTest8) {
  auto test = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  // insert several key/value pairs
  test->Insert(1, "a");
  test->Insert(2, "b");
  test->Insert(3, "c");
  test->Insert(4, "d");
  test->Insert(5, "e");
  test->Insert(6, "f");
  test->Insert(7, "g");
  test->Insert(8, "h");
  test->Insert(9, "i");
  EXPECT_EQ(2, test->GetLocalDepth(0));
  EXPECT_EQ(3, test->GetLocalDepth(1));
  EXPECT_EQ(2, test->GetLocalDepth(2));
  EXPECT_EQ(2, test->GetLocalDepth(3));

  // find test
  std::string result;
  test->Find(9, result);
  EXPECT_EQ("i", result);
  test->Find(8, result);
  EXPECT_EQ("h", result);
  test->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_EQ(0, test->Find(10, result));

  // delete test
  EXPECT_EQ(1, test->Remove(8));
  EXPECT_EQ(1, test->Remove(4));
  EXPECT_EQ(1, test->Remove(1));
  EXPECT_EQ(0, test->Remove(20));

  test->Insert(1, "a");
  test->Insert(2, "b");
  test->Insert(3, "c");
  test->Insert(4, "d");
  test->Insert(5, "e");
  test->Insert(6, "f");
  test->Insert(7, "g");
  test->Insert(8, "h");
  test->Insert(9, "i");

  test->Find(9, result);
  EXPECT_EQ("i", result);
  test->Find(8, result);
  EXPECT_EQ("h", result);
  test->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_EQ(0, test->Find(10, result));
}

TEST(ExtendibleHashTest, BasicDepthTest9) {
  auto test = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  // insert several key/value pairs
  test->Insert(6, "a");   // b'0110
  test->Insert(10, "b");  // b'1010
  test->Insert(14, "c");  // b'1110

  EXPECT_EQ(3, test->GetGlobalDepth());

  EXPECT_EQ(3, test->GetLocalDepth(2));
  EXPECT_EQ(3, test->GetLocalDepth(6));

  EXPECT_EQ(2, test->GetLocalDepth(0));
  EXPECT_EQ(1, test->GetLocalDepth(1));
  EXPECT_EQ(1, test->GetLocalDepth(3));
  EXPECT_EQ(2, test->GetLocalDepth(4));
  EXPECT_EQ(1, test->GetLocalDepth(5));
  EXPECT_EQ(1, test->GetLocalDepth(7));

  // four buckets in use
  EXPECT_EQ(4, test->GetNumBuckets());

  // insert more key/value pairs
  test->Insert(1, "d");
  test->Insert(3, "e");
  test->Insert(5, "f");

  EXPECT_EQ(5, test->GetNumBuckets());
  /*
  EXPECT_EQ(3, test->GetLocalDepth(1));
  EXPECT_EQ(3, test->GetLocalDepth(3));
  EXPECT_EQ(3, test->GetLocalDepth(5));
  */
  EXPECT_EQ(2, test->GetLocalDepth(1));
  EXPECT_EQ(2, test->GetLocalDepth(3));
  EXPECT_EQ(2, test->GetLocalDepth(5));
}

#define TEST_NUM 1000
#define BUCKET_SIZE 64
TEST(ExtendibleHashTest, BasicRandomTest10) {
  auto test = std::make_unique<ExtendibleHashTable<int, int>>(64);
  // insert
  int seed = time(nullptr);
  std::cerr << "seed: " << seed << std::endl;
  std::default_random_engine engine(seed);
  std::uniform_int_distribution<int> distribution(0, TEST_NUM);
  std::map<int, int> comparator;

  for (int i = 0; i < TEST_NUM; ++i) {
    auto item = distribution(engine);
    comparator[item] = item;
    // printf("%d,",item);
    test->Insert(item, item);
    // std::cerr << std::dec << item << std::hex << "( 0x" << item << " )" << std::endl;
  }
  // printf("\n");

  // compare result
  int value = 0;
  for (auto i : comparator) {
    test->Find(i.first, value);
    // printf("%d,%d\n",,i.first);
    EXPECT_EQ(i.first, value);
    // delete
    EXPECT_EQ(1, test->Remove(value));
    // find again will fail
    value = 0;
    EXPECT_EQ(0, test->Find(i.first, value));
  }
}

TEST(ExtendibleHashTest, LargeRandomInsertTest11) {
  auto test = std::make_unique<ExtendibleHashTable<int, int>>(10);
  int seed = 0;
  for (size_t i = 0; i < 100000; i++) {
    srand(time(0) + i);
    if (random() % 3) {
      test->Insert(seed, seed);
      seed++;
    } else {
      if (seed > 0) {
        int value;
        int x = random() % seed;
        EXPECT_EQ(true, test->Find(x, value));
        EXPECT_EQ(x, value);
      }
    }
  }
}

TEST(ExtendibleHashTest, RandomInsertAndDeleteTest12) {
  auto test = std::make_unique<ExtendibleHashTable<int, int>>(10);
  for (int i = 0; i < 1000; i++) {
    test->Insert(i, i);
  }
  unsigned int seed = time(NULL);
  for (int i = 0; i < 1000; i++) {
    srand(time(0) + i);
    if (rand_r(&seed) % 2 == 0) {
      test->Remove(i);
      int value;
      EXPECT_NE(test->Find(i, value), true);
    } else {
      test->Insert(i, i + 2);
      int value;
      EXPECT_EQ(test->Find(i, value), true);
      EXPECT_EQ(value, i + 2);
    }
  }
}

/*TEST(ExtendibleHashTableTest, BucketSplitStressTest) {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(1); // Small initial bucket size to force
splits

    // Keys designed to trigger splits
    std::vector<int> keys = {0, 1, 2, 4, 8, 16, 32, 64, 128, 256};
    for (int key : keys) {
        table->Insert(key, "test_value");
    }

    // Check if all keys are correctly inserted and can be retrieved
    for (int key : keys) {
        std::string value;
        EXPECT_TRUE(table->Find(key, value));
        EXPECT_EQ("test_value", value);
    }

    // Check the expected number of buckets after splits
    EXPECT_GE(table->GetNumBuckets(), keys.size());
}*/
TEST(ExtendibleHashTableTest, GetNumBucketsTest13) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(4, "a");
  table->Insert(12, "b");
  table->Insert(16, "c");
  EXPECT_EQ(4, table->GetNumBuckets());
  table->Insert(64, "d");
  table->Insert(31, "e");

  table->Insert(10, "f");
  table->Insert(51, "g");
  EXPECT_EQ(4, table->GetNumBuckets());
  table->Insert(15, "h");
  table->Insert(18, "i");
  table->Insert(20, "j");
  EXPECT_EQ(7, table->GetNumBuckets());
  table->Insert(7, "k");
  table->Insert(23, "l");

  EXPECT_EQ(8, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, InsertMultipleSplitTest14) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(15, "a");
  table->Insert(14, "b");
  table->Insert(23, "c");
  table->Insert(11, "d");
  table->Insert(9, "e");

  EXPECT_EQ(4, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(3, table->GetLocalDepth(3));
  EXPECT_EQ(3, table->GetLocalDepth(7));
}
TEST(ExtendibleHashTest, ConcurrentInsertTest15) {
  const int num_runs = 50;
  const int num_threads = 3;
  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto test = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &test]() { test->Insert(tid, tid); }));
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    EXPECT_EQ(test->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(test->Find(i, val));
      EXPECT_EQ(val, i);
    }
  }
}
TEST(ExtendibleHashTableTest, ConcurrentInsertTest16) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTest, ConcurrentRemoveTest17) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto test = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    std::vector<int> values{0, 10, 16, 32, 64};
    for (int value : values) {
      test->Insert(value, value);
    }

    EXPECT_EQ(test->GetGlobalDepth(), 6);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &test, &values]() {
        test->Remove(values[tid]);
        test->Insert(tid + 4, tid + 4);
      }));
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    EXPECT_EQ(test->GetGlobalDepth(), 6);
    int val;
    EXPECT_EQ(0, test->Find(0, val));
    EXPECT_EQ(1, test->Find(8, val));
    EXPECT_EQ(0, test->Find(16, val));
    EXPECT_EQ(0, test->Find(3, val));
    EXPECT_EQ(1, test->Find(4, val));
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFindTest18) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        int val;
        table->Insert(tid, tid);
        EXPECT_TRUE(table->Find(tid, val));
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFindTest19) {
  const int num_epochs = 10;
  const int num_threads = 10;
  const int num_insert = 10;

  // 运行num_epochs轮以保证结果有效性
  for (int run = 0; run < num_epochs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads_insert;  // 插入线程集合
    std::vector<std::thread> threads_find;    // 查找线程集合
    threads_insert.reserve(num_threads);
    threads_find.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      // 累计num_threads个线程，每个线程插入num_insert个pair
      threads_insert.emplace_back([tid, &table]() {
        for (int i = tid * num_insert; i < (tid + 1) * num_insert; i++) {
          table->Insert(i, i);
        }
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads_insert[i].join();
    }

    // 累计num_threads个线程，每个线程查找num_insert个pair
    for (int tid = 0; tid < num_threads; tid++) {
      threads_find.emplace_back([tid, &table]() {
        for (int i = tid * num_insert; i < (tid + 1) * num_insert; i++) {
          int val;
          EXPECT_TRUE(table->Find(i, val));
        }
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads_find[i].join();
    }
  }
}

TEST(ExtendibleHashTableTest, InsertSplit20) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  ASSERT_EQ(1, table->GetNumBuckets());
  ASSERT_EQ(0, table->GetLocalDepth(0));
  ASSERT_EQ(0, table->GetGlobalDepth());

  table->Insert(1, "a");
  table->Insert(2, "b");
  ASSERT_EQ(1, table->GetNumBuckets());
  ASSERT_EQ(0, table->GetLocalDepth(0));
  ASSERT_EQ(0, table->GetGlobalDepth());

  table->Insert(3, "c");  // first split
  ASSERT_EQ(2, table->GetNumBuckets());
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(1, table->GetGlobalDepth());
  table->Insert(4, "d");

  table->Insert(5, "e");  // second split
  ASSERT_EQ(3, table->GetNumBuckets());
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(1, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetGlobalDepth());

  table->Insert(6, "f");  // third split (global depth doesn't increase)
  ASSERT_EQ(4, table->GetNumBuckets());
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetGlobalDepth());

  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  ASSERT_EQ(5, table->GetNumBuckets());
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(3, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(3, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetGlobalDepth());

  // find table
  std::string result;
  table->Find(9, result);
  ASSERT_EQ("i", result);
  table->Find(8, result);
  ASSERT_EQ("h", result);
  table->Find(2, result);
  ASSERT_EQ("b", result);
  ASSERT_EQ(false, table->Find(10, result));

  // delete table
  ASSERT_EQ(true, table->Remove(8));
  ASSERT_EQ(true, table->Remove(4));
  ASSERT_EQ(true, table->Remove(1));
  ASSERT_EQ(false, table->Remove(20));
}

TEST(ExtendibleHashTableTest, InsertMultipleSplit21) {
  {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

    table->Insert(0, "0");
    table->Insert(1024, "1024");
    table->Insert(4, "4");  // this causes 3 splits

    ASSERT_EQ(4, table->GetNumBuckets());
    ASSERT_EQ(3, table->GetGlobalDepth());
    ASSERT_EQ(3, table->GetLocalDepth(0));
    ASSERT_EQ(1, table->GetLocalDepth(1));
    ASSERT_EQ(2, table->GetLocalDepth(2));
    ASSERT_EQ(1, table->GetLocalDepth(3));
    ASSERT_EQ(3, table->GetLocalDepth(4));
    ASSERT_EQ(1, table->GetLocalDepth(5));
    ASSERT_EQ(2, table->GetLocalDepth(6));
    ASSERT_EQ(1, table->GetLocalDepth(7));
  }
  {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

    table->Insert(0, "0");
    table->Insert(1024, "1024");
    table->Insert(16, "16");  // this causes 5 splits

    ASSERT_EQ(6, table->GetNumBuckets());
    ASSERT_EQ(5, table->GetGlobalDepth());
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFind22) {
  const int num_runs = 50;
  const int num_threads = 5;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        // for random number generation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, num_threads * 10);

        for (int i = 0; i < 10; i++) {
          table->Insert(tid * 10 + i, std::to_string(tid * 10 + i));

          // Run Find on random keys to let Thread Sanitizer check for race conditions
          std::string val;
          table->Find(dis(gen), val);
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int i = 0; i < num_threads * 10; i++) {
      std::string val;
      ASSERT_TRUE(table->Find(i, val));
      ASSERT_EQ(std::to_string(i), val);
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentRemoveInsert23) {
  const int num_threads = 5;
  const int num_runs = 50;

  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
    std::vector<std::thread> threads;
    std::vector<std::string> values;
    values.reserve(100);
    for (int i = 0; i < 100; i++) {
      values.push_back(std::to_string(i));
    }
    for (unsigned int i = 0; i < values.size(); i++) {
      table->Insert(i, values[i]);
    }

    threads.reserve(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        for (int i = tid * 20; i < tid * 20 + 20; i++) {
          table->Remove(i);
          table->Insert(i + 400, std::to_string(i + 400));
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    std::string val;
    for (int i = 0; i < 100; i++) {
      ASSERT_FALSE(table->Find(i, val));
    }
    for (int i = 400; i < 500; i++) {
      ASSERT_TRUE(table->Find(i, val));
      ASSERT_EQ(std::to_string(i), val);
    }
  }
}

TEST(ExtendibleHashTableTest, InitiallyEmpty24) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  ASSERT_EQ(0, table->GetGlobalDepth());
  ASSERT_EQ(0, table->GetLocalDepth(0));

  std::string result;
  ASSERT_FALSE(table->Find(1, result));
  ASSERT_FALSE(table->Find(0, result));
  ASSERT_FALSE(table->Find(-1, result));
}

TEST(ExtendibleHashTableTest, InsertAndFind25) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  std::string result;
  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(val[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(val[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(val[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(val[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(val[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(val[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_FALSE(table->Find(0, result));
  ASSERT_FALSE(table->Find(1, result));
  ASSERT_FALSE(table->Find(-1, result));
  ASSERT_FALSE(table->Find(2, result));
  ASSERT_FALSE(table->Find(3, result));
  for (int i = 65; i < 1000; i++) {
    ASSERT_FALSE(table->Find(i, result));
  }
}

TEST(ExtendibleHashTableTest, GlobalDepth26) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(0, table->GetGlobalDepth());

  // Inserting into another bucket
  table->Insert(5, val[5]);
  ASSERT_EQ(1, table->GetGlobalDepth());

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(2, table->GetGlobalDepth());

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(2, table->GetGlobalDepth());

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(3, table->GetGlobalDepth());

  // Inserting 2 keys into filled buckets with local depth < global depth
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  ASSERT_EQ(3, table->GetGlobalDepth());

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(3, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, LocalDepth27) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(0, table->GetLocalDepth(0));

  // Inserting into another bucket
  table->Insert(5, val[5]);
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(1, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(1, table->GetLocalDepth(7));

  // Inserting 2 keys into filled buckets with local depth < global depth
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(2, table->GetLocalDepth(7));

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(3, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(3, table->GetLocalDepth(7));
}

TEST(ExtendibleHashTableTest, InsertAndReplace28) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  std::vector<std::string> newval;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
    newval.push_back(std::to_string(i + 1));
  }

  std::string result;

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  table->Insert(4, newval[4]);
  table->Insert(12, newval[12]);
  table->Insert(16, newval[16]);
  table->Insert(64, newval[64]);
  table->Insert(5, newval[5]);
  table->Insert(10, newval[10]);
  table->Insert(51, newval[51]);
  table->Insert(15, newval[15]);
  table->Insert(18, newval[18]);
  table->Insert(20, newval[20]);
  table->Insert(7, newval[7]);
  table->Insert(21, newval[21]);
  table->Insert(11, newval[11]);
  table->Insert(19, newval[19]);

  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(newval[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(newval[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(newval[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(newval[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(newval[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(newval[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(newval[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(newval[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(newval[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(newval[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(newval[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(newval[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(newval[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(newval[19], result);
}

TEST(ExtendibleHashTableTest, Remove29) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  std::string result;

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  ASSERT_TRUE(table->Remove(4));
  ASSERT_TRUE(table->Remove(12));
  ASSERT_TRUE(table->Remove(16));
  ASSERT_TRUE(table->Remove(64));
  ASSERT_TRUE(table->Remove(5));
  ASSERT_TRUE(table->Remove(10));

  ASSERT_FALSE(table->Find(4, result));
  ASSERT_FALSE(table->Find(12, result));
  ASSERT_FALSE(table->Find(16, result));
  ASSERT_FALSE(table->Find(64, result));
  ASSERT_FALSE(table->Find(5, result));
  ASSERT_FALSE(table->Find(10, result));
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_TRUE(table->Remove(51));
  ASSERT_TRUE(table->Remove(15));
  ASSERT_TRUE(table->Remove(18));

  ASSERT_FALSE(table->Remove(5));
  ASSERT_FALSE(table->Remove(10));
  ASSERT_FALSE(table->Remove(51));
  ASSERT_FALSE(table->Remove(15));
  ASSERT_FALSE(table->Remove(18));

  ASSERT_TRUE(table->Remove(20));
  ASSERT_TRUE(table->Remove(7));
  ASSERT_TRUE(table->Remove(21));
  ASSERT_TRUE(table->Remove(11));
  ASSERT_TRUE(table->Remove(19));

  for (int i = 0; i < 1000; i++) {
    ASSERT_FALSE(table->Find(i, result));
  }

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(val[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(val[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(val[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(val[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(val[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(val[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(3, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(3, table->GetLocalDepth(7));
  ASSERT_EQ(3, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, GetNumBuckets30) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(4);

  std::vector<int> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(i);
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(1, table->GetNumBuckets());
  // Inserting into another bucket

  table->Insert(31, val[31]);
  ASSERT_EQ(2, table->GetNumBuckets());

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(4, table->GetNumBuckets());

  // Inserting 2 keys into filled buckets with local depth < global depth
  // Adding a new bucket and inserting will still be full so
  // will test if they add another bucket again.
  table->Insert(7, val[7]);
  table->Insert(23, val[21]);
  ASSERT_EQ(6, table->GetNumBuckets());

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(6, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, IntegratedTest31) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(7);

  std::vector<std::string> val;

  for (int i = 0; i <= 2000; i++) {
    val.push_back(std::to_string(i));
  }

  for (int i = 1; i <= 1000; i++) {
    table->Insert(i, val[i]);
  }

  int global_depth = table->GetGlobalDepth();
  ASSERT_EQ(8, global_depth);

  for (int i = 1; i <= 1000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 500; i++) {
    ASSERT_TRUE(table->Remove(i));
  }

  for (int i = 1; i <= 500; i++) {
    std::string result;
    ASSERT_FALSE(table->Find(i, result));
    ASSERT_FALSE(table->Remove(i));
  }

  for (int i = 501; i <= 1000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 2000; i++) {
    table->Insert(i, val[i]);
  }

  global_depth = table->GetGlobalDepth();
  ASSERT_EQ(9, global_depth);

  for (int i = 1; i <= 2000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 2000; i++) {
    ASSERT_TRUE(table->Remove(i));
  }

  for (int i = 1; i <= 2000; i++) {
    std::string result;
    ASSERT_FALSE(table->Find(i, result));
    ASSERT_FALSE(table->Remove(i));
  }
}

}  // namespace bustub

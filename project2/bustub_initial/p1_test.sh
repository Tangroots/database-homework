cd build
cmake ..

# make format
make format
make check-lint
make check-clang-tidy-p1

# RUN for build test p1
make extendible_hash_table_test
make lru_k_replacer_test
make buffer_pool_manager_instance_test
./test/extendible_hash_table_test
./test/lru_k_replacer_test
./test/buffer_pool_manager_instance_test
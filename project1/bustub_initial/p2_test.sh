cd build
cmake ..

# make format
make format
make check-lint
make check-clang-tidy-p2

# RUN for build test p2
make b_plus_tree_insert_test
make b_plus_tree_sequential_scale_test
make b_plus_tree_delete_test
make b_plus_tree_concurrent_test
./test/b_plus_tree_insert_test
./test/b_plus_tree_sequential_scale_test
./test/b_plus_tree_delete_test
./test/b_plus_tree_concurrent_test
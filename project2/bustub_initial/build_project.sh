mkdir build
cd build
cmake ..
make -j

# RUN for build test p0
make starter_trie_test  # build starter_trie_test
./test/starter_trie_test  # run starter_trie_test
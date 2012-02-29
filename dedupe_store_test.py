#!/usr/bin/env python

import unittest
from dedupe_store import FileHash

class TestFileHashes(unittest.TestCase):
    '''Test the file hashes out.'''
    # Ensure that the hashing is working as expected
    # Values generated from echo <val> | sha256sum on Ubuntu 11.10
    def setUp(self):
        self.test_sha256_hashes = [
            ('aaaaa\n', 'bdc26931acfb734b142a8d675f205becf27560dc461f501822de13274fe6fc8a'),
            ('bbbbb\n', '8b410a5102fa5a38ef71e9e7c3f7888a9c029da41cfce2b16fd6f4c062b88030')
        ]
        
        self.path_break_values = [
            ('aaaaaaaaaaaaaaaaaaaa', 4, 'aaaa/aaaa/aaaa/aaaa/aaaa'),
            ('aaaaaaaaaaaaaaaaaa', 4, 'aaaa/aaaa/aaaa/aaaa/aa'),
            ('a', 4, 'a'),
            ('aaaaaaaaaaaaaaaaaa', 100, 'aaaaaaaaaaaaaaaaaa'),
            ('aaaaa', 1, 'a/a/a/a/a'),
            ('aaaaaaaaaaaaaaaaaa', 10, 'aaaaaaaaaa/aaaaaaaa')
            
        ]

    def test_hashing_algorithms(self):
        
        file_hash = FileHash()
        for test_hash in self.test_sha256_hashes:
            self.assertEqual(file_hash.update(test_hash[0]).hash(),
                                              test_hash[1])
    
    
    def test_direct_assignment(self):
        for test_hash in self.test_sha256_hashes:
            file_hash = FileHash(test_hash[1])
            direct_hash = file_hash.hash()
            self.assertEqual(direct_hash, file_hash.update(test_hash[0]).hash())
            
    def test_str_representation(self):
        for test_hash in self.test_sha256_hashes:
            file_hash = FileHash(test_hash[1])
            self.assertEqual(str(file_hash), file_hash.hash())
            
    def test_str_representation(self):
        for test_hash in self.test_sha256_hashes:
            file_hash = FileHash(test_hash[1])
            self.assertEqual(str(file_hash), file_hash.hash())
            
    def test_path_split(self):
        for path_test in self.path_break_values:
            file_hash = FileHash(path_test[0])
            hash_path = file_hash.hash_path(path_break=path_test[1])
            self.assertEqual(hash_path, path_test[2])
 
    
if __name__ == '__main__':
    unittest.main()
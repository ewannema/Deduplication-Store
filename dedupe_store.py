#!/usr/bin/env python

'''Copyright 2012 Eric Wannemacher

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>'''

import logging
import os
import os.path
import sys
import getopt
import hashlib
import sqlite3


def usage():
    """Show the standard usage screen and exit."""
    print 'Usage:' + sys.argv[0] + ' <-r|--repository> location <command>'
    print ''
    print 'COMMANDS:'
    print ''
    print 'add <file1> <fileN>      add file(s) to the repository'
    #print 'validate                    check the repository for issues'
    print 'get <file1> <fileN>      get file(s) from the repository'
    print 'init                     initialize the repository'
    print 'list                     list files in the repository'
    print 'remove <file1> <fileN>   delete file(s) from the repository'
    print sys.exit(2)

class DedupeStore:
    """The main interface to the deduplication store."""
    def __init__(self, repository):
        logging.debug("Creating the deduplication store object.")
        self.repository = repository
        self.metadata_manager = MetadataManagerSqlite(self.repository)
        self.data_dir = os.path.join(self.repository, 'data')
                
        logging.info("The data directory is %s", self.data_dir)

        # data chunk size in bytes
        self.chunk_size = 1024*1024*10
        
    def run(self, args):
        """Call the appropriate command given a set of arguments."""
        command = args[0]
        
        logging.debug("Running the command %s", command)
        
        if command == 'init':
            self.metadata_manager.open(validate=False)
            self.init()
        else:
            self.metadata_manager.open(validate=True)
            if command == 'list':
                self.list()
            elif command == 'add':
                self.add(args)
            elif command == 'remove':
                self.remove(args)
            elif command == 'get':
                self.get(args)
            elif command == 'init':
                self.init()
            else:
                raise Exception('InvalidCommand')
    
        self.metadata_manager.close()
        
    def list(self):
        """List files in the store."""
        logging.debug("Listing files.")
        files = self.metadata_manager.list_file()
        files.sort()
        for file_name in files:
            print file_name
    
    def init(self):
        """Initialize the store."""
        logging.debug("Initializing the repository.")
            
        self.metadata_manager.create()
        
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

    def check_store(self):
        """Check that the store is healthy."""
        return self.metadata_manager.validate and True
    
    def add(self, args):
        """Add files to the store."""
        logging.debug("Adding files.")
        if len(args) > 1:
            files = args[1:]
        else:
            print 'No files passed to command: add.'
            raise Exception('InvalidCommand')
        
        for file_name in files:
            short_name = os.path.basename(file_name)
            if self.metadata_manager.file_exists(short_name):
                print "%s is already in the repository." % (short_name,)
                continue
            
            # Chunk the file
            with open(file_name,'rb') as source_file:
                data = source_file.read(self.chunk_size)
                file_hashes = []
                while data:
                    file_hash = FileHash()
                    file_hash.update(data)
                    hash_file = os.path.join(self.data_dir,
                                             file_hash.hash_path())
                    logging.debug("Adding %s", hash_file)
                   
                    # Add the hashed chunk to the datastore
                    if not os.path.exists(hash_file):
                        # Make any parent directories
                        try:
                            os.makedirs(os.path.dirname(hash_file))
                        except OSError as exc:
                            if exc.errno == os.errno.EEXIST:
                                pass
                            else:
                                raise
                        except Exception:
                            logging.exception('Unhandled exception in add.')
            
                        with open(hash_file, 'wb') as output:
                            output.write(data)
                    
                    file_hashes.append(file_hash.hash())
                    
                    # Work on the next chunk                    
                    data = source_file.read(self.chunk_size)
                
                logging.debug('Adding metadata for %d hashes.',
                              len(file_hashes))
                self.metadata_manager.add_file(short_name, file_hashes)
    
    def remove(self, args):
        """Remove files from the store."""
        if len(args) > 1:
            files = args[1:]
        else:
            print 'No files passed to command: remove.'
            raise Exception('InvalidCommand')
        
        for file_name in files:
            short_name = os.path.basename(file_name)
            if not self.metadata_manager.file_exists(short_name):
                print "%s is not in the repsitory." % (short_name,)
                continue
            hashes = self.metadata_manager.remove_file(short_name)
            
            for file_hash in hashes:
                file_hash = FileHash(file_hash)
                logging.debug("Need to remove %s", file_hash.hash_path())
                
                os.remove(os.path.join(self.data_dir, file_hash.hash_path()))
            
                #TODO: Remove all empty parent directories

    def get(self, args):
        """Get files from the store."""
        if len(args) > 1:
            files = args[1:]
        else:
            print 'No files passed to command: get.'
            raise Exception('InvalidCommand')
        
        for file_name in files:
            short_name = os.path.basename(file_name)
            hashes = self.metadata_manager.get_file(short_name)
            if hashes:
                with open(file_name, 'wb') as output:
                    for file_hash in hashes:
                        file_hash = FileHash(file_hash)
                        hash_file = os.path.join(self.data_dir,
                                                 file_hash.hash_path())
                        with open(hash_file, 'rb') as source_file:
                            output.write(source_file.read())
            else:
                print "File %s not found in the repository." % (short_name, )
                
class FileHash:
    """A helper for operations dealing with file hashes."""
    def __init__(self, file_hash=''):
        self.file_hash = file_hash
    
    def update(self, data):
        """Read data in and hash it using the appropriate algorithm."""
        hash_method = hashlib.sha256()
        hash_method.update(data)
        self.file_hash = hash_method.hexdigest()
        return self.file_hash
        
    def hash_path(self, path_break=4):
        """Return a path representing the hash."""
        path = ''
        for i in range(0, len(self.file_hash)-path_break, path_break):
            path = os.path.join(path, self.file_hash[i:i+path_break])
            
        return path
    
    def hash(self):
        """The hash for the file."""
        return self.file_hash
        
def main():
    """Where the fun begins."""
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       'hvdr:', ['help', 'repository='])
    except getopt.GetoptError, err:
        print str(err)
        usage()

    repository = ''
    
    for option, argument in opts:
        if option == '-v':
            logging.basicConfig(format='%(message)s',
                                level=logging.INFO)
        if option == '-d':
            logging.basicConfig(
                format='%(levelname)s::%(asctime)s::%(message)s',
                level=logging.DEBUG)
        elif option in ('-r', '--repository'):
            repository = argument
        elif option in ('-h', '--help'):
            usage()
                   
    if not repository:
        print 'A repository location must be specified.'
        usage()
        
    if len(args) < 1:
        print 'A command must be specified.'
        usage()
    
    dedupe_store = DedupeStore(repository)
    
    try:
        dedupe_store.run(args)
    except Exception, err:
        if err[0] == 'InvalidCommand':
            print 'Invalid command specified.'
            usage()
        if err[0] == 'InvalidMetadata':
            logging.error('The repository is invalid. Is it initialized?')
        else:
            logging.exception('UNHANDLED EXCEPTION')

class MetadataManagerSqlite:
    """An implementation of metadata manager that uses a sqlite3 backend."""
    
    def __init__(self, repository, dbname='metadata'):
        self.connection = None
        self.cursor = None
        self.dbname = os.path.join(repository, dbname)
        
    def open(self, validate=True):
        """Open the connection to the sqlite3 database"""
        logging.debug("Opening the metadata manager.")
        
        if validate:
            self.validate_path()

        self.connection = sqlite3.connect(self.dbname)
        self.connection.isolation_level = 'EXCLUSIVE'
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
            
        if validate:
            self.validate_schema()
            
    def close(self):
        """Close the connection to the sqlite3 database"""
        logging.debug("Closing the metadata manager")
        self.cursor.close()
        self.connection.close()
    
    def add_file(self, file_name, hashes):
        """Add a single file and its associated hashes to the database"""
        logging.debug('Adding metadata for %s.', file_name)
       
        try:
            self.cursor.execute('INSERT INTO files (file) VALUES (?)',
                                (file_name,))
            
            self.cursor.execute('SELECT id FROM files WHERE file=?',
                                (file_name,))
            
            file_id = self.cursor.fetchone()['id']
            
            hash_ids = []
            
            for file_hash in hashes:
                self.cursor.execute('''SELECT id
                                        FROM hashes
                                        WHERE hash=?''', (file_hash,))
                row = self.cursor.fetchone()
                if not row:
                    logging.debug('Existing hash not found. Adding it.')
                    self.cursor.execute('''INSERT INTO hashes
                                            (hash)
                                            VALUES (?)''', (file_hash,))
                    
                    self.cursor.execute('''SELECT id
                                            FROM hashes
                                            WHERE hash=?''',
                                            (file_hash,))
                    
                    row = self.cursor.fetchone()
                    
                hash_ids.append(row['id'])
            
            seq = 0
            for hash_id in hash_ids:
                self.cursor.execute('''INSERT INTO filemap
                                        (file, hash, sequence)
                                        VALUES (?,?,?)''',
                                        (file_id, hash_id, seq))
                seq += 1
        
            self.connection.commit()
        except sqlite3.IntegrityError:
            logging.debug("%s is already in the repository.", file_name)
        except Exception:
            logging.exception('Unhandled exception in add_file.')
        
        
    def file_exists(self, file_name):
        """Return True if the file exists in the repository, False otherwise."""
        
        logging.debug("Checking the existence of %s", file_name)
        try:
            self.cursor.execute('''SELECT id 
                                   FROM files
                                   WHERE file=?''', (file_name,))
            row = self.cursor.fetchone()
            if row:
                return True
            else:
                return False
            
        except Exception:
            logging.exception('Unhandled exception in file_exists.')
    
    def remove_file(self, file_name):
        """Remove a single file from the database.
        
        Any unreferenced hashes are also removed.
        A list of removed hashes is returned to the caller."""
        
        logging.debug("Removing the metadata for %s", file_name)
        try:
            self.cursor.execute('''SELECT id
                                    FROM files
                                    WHERE file=?''', (file_name,))
            row = self.cursor.fetchone()
            if row:
                file_id = row['id']
            else:
                return []
            
            self.cursor.execute('''DELETE FROM files
                                    WHERE id=?''', (file_id,))
            self.cursor.execute('''DELETE FROM filemap
                                    WHERE file=?''', (file_id,))
            
            # Determine hashes that are no longer used
            self.cursor.execute('''SELECT hashes.id AS hashid,
                                    hashes.hash AS hash
                                    FROM hashes
                                    LEFT JOIN filemap
                                    ON hashes.id=filemap.hash
                                    WHERE filemap.hash IS NULL''')
            
            
            rows = self.cursor.fetchall() 
            hashes = [x['hash'] for x in rows]
            hash_ids = [(x['hashid'],) for x in rows]
            self.cursor.executemany('''DELETE FROM hashes
                                        WHERE id=?''', hash_ids)
            self.connection.commit()
            return hashes
        
        except Exception:
            logging.exception('Unhandled exception in remove_file.')
    
    def get_file(self, file_name):
        """Get a list of hashes for the file.
        
        The hashes are returned in the order necessary to recreate the file."""
        
        logging.debug("Getting the metadata for %s", file_name)
        try:
            self.cursor.execute('''SELECT id 
                                   FROM files
                                   WHERE file=?''', (file_name,))
            row = self.cursor.fetchone()
            if row:
                file_id = row['id']
            else:
                return []
            
            self.cursor.execute('''SELECT hashes.hash AS hash
                                    FROM hashes
                                    INNER JOIN filemap
                                    ON hashes.id=filemap.hash
                                    WHERE file=?
                                    ORDER BY sequence''', (file_id,))
            
            rows = self.cursor.fetchall() 
            hashes = [x['hash'] for x in rows]
            self.connection.commit()
            return hashes
        
        except Exception:
            logging.exception('Unhandled exception in get_file.')
    
    def list_file(self):
        """Return a list of all files in the database."""
        
        try:
            self.cursor.execute('''select file from files''')
            return [x['file'] for x in self.cursor.fetchall()]
        except Exception:
            logging.exception('Unhandled exception in list_file.')
    
    def create(self):
        """Create the database on disk and populate the schema."""
        
        logging.debug("Creating the metadata store.")
            
        try:
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS hashes
                        (id INTEGER PRIMARY KEY,
                         hash TEXT UNIQUE NOT NULL)''')

            self.cursor.execute('''CREATE TABLE IF NOT EXISTS files
                        (id INTEGER PRIMARY KEY,
                         file TEXT UNIQUE NOT NULL)''')
    
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS filemap
                        (file INTEGER NOT NULL,
                         hash INTEGER NOT NULL,
                         sequence INTEGER NOT NULL,
                         FOREIGN KEY (file) REFERENCES files(id),
                         FOREIGN KEY (hash) REFERENCES hashes(id),
                         PRIMARY KEY (file, hash, sequence))''')

        except Exception:
            logging.exception('Unhandled exception in create.')

    def validate_path(self):
        """Validate the path to the database."""
        if not os.path.exists(self.dbname):
            logging.debug('The path %s does not exist.', self.dbname)
            raise Exception('InvalidMetadata')
            

    def validate_schema(self):
        """Validate the schema of the database."""
        return True
    
    def validate(self):
        """Validate everything about the metadata."""
        self.open()
        self.close()
    
    def upgrade(self):
        """Upgrade the database from a previous version."""
        pass
    
    def get_config(self):
        """Get the configuration information from the database."""
        return {'schema':'0.2'}
    
if __name__ == '__main__':
    main()

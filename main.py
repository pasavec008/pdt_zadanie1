import gzip
from time import time

#my modules
import code.database_init as database_init
import code.main_migration_authors as main_migration_authors
import code.main_migration_conversations as main_migration_conversations
import code.main_migration_references as main_migration_references
import code.add_constraints as add_constraints

def main():
    #Database setup
    start = time()
    conn = database_init.connect_to_database()
    print("Connected to database")
    database_init.create_tables(conn)
    print("Database and tables setup completed in time: ", time()-start)

    #first reading (author file)
    start = time()
    print("First reading (author file)")
    authors_file = gzip.open('data/authors.jsonl.gz')
    authors_hashmap = main_migration_authors.migration(conn, authors_file)
    authors_file.close()
    print("First reading completed in time: ", time()-start)

    #second reading (conv file)
    start = time()
    print("Second reading (conversations file)")
    conversations_file = gzip.open('data/conversations.jsonl.gz')
    conversations_hashmap = main_migration_conversations.migration(conn, conversations_file, authors_hashmap)
    authors_hashmap = None
    print("Second reading completed in time: ", time()-start)

    #third reading (conv file)
    start = time()
    print("Third reading (conversations file)")
    conversations_file.seek(0)
    main_migration_references.migration(conn, conversations_file, conversations_hashmap)
    conversations_hashmap = None
    conversations_file.close()
    print("Third reading completed in time: ", time()-start)

    #add constraints
    print("Adding constraints")
    add_constraints.add_constraints(conn)
    print("Constraints added in time: ", time()-start)
    
    conn.close()
    return

main()
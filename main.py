import gzip

#my modules
import code.database_init as database_init
import code.main_migration_authors as main_migration_authors
import code.main_migration_conversations as main_migration_conversations
import code.add_constraints as add_constraints

def main():
    conn = database_init.connect_to_database()
    database_init.create_tables(conn)

    authors_file = gzip.open('data/authors.jsonl.gz')
    authors_hash_map = main_migration_authors.migration(conn, authors_file)

    conversations_file = gzip.open('data/conversations.jsonl.gz')
    main_migration_conversations.migration(conn, conversations_file, authors_hash_map)

    #add_constraints.add_constraints(conn)
    conn.close()
    return

main()
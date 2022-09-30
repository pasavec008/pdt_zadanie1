import gzip

#my modules
import database_init
import main_migration_authors
import main_migration_conversations
import add_constraints

def main():
    conn = database_init.connect_to_database()
    database_init.create_tables(conn)

    # authors_file = gzip.open('data/authors.jsonl.gz')
    # authors_hash_map = main_migration_authors.migration(conn, authors_file)

    # conversations_file = gzip.open('data/conversations.jsonl.gz')
    # main_migration_conversations.migration(conn, conversations_file, authors_hash_map)

    add_constraints.add_constraints(conn)
    conn.close()
    return

main()
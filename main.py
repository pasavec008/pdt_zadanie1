import gzip

#my modules
import database_init
import migration_authors
import migration_conversations

def main():
    conn = database_init.connect_to_database()
    database_init.create_tables(conn)

    # authors_file = gzip.open('data/authors.jsonl.gz')
    # migration_authors.migration(conn, authors_file)

    conversations_file = gzip.open('data/conversations.jsonl.gz')
    migration_conversations.migration(conn, conversations_file)

    conn.close()
    return

main()
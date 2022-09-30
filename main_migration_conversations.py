import json
import time
import conversations_migration
import hashtags_migration
import context_migration

BATCH_SIZE = 100000

def migration(conn, conversations_file, authors_hash_map):
    cursor = conn.cursor()
    how_many_in_batch = 0
    hashtag_max_id = 1
    batch_conversations = []
    batch_hashtags = []
    batch_conversation_hashtags = []
    batch_context_domains = []
    batch_context_entities = []
    batch_context_annotations = []

    authors_hash_map_length = len(authors_hash_map)
    hashtag_hashmap, hashtag_hashmap_length = hashtags_migration.make_hashtag_hashmap()
    start = time.time()

    for record in conversations_file:
        conversations_dict = json.loads(record)
        #print(conversations_dict)

        if not conversations_migration.add_data_to_conversation_batch(
        conversations_dict, authors_hash_map, authors_hash_map_length, batch_conversations):
            continue
            
        #Data for hashtags table
        hashtag_max_id = hashtags_migration.add_data_to_hashtag_batch(conversations_dict, batch_hashtags, batch_conversation_hashtags, hashtag_hashmap, hashtag_hashmap_length, hashtag_max_id)

        #Data for context_domains, context_entities and context_annotations tables
        context_migration.add_data_to_context_batches(conversations_dict, batch_context_domains, batch_context_entities, batch_context_annotations)
        
        how_many_in_batch += 1
        
        if(how_many_in_batch == BATCH_SIZE):
            conversations_migration.send_conversations_batch(conn, cursor, batch_conversations)
            hashtags_migration.send_hashtag_batch(conn, cursor, batch_hashtags)
            hashtags_migration.send_conversation_hashtag_batch(conn, cursor, batch_conversation_hashtags)
            context_migration.send_context_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
            batch_conversations = []
            batch_hashtags = []
            batch_conversation_hashtags = []
            batch_context_domains = []
            batch_context_entities = []
            batch_context_annotations = []
            how_many_in_batch = 0
            print(time.time()-start)
            start = time.time()
        
    #send final data
    if how_many_in_batch:
        conversations_migration.send_conversations_batch(conn, cursor, batch_conversations)
        hashtags_migration.send_hashtag_batch(conn, cursor, batch_hashtags)
        hashtags_migration.send_conversation_hashtag_batch(conn, cursor, batch_conversation_hashtags)
        context_migration.send_context_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
    return
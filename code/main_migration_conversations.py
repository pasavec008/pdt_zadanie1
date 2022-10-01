import json
import time
import code.conversations_migration as conversations_migration
import code.hashtags_migration as hashtags_migration
import code.context_migration as context_migration
import code.annotations_migration as annotations_migration
import code.links_migration as links_migration
import code.references_migration as references_migration


BATCH_SIZE = 100000

def migration(conn, conversations_file, authors_hashmap):
    cursor = conn.cursor()
    how_many_in_batch = 0
    hashtag_max_id = 1
    batch_conversations = []
    batch_hashtags = []
    batch_conversation_hashtags = []
    batch_context_domains = []
    batch_context_entities = []
    batch_context_annotations = []
    batch_annotations = []
    batch_links = []

    authors_hashmap_length = len(authors_hashmap)
    hashtag_hashmap, hashtag_hashmap_length = hashtags_migration.make_hashtag_hashmap()
    conversation_hashmap, conversation_hashmap_length = 0,0#conversations_migration.make_conversation_hashmap()
    start = time.time()


    for record in conversations_file:
        conversations_dict = json.loads(record)
        #print(conversations_dict)

        if not conversations_migration.add_data_to_conversation_batch(conversations_dict, authors_hashmap,
        authors_hashmap_length, batch_conversations, conversation_hashmap, conversation_hashmap_length):
            continue
            
        #Data for hashtags table
        hashtag_max_id = hashtags_migration.add_data_to_hashtag_batch(conversations_dict, batch_hashtags, batch_conversation_hashtags, hashtag_hashmap, hashtag_hashmap_length, hashtag_max_id)

        #Data for context_domains, context_entities and context_annotations tables
        context_migration.add_data_to_context_batches(conversations_dict, batch_context_domains, batch_context_entities, batch_context_annotations)
        
        #Data for annotations
        annotations_migration.add_data_to_annotations_batch(conversations_dict, batch_annotations)

        #Data for links
        links_migration.add_data_to_links_batch(conversations_dict, batch_links)

        how_many_in_batch += 1
        
        if(how_many_in_batch == BATCH_SIZE):
            conversations_migration.send_conversations_batch(conn, cursor, batch_conversations)
            hashtags_migration.send_hashtag_batch(conn, cursor, batch_hashtags)
            hashtags_migration.send_conversation_hashtag_batch(conn, cursor, batch_conversation_hashtags)
            context_migration.send_context_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
            annotations_migration.send_annotations_batch(conn, cursor, batch_annotations)
            links_migration.send_links_batch(conn, cursor, batch_links)
            conn.commit()
            
            batch_conversations = []
            batch_hashtags = []
            batch_conversation_hashtags = []
            batch_context_domains = []
            batch_context_entities = []
            batch_context_annotations = []
            batch_annotations = []
            batch_links = []
            how_many_in_batch = 0
            print(time.time()-start)
            start = time.time()
        
    #send final data
    if how_many_in_batch:
        conversations_migration.send_conversations_batch(conn, cursor, batch_conversations)
        hashtags_migration.send_hashtag_batch(conn, cursor, batch_hashtags)
        hashtags_migration.send_conversation_hashtag_batch(conn, cursor, batch_conversation_hashtags)
        context_migration.send_context_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
        annotations_migration.send_annotations_batch(conn, cursor, batch_annotations)
        links_migration.send_links_batch(conn, cursor, batch_links)
        conn.commit()
    return conversation_hashmap
import json
import time

BATCH_SIZE = 100000

def send_conversations_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    #print(query_base + query)
    #start = time.time()
    cursor.execute("INSERT INTO conversations VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()
    #print(time.time()-start)

def send_hashtag_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO hashtags(tag) VALUES " + formated_data + "ON CONFLICT (tag) DO NOTHING")
    conn.commit()

def first_data_reading(conn, conversations_file, cursor, authors_hash_map):
    start = time.time()
    i = 0
    batch_conversations = []
    batch_hashtags = []
    authors_hash_map_length = len(authors_hash_map)

    for record in conversations_file:
        conversations_dict = json.loads(record)
        #print(conversations_dict)

        can_be_inserted = False
        x = int(conversations_dict['author_id'])
        if x in authors_hash_map[x % authors_hash_map_length]:
            can_be_inserted = True
        
        if(not can_be_inserted):
            continue
        
        #Data for conversations table
        batch_conversations.append((
            conversations_dict['id'],
            conversations_dict['author_id'],
            conversations_dict['text'].replace('\x00', ''),
            conversations_dict['possibly_sensitive'],
            conversations_dict['lang'],
            conversations_dict['source'],
            conversations_dict['public_metrics']['retweet_count'],
            conversations_dict['public_metrics']['reply_count'],
            conversations_dict['public_metrics']['like_count'],
            conversations_dict['public_metrics']['quote_count'],
            conversations_dict['created_at']
        ))
    

        #Data for hashtags table
        # if 'entities' in conversations_dict and 'hashtags' in conversations_dict['entities']:
        #     for hashtag in conversations_dict['entities']['hashtags']:
        #         batch_hashtags.append((hashtag['tag'],))

        #Data for context_domains table

        #Data for context_entities table

        #print(batch[-1])
        
        i += 1

        
        if(i == BATCH_SIZE):
            send_conversations_batch(conn, cursor, batch_conversations)
            # send_hashtag_batch(conn, cursor, batch_hashtags)
            batch_conversations = []
            # batch_hashtags = []
            i = 0
            #b+=1
            print(time.time()-start)
            start = time.time()
        
    #send final data
    if i:
        send_conversations_batch(conn, cursor, batch_conversations)
        # send_hashtag_batch(conn, cursor, batch_hashtags)
    
    

def migration(conn, conversations_file, authors_hash_map):
    cursor = conn.cursor()

    #First reading of data
    first_data_reading(conn, conversations_file, cursor, authors_hash_map)

    return
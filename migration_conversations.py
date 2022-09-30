import json
import time

BATCH_SIZE = 1000

def send_conversations_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO conversations VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()

def send_hashtag_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO hashtags VALUES " + formated_data)
    conn.commit()

def send_context_domains_batch(conn, cursor, batch_context_domains):
    formated_batch = []
    for x in batch_context_domains:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_domains VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()

def send_context_entities_batch(conn, cursor, batch_context_entities):
    formated_batch = []
    for x in batch_context_entities:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_entities VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()

def send_context_annotations_batch(conn, cursor, batch_context_annotations):
    formated_batch = []
    for x in batch_context_annotations:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_annotations(conversation_id, context_domain_id, context_entity_id) VALUES " + formated_data)
    conn.commit()

def send_context_annotations_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations):
    send_context_domains_batch(conn, cursor, batch_context_domains)
    send_context_entities_batch(conn, cursor, batch_context_entities)
    send_context_annotations_batch(conn, cursor, batch_context_annotations)

def make_hashtag_hashmap():
    hashtag_hashmap = []
    for i in range(1000000):
        hashtag_hashmap.append([])
    hashtag_hashmap_length = len(hashtag_hashmap)
    print("Hashtag hashmap created")

    return hashtag_hashmap, hashtag_hashmap_length

def add_data_to_conversation_batch(conversations_dict, authors_hash_map, authors_hash_map_length,
batch_conversations):
    can_be_inserted = False
    x = int(conversations_dict['author_id'])
    if x in authors_hash_map[x % authors_hash_map_length]:
        can_be_inserted = True
    
    if(not can_be_inserted):
        return False
    
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

    return True

def add_data_to_hashtag_batch(conversations_dict, batch_hashtags, hashtag_hashmap, hashtag_hashmap_length, hashtag_max_id):
    if 'entities' in conversations_dict and 'hashtags' in conversations_dict['entities']:
        for hashtag in conversations_dict['entities']['hashtags']:
            x = hash(hashtag['tag'])
            for xy in hashtag_hashmap[x % hashtag_hashmap_length]:
                if xy[0] == hashtag['tag']:
                    continue
            hashtag_hashmap[x % hashtag_hashmap_length].append([hashtag['tag'], hashtag_max_id])
            batch_hashtags.append((hashtag_max_id, hashtag['tag'],))
            hashtag_max_id += 1
    return hashtag_max_id

def add_data_to_context_annotations_tables(conversations_dict, batch_context_domains, batch_context_entities, batch_context_annotations):
    if 'context_annotations' in conversations_dict:
        for context_annotation in conversations_dict['context_annotations']:
            if 'domain' in context_annotation and 'entity' in context_annotation:
                if 'description' in context_annotation['domain']:
                    batch_context_domains.append((
                        context_annotation['domain']['id'],
                        context_annotation['domain']['name'],
                        context_annotation['domain']['description']
                    ))
                else:
                    batch_context_domains.append((
                        context_annotation['domain']['id'],
                        context_annotation['domain']['name'],
                        ""
                    ))
                
                if 'description' in context_annotation['entity']:
                    batch_context_entities.append((
                        context_annotation['entity']['id'],
                        context_annotation['entity']['name'],
                        context_annotation['entity']['description']
                    ))
                else:
                    batch_context_entities.append((
                        context_annotation['entity']['id'],
                        context_annotation['entity']['name'],
                        ""
                    ))
            else:
                continue
            #if domain or entity does not exist, we cannot create context_annotation
            batch_context_annotations.append((
                conversations_dict['id'],
                context_annotation['domain']['id'],
                context_annotation['entity']['id']
            ))



def migration(conn, conversations_file, authors_hash_map):
    cursor = conn.cursor()
    i = 0
    hashtag_max_id = 1
    batch_conversations = []
    batch_hashtags = []
    batch_context_domains = []
    batch_context_entities = []
    batch_context_annotations = []

    authors_hash_map_length = len(authors_hash_map)
    hashtag_hashmap, hashtag_hashmap_length = make_hashtag_hashmap()
    start = time.time()

    for record in conversations_file:
        conversations_dict = json.loads(record)
        #print(conversations_dict)

        if not add_data_to_conversation_batch(conversations_dict, authors_hash_map, authors_hash_map_length, batch_conversations):
            continue
            
        #Data for hashtags table
        hashtag_max_id = add_data_to_hashtag_batch(conversations_dict, batch_hashtags, hashtag_hashmap, hashtag_hashmap_length, hashtag_max_id)

        #Data for context_domains, context_entities and context_annotations tables
        add_data_to_context_annotations_tables(conversations_dict, batch_context_domains, batch_context_entities, batch_context_annotations)
        
        i += 1

        
        if(i == BATCH_SIZE):
            send_conversations_batch(conn, cursor, batch_conversations)
            send_hashtag_batch(conn, cursor, batch_hashtags)
            send_context_annotations_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
            batch_conversations = []
            batch_hashtags = []
            batch_context_domains = []
            batch_context_entities = []
            batch_context_annotations = []
            i = 0
            #b+=1
            print(time.time()-start)
            start = time.time()
            return
        
    #send final data
    if i:
        send_conversations_batch(conn, cursor, batch_conversations)
        send_hashtag_batch(conn, cursor, batch_hashtags)
        send_context_annotations_batches(conn, cursor, batch_context_domains, batch_context_entities, batch_context_annotations)
    return
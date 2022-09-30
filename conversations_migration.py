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

def send_conversations_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO conversations VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()
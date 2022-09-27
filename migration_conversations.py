import json

BATCH_SIZE = 100000

def send_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    #print(query_base + query)
    #start = time.time()
    cursor.execute("INSERT INTO conversations VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()
    #print(time.time()-start)


def migration(conn, conversations_file):
    cursor = conn.cursor()
    i = 0
    batch = []
    for record in conversations_file:
        conversations_dict = json.loads(record)
        
        batch.append((
            conversations_dict['conversation_id'],
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

        #print(batch[-1])
        
        i += 1

        if(i == BATCH_SIZE):
            send_batch(conn, cursor, batch)
            batch = []
            i = 0

    #send final data
    if i:
        send_batch(conn, cursor, batch)

    return
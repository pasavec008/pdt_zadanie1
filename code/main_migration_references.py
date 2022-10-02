import json
from time import time
from time import gmtime
from datetime import datetime

BATCH_SIZE = 100000

def add_data_to_references_batch(conversations_dict, batch_references, conversation_hashmap, conversation_hashmap_length):
    return_value = 0
    if 'referenced_tweets' in conversations_dict:
        for tweet in conversations_dict['referenced_tweets']:
            conversation_exist = False
            x = int(tweet['id'])
            if x in conversation_hashmap[x % conversation_hashmap_length]:
                conversation_exist = True
            
            if(conversation_exist):
                return_value += 1
                batch_references.append((
                    conversations_dict['id'],
                    tweet['id'],
                    tweet['type']
                ))
    return return_value

def send_references_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO conversation_references(conversation_id, parent_id, type) VALUES " + formated_data)
    return

def migration(conn, conversations_file, conversation_hashmap):
    third_reading_csv = open('results_times/third_reading.csv', 'w')
    cursor = conn.cursor()
    how_many_in_batch = 0
    batch_references = []
    conversation_hashmap_length = len(conversation_hashmap)

    ultimate_start = time()
    start = time()
    for record in conversations_file:
        conversations_dict = json.loads(record)
        
        how_many_in_batch += add_data_to_references_batch(conversations_dict, batch_references, conversation_hashmap, conversation_hashmap_length)

        if(how_many_in_batch == BATCH_SIZE):
            send_references_batch(cursor, batch_references)
            conn.commit()
            batch_references = []
            how_many_in_batch = 0
            print("Reference batch: ", time()-start)

            big_diff = gmtime(time() - ultimate_start)
            small_diff = gmtime(time() - start)
            time_to_write = datetime.now().strftime('%Y-%m-%dT%H:%MZ') + ';' + \
                str((big_diff.tm_hour * 60 + big_diff.tm_min)).zfill(2) + ":" + \
                str(big_diff.tm_sec).zfill(2) + ';' +\
                str((small_diff.tm_hour * 60 + small_diff.tm_min)).zfill(2) + ":" + \
                str(small_diff.tm_sec).zfill(2) + '\n'

            third_reading_csv.write(time_to_write)

            start = time()
        
    #send final data
    if how_many_in_batch:
        send_references_batch(cursor, batch_references)
        conn.commit()
    
    third_reading_csv.close()
    return
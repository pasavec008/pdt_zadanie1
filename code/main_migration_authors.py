import json
import time

BATCH_SIZE = 100000

def send_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    #print(query_base + query)
    #start = time.time()
    cursor.execute("INSERT INTO authors VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")
    conn.commit()
    #print(time.time()-start)


def migration(conn, authors_file):
    hash_map = []

    for i in range(8000000):
        hash_map.append([])
    print("Author hashmap created")
    
    hash_map_length = len(hash_map)

    cursor = conn.cursor()
    number_in_batch = 0
    batch = []

    start = time.time()
    for record in authors_file:
        authors_dict = json.loads(record)

        # find place in hashmap for new author id
        x = int(authors_dict['id'])
        hash_map[x % hash_map_length].append(x)

        # batch.append((
        #     authors_dict['id'],
        #     authors_dict['name'].replace('\x00', ''),
        #     authors_dict['username'].replace('\x00', ''),
        #     authors_dict['description'].replace('\x00', ''),
        #     authors_dict['public_metrics']['followers_count'],
        #     authors_dict['public_metrics']['following_count'],
        #     authors_dict['public_metrics']['tweet_count'],
        #     authors_dict['public_metrics']['listed_count']
        # ))
        
        # number_in_batch += 1

        # if(number_in_batch == BATCH_SIZE):
        #     send_batch(conn, cursor, batch)
        #     batch = []
        #     number_in_batch = 0
        #     print("Author batch: ", time.time()-start)
        #     start = time.time()

    #send final data
    if number_in_batch:
        send_batch(conn, cursor, batch)

    return hash_map
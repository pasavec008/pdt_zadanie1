import json
from time import time
from time import gmtime
from datetime import datetime

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
    first_reading_csv = open('results_times/first_reading.csv', 'w')
    hash_map = []

    for i in range(8000000):
        hash_map.append([])
    print("Author hashmap created")
    
    hash_map_length = len(hash_map)

    cursor = conn.cursor()
    number_in_batch = 0
    batch = []

    ultimate_start = time()
    start = time()
    for record in authors_file:
        authors_dict = json.loads(record)

        # find place in hashmap for new author id
        x = int(authors_dict['id'])
        hash_map[x % hash_map_length].append(x)

        batch.append((
            authors_dict['id'],
            authors_dict['name'].replace('\x00', ''),
            authors_dict['username'].replace('\x00', ''),
            authors_dict['description'].replace('\x00', ''),
            authors_dict['public_metrics']['followers_count'],
            authors_dict['public_metrics']['following_count'],
            authors_dict['public_metrics']['tweet_count'],
            authors_dict['public_metrics']['listed_count']
        ))
        
        number_in_batch += 1

        if(number_in_batch == BATCH_SIZE):
            send_batch(conn, cursor, batch)
            batch = []
            number_in_batch = 0
            print("Author batch: ", time()-start)

            big_diff = gmtime(time() - ultimate_start)
            small_diff = gmtime(time() - start)
            time_to_write = datetime.now().strftime('%Y-%m-%dT%H:%MZ') + ';' + \
                str((big_diff.tm_hour * 60 + big_diff.tm_min)).zfill(2) + ":" + \
                str(big_diff.tm_sec).zfill(2) + ';' +\
                str((small_diff.tm_hour * 60 + small_diff.tm_min)).zfill(2) + ":" + \
                str(small_diff.tm_sec).zfill(2) + '\n'

            first_reading_csv.write(time_to_write)

            start = time()

            

    #send final data
    if number_in_batch:
        send_batch(conn, cursor, batch)

    first_reading_csv.close()
    return hash_map
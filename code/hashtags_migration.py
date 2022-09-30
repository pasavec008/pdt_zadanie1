def add_data_to_hashtag_batch(conversations_dict, batch_hashtags, batch_conversation_hashtags, hashtag_hashmap, hashtag_hashmap_length, hashtag_max_id):
    if 'entities' in conversations_dict and 'hashtags' in conversations_dict['entities']:
        for hashtag in conversations_dict['entities']['hashtags']:
            x = hash(hashtag['tag'])
            already_there = False
            for xy in hashtag_hashmap[x % hashtag_hashmap_length]:
                if xy[0] == hashtag['tag']:
                    already_there = True
                    already_there_id = xy[1]
                    break
            if already_there:
                batch_conversation_hashtags.append((conversations_dict['id'], already_there_id))
                continue
            hashtag_hashmap[x % hashtag_hashmap_length].append([hashtag['tag'], hashtag_max_id])
            batch_hashtags.append((hashtag_max_id, hashtag['tag'],))
            batch_conversation_hashtags.append((conversations_dict['id'], hashtag_max_id))
            hashtag_max_id += 1
    return hashtag_max_id
    
def send_hashtag_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO hashtags VALUES " + formated_data)

def send_conversation_hashtag_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO conversation_hashtags(conversation_id, hashtag_id) VALUES " + formated_data)

def make_hashtag_hashmap():
    hashtag_hashmap = []
    for i in range(1000000):
        hashtag_hashmap.append([])
    hashtag_hashmap_length = len(hashtag_hashmap)
    print("Hashtag hashmap created")

    return hashtag_hashmap, hashtag_hashmap_length
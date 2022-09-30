def add_data_to_references_batch(conversations_dict, batch_references):
    if 'referenced_tweets' in conversations_dict:
        for tweet in conversations_dict['referenced_tweets']:
            batch_references.append((
                conversations_dict['id'],
                tweet['id'],
                tweet['type']
            ))
    return

def send_references_batch(conn, cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO conversation_references(conversation_id, parent_id, type) VALUES " + formated_data)
    return
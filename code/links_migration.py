def add_data_to_links_batch(conversations_dict, batch_links):
    if 'entities' in conversations_dict and 'urls' in conversations_dict['entities']:
        for link in conversations_dict['entities']['urls']:
            batch_links.append((
                conversations_dict['id'],
                link['expanded_url'],
                '' if not 'title' in link else link['title'],
                '' if not 'description' in link else link ['description']
            ))
    return

def send_links_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO links(conversation_id, url, title, description) VALUES " + formated_data)
    return
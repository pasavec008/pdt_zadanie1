def add_data_to_annotations_batch(conversations_dict, batch_annotations):
    if 'entities' in conversations_dict and 'annotations' in conversations_dict['entities']:
        for annotation in conversations_dict['entities']['annotations']:
            batch_annotations.append((conversations_dict['id'], annotation['normalized_text'], annotation['type'], annotation['probability']))
    return

def send_annotations_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO annotations(conversation_id, value, type, probability) VALUES " + formated_data)
    return
def add_data_to_context_batches(conversations_dict, batch_context_domains, batch_context_entities, batch_context_annotations):
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

def send_context_domains_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_domains VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")

def send_context_entities_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_entities VALUES " + formated_data + "ON CONFLICT (id) DO NOTHING")

def send_context_annotations_batch(cursor, batch):
    formated_batch = []
    for x in batch:
        formated_batch.append(cursor.mogrify("(%s, %s, %s)", x).decode("utf-8"))

    formated_data = ','.join(formated_batch)

    cursor.execute("INSERT INTO context_annotations(conversation_id, context_domain_id, context_entity_id) VALUES " + formated_data)

def send_context_batches(cursor, batch_context_domains, batch_context_entities, batch_context_annotations):
    send_context_domains_batch(cursor, batch_context_domains)
    send_context_entities_batch(cursor, batch_context_entities)
    send_context_annotations_batch(cursor, batch_context_annotations)
def add_constraints(conn):
    cursor = conn.cursor()
    cursor.execute('''
        ALTER TABLE conversations
        ADD CONSTRAINT fk_user FOREIGN KEY(author_id) REFERENCES authors(id);

        ALTER TABLE hashtags
        ADD CONSTRAINT unq_key UNIQUE(tag);

        ALTER TABLE conversation_hashtags
        ADD CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id);
        ALTER TABLE conversation_hashtags
        ADD CONSTRAINT fk_hashtag FOREIGN KEY(hashtag_id) REFERENCES hashtags(id);

        ALTER TABLE context_annotations
        ADD CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id);
        ALTER TABLE context_annotations
        ADD CONSTRAINT fk_context_domain FOREIGN KEY(context_domain_id) REFERENCES context_domains(id);
        ALTER TABLE context_annotations
        ADD CONSTRAINT fk_context_entity FOREIGN KEY(context_entity_id) REFERENCES context_entities(id);

        ALTER TABLE annotations
        ADD CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id);

        ALTER TABLE links
        ADD CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id);

        ALTER TABLE conversation_references
        ADD CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id);
        ALTER TABLE conversation_references
        CONSTRAINT fk_conversation_parent FOREIGN KEY(parent_id) REFERENCES conversations(id);
    ''')
    conn.commit()
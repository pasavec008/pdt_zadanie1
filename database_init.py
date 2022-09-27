import psycopg2

def connect_to_database():
    conn = psycopg2.connect(
        host='localhost',
        database='twitter',
        user='postgres',
        password='admin'
    )
    return conn

def create_tables(conn):
    cursor = conn.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS authors(
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            username VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            followers_count INT NOT NULL,
            following_count INT NOT NULL,
            tweet_count INT NOT NULL,
            listed_count INT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS conversations(
            id BIGINT PRIMARY KEY,
            author_id BIGINT,
            content TEXT,
            possibly_sensitive BOOLEAN,
            language VARCHAR(3),
            source TEXT,
            retweet_count INT NOT NULL,
            reply_count INT NOT NULL,
            like_count INT NOT NULL,
            quote_count INT NOT NULL,
            created_at TIMESTAMPTZ,
            CONSTRAINT fk_user FOREIGN KEY(author_id) REFERENCES authors(id)
        );

        CREATE TABLE IF NOT EXISTS hashtags(
            id BIGINT PRIMARY KEY,
            tag TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS conversation_hashtags(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            hashtag_id BIGINT,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_hashtag FOREIGN KEY(hashtag_id) REFERENCES hashtags(id)
        );

        CREATE TABLE IF NOT EXISTS conversation_hashtags(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            hashtag_id BIGINT,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_hashtag FOREIGN KEY(hashtag_id) REFERENCES hashtags(id)
        );

        CREATE TABLE IF NOT EXISTS context_domains(
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            description TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS context_entities(
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            description TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS context_annotations(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            context_domain_id BIGINT,
            context_entity_id BIGINT,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_context_domain FOREIGN KEY(context_domain_id) REFERENCES context_domains(id),
            CONSTRAINT fk_context_entity FOREIGN KEY(context_entity_id) REFERENCES context_entities(id)
        );

        CREATE TABLE IF NOT EXISTS annotations(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            value TEXT,
            type TEXT,
            probability NUMERIC(4, 3),
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );

        CREATE TABLE IF NOT EXISTS links(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            url VARCHAR(2048),
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );

        CREATE TABLE IF NOT EXISTS conversation_references(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT,
            parent_id BIGINT,
            type VARCHAR(20),
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_conversation_parent FOREIGN KEY(parent_id) REFERENCES conversations(id)
        );
    '''
    cursor.execute(query)
    conn.commit()

    return
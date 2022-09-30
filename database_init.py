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
            name VARCHAR(255),
            username VARCHAR(255),
            description TEXT,
            followers_count INT,
            following_count INT,
            tweet_count INT,
            listed_count INT
        );

        CREATE TABLE IF NOT EXISTS conversations(
            id BIGINT PRIMARY KEY,
            author_id BIGINT NOT NULL,
            content TEXT NOT NULL,
            possibly_sensitive BOOLEAN NOT NULL,
            language VARCHAR(3) NOT NULL,
            source TEXT NOT NULL,
            retweet_count INT,
            reply_count INT,
            like_count INT,
            quote_count INT,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hashtags(
            id BIGINT PRIMARY KEY,
            tag TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS conversation_hashtags(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            hashtag_id BIGINT NOT NULL,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_hashtag FOREIGN KEY(hashtag_id) REFERENCES hashtags(id)
        );

        CREATE TABLE IF NOT EXISTS context_domains(
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS context_entities(
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS context_annotations(
            id BIGSERIAL PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            context_domain_id BIGINT NOT NULL,
            context_entity_id BIGINT NOT NULL,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_context_domain FOREIGN KEY(context_domain_id) REFERENCES context_domains(id),
            CONSTRAINT fk_context_entity FOREIGN KEY(context_entity_id) REFERENCES context_entities(id)
        );

        CREATE TABLE IF NOT EXISTS annotations(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            value TEXT NOT NULL,
            type TEXT NOT NULL,
            probability NUMERIC(4, 3) NOT NULL,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );

        CREATE TABLE IF NOT EXISTS links(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            url VARCHAR(2048) NOT NULL,
            title TEXT,
            description TEXT,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );

        CREATE TABLE IF NOT EXISTS conversation_references(
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            parent_id BIGINT NOT NULL,
            type VARCHAR(20) NOT NULL,
            CONSTRAINT fk_conversation FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            CONSTRAINT fk_conversation_parent FOREIGN KEY(parent_id) REFERENCES conversations(id)
        );
    '''
    cursor.execute(query)
    conn.commit()

    return
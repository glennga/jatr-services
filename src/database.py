import os
import sqlite3
import logging
import dotenv
import json

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('DatabaseInitializer')
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    dotenv.load_dotenv()
    with open('config/config.json') as config_file:
        config = json.load(config_file)

    # Create the directory to hold database if it does not exist.
    db_location = config['databaseDescription']['location']
    if not os.path.exists(db_location):
        os.makedirs(db_location.split('/')[0])

    try:
        # Open a connection to our database file.
        conn = sqlite3.connect(db_location)
        cursor = conn.cursor()

        # Initialize...
        cursor.executescript("""
            DROP TABLE IF EXISTS DiscordMessages;              
            DROP TABLE IF EXISTS JapanLocations;          
            DROP TABLE IF EXISTS JapanLocationsCategories;     
            DROP TABLE IF EXISTS BlacklistedLocations;         
            DROP TABLE IF EXISTS MessagesToJapanLocations;  
            
            -- Our entities...
            CREATE TABLE DiscordMessages (
                id          INTEGER PRIMARY KEY,
                search_term TEXT NOT NULL,
                author      TEXT NOT NULL,
                channel     TEXT NOT NULL,
                content     TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                jump_url    TEXT NOT NULL,
                UNIQUE(search_term)
            );
            CREATE TABLE JapanLocations (
                id                 TEXT PRIMARY KEY,
                name               TEXT NOT NULL,
                alias              TEXT,
                image_url          TEXT,
                yelp_url           TEXT NOT NULL,
                coord_latitude     TEXT NOT NULL,
                coord_longitude    TEXT NOT NULL,
                rating             INTEGER NOT NULL,
                review_count       INTEGER NOT NULL,
                price              TEXT,
                location_address_1 TEXT NOT NULL,
                location_address_2 TEXT,
                location_address_3 TEXT,
                location_city      TEXT NOT NULL,
                location_zip_code  TEXT NOT NULL,
                phone              TEXT
            );
            CREATE TABLE JapanLocationsCategories (
                id       TEXT,
                category TEXT,
                alias    TEXT,
                PRIMARY KEY (id, category),
                FOREIGN KEY (id) REFERENCES JapanLocations
            );
            CREATE TABLE BlacklistedLocations (
                id TEXT PRIMARY KEY
            );
            
            -- ...and our M:N relationships...
            CREATE TABLE MessagesToJapanLocations (
                discord_message_id INTEGER,
                japan_locations_id TEXT,
                PRIMARY KEY (discord_message_id, japan_locations_id),
                FOREIGN KEY (discord_message_id) REFERENCES DiscordMessages,
                FOREIGN KEY (japan_locations_id) REFERENCES JapanLocations
            );
        """)
        logger.info('Our database has been initialized.')

    except sqlite3.Error as e:
        logger.error('Encountered error with our database!')
        logger.error(e)

import http.server
import os
import sqlite3
import sys
import urllib.parse
import dotenv
import logging
import json
import requests

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('IndexerService')
logger.setLevel(logging.DEBUG)

# We expose the following OP-CODEs.
OP_CODE_INDEX = 'INDEX'
OP_CODE_DELETE = 'DELETE'

class IndexerService(http.server.BaseHTTPRequestHandler):
    @staticmethod
    def respond_to_client(handler, status_code, content):
        handler.send_response(status_code)
        handler.send_header('Content-type', 'text/plain')
        handler.send_header('Content-Length', len(content))
        handler.send_header('Connection', 'close')
        handler.end_headers()
        handler.wfile.write(content.encode('UTF-8'))
        handler.wfile.flush()

    def do_INDEX(self):
        content_length = int(self.headers['Content-Length'])
        request_data = self.rfile.read(content_length)
        logger.debug(f'INDEX request received, of length {content_length}.')

        # Using our caller's map, search for each index key using the Yelp API.
        empty_search_terms, inverted_map = list(), json.loads(request_data)
        logger.debug(f'Inverted map loaded. We have {len(inverted_map.keys())} terms.')
        for search_term, message_dicts in inverted_map.items():
            # First, check if we have already searched for this term. Look in our discord messages.
            try:
                cursor.execute("""
                    SELECT 1
                    FROM   DiscordMessages 
                    WHERE  :search_term = search_term;
                """, {"search_term": search_term})
                if cursor.fetchone() is not None:
                    logger.info(f'Search term {search_term} has been indexed. Skipping.')
                    continue

                # ...otherwise, we have not searched for this term. Store our messages in our database.
                for message_dict in message_dicts:
                    cursor.execute("""
                        INSERT INTO DiscordMessages (id, search_term, author, channel, content, created_at, jump_url)
                        VALUES                      (:id, :search_term, :author, :channel, :content, :created_at, 
                                                     :jump_url);
                    """, {**message_dict, **{'search_term': search_term}})

                # Issue a request to Yelp.
                logger.info(f'Given search term {search_term}. Issuing request to Yelp.')
                url = 'https://api.yelp.com/v3/businesses/search?' + \
                      urllib.parse.urlencode({
                          'location': config['yelpSearch']['location'],
                          'limit': config['yelpSearch']['limit'],
                          'term': search_term,
                          'sort_by': 'best_match',
                          'locale': 'en_US'
                      }, safe="()", quote_via=urllib.parse.quote)
                headers = {
                    'accept': 'application/json',
                    'Authorization': f'Bearer {os.getenv("YELP_TOKEN")}'
                }
                yelp_response = requests.get(url, headers=headers)
                if not yelp_response.ok:
                    logger.error('Yelp has returned a non-200 response code!')
                    logger.error(yelp_response.content)
                    return IndexerService.respond_to_client(self, 500, 'INDEX request could not be processed.')

                # Do a bit of cleaning...
                raw_businesses, clean_businesses = yelp_response.json()['businesses'], list()
                null_handler = lambda s, n: s[n] if (n in s and s[n] != '' and
                                                     not (type(s[n]) == list and len(s[n]) == 0)) else None
                required_attributes = {'id', 'name', 'coordinates', 'rating', 'review_count', 'location'}
                logger.info(f'Yelp has responded! Given {len(raw_businesses)} businesses.')
                for b in raw_businesses:
                    if 'is_closed' in b and b['is_closed']:
                        logger.debug(f'Skipping businesses {b}. This location is closed.')
                        continue
                    if not required_attributes.issubset(b.keys()):
                        logger.debug(f'Skipping businesses {b}. Missing one of the required fields.')
                        continue
                    logger.debug(f'Business {b} has been accepted.')
                    clean_businesses.append({
                        'id': b['id'],
                        'name': b['name'],
                        'alias': null_handler(b, 'alias'),
                        'image_url': null_handler(b, 'image_url'),
                        'yelp_url': b['url'],
                        'coord_latitude': b['coordinates']['latitude'],
                        'coord_longitude': b['coordinates']['longitude'],
                        'rating': b['rating'],
                        'review_count': b['review_count'],
                        'price': null_handler(b, 'price'),
                        'location_address_1': b['location']['address1'],
                        'location_address_2': null_handler(b['location'], 'address2'),
                        'location_address_3': null_handler(b['location'], 'address3'),
                        'location_city': b['location']['city'],
                        'location_zip_code': b['location']['zip_code'],
                        'phone': null_handler(b, 'phone'),
                        'categories': null_handler(b, 'categories')
                    })

                # Do we not have any business? Continue (and reply to our caller).
                if len(clean_businesses) == 0:
                    empty_search_terms.append(search_term)
                    continue

                # ...and store these results in our database if we have not already stored them.
                cursor.executemany("""
                    INSERT INTO JapanLocations (id, name, alias, image_url, yelp_url, coord_latitude, coord_longitude,
                                                rating, review_count, price, location_address_1, location_address_2, 
                                                location_address_3, location_city, location_zip_code, phone)
                    VALUES                     (:id, :name, :alias, :image_url, :yelp_url, :coord_latitude, 
                                                :coord_longitude, :rating, :review_count, :price, :location_address_1, 
                                                :location_address_2, :location_address_3, :location_city, 
                                                :location_zip_code, :phone)
                    ON CONFLICT (id) DO NOTHING;
                """, clean_businesses)
                for business in clean_businesses:
                    if 'categories' in business and business['categories'] is not None:
                        categories = [{'id': business['id'], 'category': c['title'], 'alias': c['alias']}
                                      for c in business['categories']]
                        cursor.executemany("""
                            INSERT INTO JapanLocationsCategories (id, category, alias) 
                            VALUES                               (:id, :category, :alias)
                            ON CONFLICT (id, category) DO NOTHING;
                        """, categories)

                # We want to keep the association between our message and each of these businesses as well.
                for message_dict in message_dicts:
                    cursor.executemany("""
                        INSERT INTO MessagesToJapanLocations (discord_message_id, japan_locations_id)
                        VALUES                               (:discord_id, :yelp_id)
                        ON CONFLICT (discord_message_id, japan_locations_id) DO NOTHING;
                    """, [{'discord_id': message_dict['id'], 'yelp_id': s['id']} for s in clean_businesses])
                logger.info('All **cleaned** businesses have been inserted into our database.')
                conn.commit()

            except sqlite3.Error as e:
                logger.error('Could not process INSERT! Database error encountered: ')
                logger.error(e)
                return IndexerService.respond_to_client(self, 500, 'INDEX request could not be processed.')

        # Respond to our caller.
        if len(empty_search_terms) > 0:
            empty_search_term_string = ','.join(f'"{s}"' for s in empty_search_terms)
            return IndexerService.respond_to_client(
                self, 199, f'Warning! INDEX request processed, but the following terms yielded no '
                           f'Yelp results:\n{empty_search_term_string}')
        else:
            return IndexerService.respond_to_client(self, 200, 'INDEX request successfully processed.')

    def do_DELETE(self):
        content_length = int(self.headers['Content-Length'])
        request_data = self.rfile.read(content_length)
        logger.debug(f'DELETE request received, of length {content_length}.')

        # We expect a list of primary keys (of locations).
        location_ids = json.loads(request_data)
        try:
            # Note: our DELETE is not physical, this simply tells our application to not display these in the future.
            cursor.executemany("""
                INSERT INTO BlacklistedLocations (id)
                VALUES                           (?)
                ON CONFLICT (id) DO NOTHING;
            """, tuple([(i,) for i in location_ids]))
            conn.commit()

            # Respond to our caller.
            return IndexerService.respond_to_client(self, 200, 'DELETE request successfully processed.')

        except sqlite3.Error as e:
            logger.error('Could not process DELETE! Database error encountered: ')
            logger.error(e)
            return IndexerService.respond_to_client(self, 500, 'DELETE request could not be processed!')

if __name__ == '__main__':
    dotenv.load_dotenv()
    with open('config/config.json') as config_file:
        config = json.load(config_file)

    # Open a connection to our database.
    try:
        db_location = config['databaseDescription']['location']
        conn = sqlite3.connect(db_location)
        cursor = conn.cursor()

    except sqlite3.Error as err:
        logger.error('Encountered error with our database!')
        logger.error(err)
        sys.exit(1)

    # Start our indexer service.
    server = http.server.HTTPServer(('localhost', config['serviceDescription']['indexerPort']), IndexerService)
    logger.info('Indexer service has started.')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Indexer service has been interrupted.')
        pass

    server.server_close()
    logger.info('Indexer service has been shutdown.')

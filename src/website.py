import sqlite3
import sys
import dotenv
import bokeh.io
import bokeh.layouts
import bokeh.models
import bokeh.plotting
import logging
import pandas

logger = logging.getLogger('WebsiteLayoutService')
logger.setLevel(logging.DEBUG)

class WebsiteLayout:
    def refresh_view(self, limit, offset=0, category=None, alias=None):
        try:
            # We don't have built-in UDFs... :-(
            logger.info('Dropping old view PointsOfInterest.')
            self.cursor.execute("""
                DROP VIEW IF EXISTS PointsOfInterest;
            """)

            # Build our "categories" clause (we'll keep the parameterizing for SQLite).
            categories_clause = None
            categories_params = tuple()
            if category is not None or alias is not None:
                category_join = """
                    EXISTS     ( SELECT 1
                                 FROM   JapanLocationsCategories JLC 
                                 WHERE  JLC.id = JLI.id AND {} )
                """
                if category is not None and alias is not None:
                    categories_clause = category_join.format("( JLC.category LIKE ? OR JLC.alias LIKE ? )")
                    categories_params = (category, alias,)

                elif category is not None and alias is None:
                    categories_clause = category_join.format("JLC.category LIKE ?")
                    categories_params = (category,)

                elif category is None and alias is not None:
                    categories_clause = category_join.format("JLC.alias LIKE ?")
                    categories_params = (alias,)

            logger.info('Creating PointsOfInterest view.')
            self.cursor.execute(f"""
                CREATE   TEMPORARY VIEW PointsOfInterest ( location_id,
                                                           message_id,
                                                           review_count,
                                                           rank )
                AS
                SELECT   JLI.id                                  AS location_id,
                         MJL.discord_message_id                  AS message_id,
                         JLI.review_count                        AS review_count,
                         RANK() OVER (
                            PARTITION BY MJL.discord_message_id
                            ORDER BY     JLI.review_count DESC ) AS rank
                FROM     JapanLocations JLI,
                         MessagesToJapanLocations MJL
                WHERE    MJL.japan_locations_id = JLI.id AND
                         NOT EXISTS ( SELECT 1
                                      FROM   BlacklistedLocations BL
                                      WHERE  BL.id = JLI.id )
                         {("AND " + categories_clause) if categories_clause is not None else ""}
                LIMIT    ?
                OFFSET   ?;
            """, categories_params + (limit, offset,))

        except sqlite3.Error as e:
            logger.error('Encountered error with our database!')
            logger.error(e)

    def __init__(self, **kwargs):
        self.config = kwargs
        dotenv.load_dotenv()
        self.data_frame = pandas.DataFrame()

        # Open a connection to our database.
        try:
            db_location = self.config['databaseDescription']['location']
            self.conn = sqlite3.connect(db_location)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

            # Set up our initial view. There are no restrictions on keywords.
            initial_data_range = self.config['bokehLayout']['initialDataRange']
            self.refresh_view(limit=initial_data_range[1], offset=initial_data_range[0])

            # Determine our centroid range.
            initial_group_rank = self.config['bokehLayout']['initialGroupRank']
            self.cursor.execute("""
                WITH   CoordinatesOfInterest AS ( SELECT JL.coord_latitude,
                                                         JL.coord_longitude
                                                  FROM   PointsOfInterest POI,
                                                         JapanLocations JL
                                                  WHERE  POI.location_id = JL.id AND 
                                                         POI.r_number <= ? )
                SELECT AVG(COI.coord_latitude)  AS avg_latitude,
                       AVG(COI.coord_longitude) AS avg_longitude
                FROM   CoordinatesOfInterest COI;
            """, (initial_group_rank,))
            centroid_tuple = self.cursor.fetchone()
            self.centroid = {'longitude': centroid_tuple['avg_longitude'], 'latitude': centroid_tuple['avg_latitude']}

        except sqlite3.Error as e:
            logger.error('Encountered error with our database!')
            logger.error(e)
            sys.exit(1)

    def generate_range_tool(self, **kwargs):
        raise NotImplementedError

    def generate_map(self, **kwargs):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return bokeh.layouts.column(
            self.generate_range_tool(sizing_mode='inherit'),
            self.generate_map(sizing_mode='inherit'),
            sizing_mode='stretch_width'
        )

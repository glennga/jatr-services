# Ja-Tr Repository 
A repository that contains services to plan a trip to Japan.

There are three components of interest here: 
1. The Discord scraper service (`src/scraper.py`), which scans through all Discord messages and responds to various commands.
2. The indexer service (`src/indexer.py`), which will manage `INDEX` and `DELETE` requests from the scraper service above and the web application. The `INDEX` command searches for locations in Discord messages via Yelp. The `DELETE` command prevents locations from appearing on the web application. 
3. The web application (`src/website.py`), which display all locations from the indexer service using Bokeh (and a UI element via Google Maps).
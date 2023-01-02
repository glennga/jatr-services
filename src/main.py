import json
import sys
import dotenv
import bokeh.io
import bokeh.plotting
import logging

import website

logger = logging.getLogger('BokehEntryPoint')
logger.setLevel(logging.DEBUG)

# Load our configurations.
dotenv.load_dotenv()
with open('config/config.json') as config_file:
    config = json.load(config_file)
    logger.info(f'Configuration loaded: {config}')
logger.info('Starting up! Bokeh has called us.')

# Set up the layout for our web-app.
layout = website.WebsiteLayout(**config)()
bokeh.io.curdoc().add_root(layout)
bokeh.io.curdoc().title = config['bokehLayout']['title']

if __name__ == '__main__':
    logger.info('Do not call this script directly! Use "bokeh serve --show src".')
    sys.exit(1)

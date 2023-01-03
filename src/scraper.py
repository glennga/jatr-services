import aiohttp
import logging
import dotenv
import json
import discord.errors
import discord.ui
import discord.interactions
import discord.ext.commands
import nltk.tokenize
import nltk.corpus
import emoji
import os
import indexer
import asyncio
import collections

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('DiscordService')
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    dotenv.load_dotenv()
    with open('config/config.json') as config_file:
        config = json.load(config_file)

    # Pull the NLTK stopwords (we should only need to run this once)
    while True:
        try:
            stopwords = set(nltk.corpus.stopwords.words('english'))
            break
        except LookupError:
            nltk.download('stopwords')

    # Try to tokenize a message.
    while True:
        try:
            nltk.tokenize.word_tokenize('')
            break
        except LookupError:
            nltk.download('punkt')

    # Connect to Discord's API endpoint.
    intents = discord.Intents.default()
    intents.message_content = True
    bot = discord.ext.commands.Bot(command_prefix='!', intents=intents)

    @bot.event
    async def on_ready():
        logger.info('Connected to Discord\'s endpoint.')

    @bot.command(name='refresh')
    async def refresh_command(ctx: discord.ext.commands.Context):
        await ctx.reply('Got it! Scraping and tokenizing all discord messages.')
        emoji_whitelist = config['discordSearch']['emojiWhitelist']

        # Iterate through each channel.
        inverted_map = dict()
        for channel in bot.get_all_channels():
            if 'Text Channels' in channel.name or 'Voice Channels' in channel.name:
                logger.info(f'Skipping through channel {channel.name}.')
                continue
            else:
                logger.info(f'Iterating through channel {channel.name}.')

            # Ensure we have access to the channel being scraped.
            try:
                async for message in channel.history(limit=None):
                    if message.author.bot:
                        logger.debug('Bot message found. Ignoring.')
                        continue
                    elif not any(m.emoji in emoji_whitelist for m in message.reactions):
                        logger.debug(f'Ignoring message: "{message.content}". No whitelisted reactions.')
                        continue
                    else:
                        sanitized_message = emoji.replace_emoji(message.content)
                        logger.debug(f'Sanitized message fetch: "{sanitized_message}"')

                        # Remove all stop-words from our message and insert this into our map.
                        word_tokens = [
                            w for w in nltk.tokenize.word_tokenize(sanitized_message) if not w.lower() in stopwords
                        ]
                        logger.debug(f'Message has been tokenized. Found {len(word_tokens)} tokens.')
                        index_key = ' '.join(word_tokens)
                        if index_key not in inverted_map:
                            inverted_map[index_key] = []
                        inverted_map[index_key].append({
                            'id': message.id,
                            'author': str(message.author.name),
                            'channel': str(message.channel.name),
                            'content': sanitized_message,
                            'created_at': str(message.created_at),
                            'jump_url': str(message.jump_url)
                        })

            except discord.errors.Forbidden:
                logger.warning(f'Denied access to channel {channel.name}. Skipping.')

        # Wrap our inverted map and send this to our indexer.
        indexer_endpoint = f'http://localhost:{config["serviceDescription"]["indexerPort"]}'
        logger.info(f'Pushing inverted map to indexer at endpoint {indexer_endpoint}.')

        # TODO (GLENN): I'm sure there's a cleaner way to do this... but I don't know asyncio :-)
        async def issue_request():
            IndexerResponser = collections.namedtuple('IndexerResponse', 'response status')
            async with aiohttp.ClientSession() as session:
                async with session.request(indexer.OP_CODE_INDEX, url=indexer_endpoint, json=inverted_map) as r:
                    indexer_s = r.status
                    indexer_r = await r.text()
                    return IndexerResponser(indexer_r, indexer_s)

        # Finally, we'll exit by sending our user to the GUI.
        response_list = await asyncio.gather(issue_request())
        response = response_list[0]
        website_address = 'https://' + config['serviceDescription']['websiteURL']
        if response.status == 200:
            await ctx.reply(f'Messages have been processed by the indexer. Visit {website_address} to see the updates!')

        elif response.status == 199:
            search_terms = response.response.split('no Yelp results:\n')[1]
            await ctx.reply(f'Some messages yielded no results! The following search terms gave no results on the '
                            f'Yelp search: {search_terms}\nBesides that... all other messages have been processed by '
                            f'the indexer. Visit {website_address} to see the updates!')

        else:
            logger.error('Non-200/199 status from our indexer!')
            logger.error(response.response)
            await ctx.reply('Error encountered! Report this to Glenn!')

    class HelpMenuView(discord.ui.View):
        @discord.ui.select(
            options=[
                discord.SelectOption(value='website', label="What is the website again?"),
                discord.SelectOption(value='refresh', label="How do I add a location keyword to the website?"),
                discord.SelectOption(value='delete', label="How do I delete a location from the website?")
            ],
            custom_id='help_menu'
        )
        async def select_callback(self, interaction, select):
            selected_option = select.values[0]
            logger.debug(f'User has selected option "{selected_option}".')

            # Execute the help-menu command.
            if selected_option == 'website':
                website_address = 'https://' + config['serviceDescription']['websiteURL']
                return await interaction.response.send_message(f'The website is located at: {website_address}')
            elif selected_option == 'refresh':
                whitelisted_emojis = config['discordSearch']['emojiWhitelist']
                return await interaction.response.send_message(
                    f'To add a new location to the website, add a new message to any channel and react with '
                    f'one of the following emojis: [{",".join(whitelisted_emojis)}]. You can also react '
                    f'to an existing message with one of previous emojis. Once react-ed, enter the command '
                    f'"!refresh" into any channel.')
            elif selected_option == 'delete':
                return await interaction.response.send_message(
                    'To delete a location from the website, hover over the location you want to delete on '
                    'the map and click "Delete". To simply see less points, use the range slide directly '
                    'below the map.')

    @bot.command(name='website')
    async def website_command(ctx: discord.ext.commands.Context):
        await ctx.send('I am JapanTripBot! What do you need help with?', view=HelpMenuView())

    # Start our bot.
    logger.info('Starting bot.')
    bot.run(os.getenv('DISCORD_TOKEN'), log_handler=None)

import discord
from discord import app_commands
from discord.ext import commands, tasks
import MySQLdb
import feedparser
from datetime import datetime, timedelta
import time
import asyncio
import socket
import urllib.error
from typing import List
import raccoonss_config

# Database configuration - replace with your actual credentials
DB_HOST = raccoonss_config.DB_HOST
DB_USER = raccoonss_config.DB_USER
DB_PASSWORD = raccoonss_config.DB_PASSWORD
DB_NAME = raccoonss_config.DB_NAME

# Bot configuration - replace with your bot token
BOT_TOKEN = raccoonss_config.BOT_TOKEN

# Setup database connection
def get_db_connection():
    return MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)

# Create tables if they don't exist
def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feeds (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(255) UNIQUE,
            last_refresh DATETIME,
            failure_start DATETIME
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_feeds (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guild_id BIGINT,
            feed_id INT,
            last_post_timestamp DATETIME,
            FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE CASCADE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_feed_channels (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guild_feed_id INT,
            channel_id BIGINT,
            failure_count INT DEFAULT 0,
            FOREIGN KEY (guild_feed_id) REFERENCES guild_feeds(id) ON DELETE CASCADE
        )
    """)
    # Add columns if not exists
    cursor.execute("ALTER TABLE feeds ADD COLUMN IF NOT EXISTS failure_start DATETIME")
    cursor.execute("ALTER TABLE guild_feed_channels ADD COLUMN IF NOT EXISTS failure_count INT DEFAULT 0")
    conn.commit()
    cursor.close()
    conn.close()

setup_database()

intents = discord.Intents.default()
intents.guilds = True
intents.messages = True  # Needed for sending messages

bot = commands.Bot(command_prefix='!', intents=intents)  # Prefix not used, but required

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} command(s)')
    except Exception as e:
        print(e)
    rss_checker.start()

class ChannelSelectView(discord.ui.View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=300)
        
        # Create the select and keep a direct reference
        self.channel_select = discord.ui.ChannelSelect(
            placeholder="Select channels to post to",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=25,
        )
        self.add_item(self.channel_select)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.primary)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        self.stop()

@bot.tree.command(name="add_rss", description="Add an RSS feed to monitor")
@app_commands.describe(url="The RSS feed URL")
async def add_rss(interaction: discord.Interaction, url: str):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a guild.", ephemeral=True)
        return

    view = ChannelSelectView(interaction.guild)
    await interaction.response.send_message(
        "Select the channels to post new entries to (text channels only).",
        ephemeral=True,
        view=view
    )

    await view.wait()

    selected_app_channels = view.channel_select.values

    if not selected_app_channels:
        await interaction.edit_original_response(content="Timed out or no channels selected. Please try again.")
        return

    # Resolve AppCommandChannel → full discord.TextChannel objects
    valid_channels = []
    invalid_channels = []
    guild_me = interaction.guild.me

    for app_ch in selected_app_channels:
        full_ch = interaction.guild.get_channel(app_ch.id)
        if full_ch is None or not isinstance(full_ch, discord.TextChannel):
            invalid_channels.append(f"#{app_ch.name or app_ch.id} (could not resolve)")
            continue

        perms = full_ch.permissions_for(guild_me)
        if perms.view_channel and perms.send_messages:
            valid_channels.append(full_ch)
        else:
            reasons = []
            if not perms.view_channel:
                reasons.append("View Channel")
            if not perms.send_messages:
                reasons.append("Send Messages")
            invalid_channels.append(f"{full_ch.mention} ({', '.join(reasons)})")

    if not valid_channels:
        await interaction.edit_original_response(
            content="None of the selected channels allow me to view and send messages.\n"
                    "Please grant the necessary permissions and try again.",
            ephemeral=True
        )
        return

    # Validate feed
    feed = feedparser.parse(url)
    if feed.bozo:
        await interaction.edit_original_response(content=f"Invalid RSS feed: {feed.bozo_exception}", ephemeral=True)
        return

    entries = feed.entries
    last_timestamp = None
    if entries:
        published_times = [
            datetime.fromtimestamp(time.mktime(e.published_parsed))
            for e in entries
            if 'published_parsed' in e and e.published_parsed
        ]
        if published_times:
            last_timestamp = max(published_times)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM feeds WHERE url = %s", (url,))
    result = cursor.fetchone()
    if result:
        feed_id = result[0]
    else:
        cursor.execute("INSERT INTO feeds (url, last_refresh, failure_start) VALUES (%s, NULL, NULL)", (url,))
        feed_id = cursor.lastrowid

    cursor.execute("SELECT id FROM guild_feeds WHERE guild_id = %s AND feed_id = %s", (interaction.guild.id, feed_id))
    if cursor.fetchone():
        await interaction.edit_original_response(content="This feed is already added to this guild.", ephemeral=True)
        conn.close()
        return

    cursor.execute(
        "INSERT INTO guild_feeds (guild_id, feed_id, last_post_timestamp) VALUES (%s, %s, %s)",
        (interaction.guild.id, feed_id, last_timestamp)
    )
    guild_feed_id = cursor.lastrowid

    for channel in valid_channels:
        cursor.execute(
            "INSERT INTO guild_feed_channels (guild_feed_id, channel_id, failure_count) VALUES (%s, %s, 0)",
            (guild_feed_id, channel.id)
        )

    conn.commit()
    cursor.close()
    conn.close()

    msg = f"Added RSS feed: {url}\nPosting to: {', '.join(c.mention for c in valid_channels)}"
    if invalid_channels:
        msg += f"\n\nSkipped: {', '.join(invalid_channels)}"

    await interaction.edit_original_response(content=msg, view=None)

class FeedSelect(discord.ui.Select):
    def __init__(self, feeds):
        options = [discord.SelectOption(label=url, value=str(fid)) for fid, url in feeds]
        super().__init__(placeholder="Select feeds to remove", min_values=1, max_values=len(options), options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.stop()

class RemoveFeedView(discord.ui.View):
    def __init__(self, feeds):
        super().__init__(timeout=300)
        self.add_item(FeedSelect(feeds))

@bot.tree.command(name="remove_rss", description="Remove an RSS feed from this guild")
async def remove_rss(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a guild.", ephemeral=True)
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT f.id, f.url FROM feeds f
        JOIN guild_feeds gf ON f.id = gf.feed_id
        WHERE gf.guild_id = %s
    """, (interaction.guild.id,))
    feeds = cursor.fetchall()
    cursor.close()
    conn.close()

    if not feeds:
        await interaction.response.send_message("No feeds added to this guild.", ephemeral=True)
        return

    view = RemoveFeedView(feeds)
    await interaction.response.send_message("Select feeds to remove.", ephemeral=True, view=view)

    await view.wait()

    if view.timeout:
        await interaction.edit_original_response(content="Timed out. Please try again.")
        return

    selected_feed_ids = view.children[0].values

    conn = get_db_connection()
    cursor = conn.cursor()
    for fid in selected_feed_ids:
        cursor.execute("DELETE FROM guild_feeds WHERE guild_id = %s AND feed_id = %s", (interaction.guild.id, int(fid)))

    conn.commit()
    cursor.close()
    conn.close()

    await interaction.edit_original_response(content="Selected feeds removed.")

@tasks.loop(minutes=1)
async def rss_checker():
    now = datetime.now()
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get feeds that need refresh
    cursor.execute("""
        SELECT id, url FROM feeds
        WHERE last_refresh IS NULL OR last_refresh < %s
    """, (now - timedelta(minutes=15),))
    feeds_to_refresh = cursor.fetchall()

    for feed_id, url in feeds_to_refresh:
        feed = feedparser.parse(url)

        is_unavailable = feed.bozo and isinstance(feed.bozo_exception, (urllib.error.URLError, urllib.error.HTTPError, socket.timeout))

        if is_unavailable:
            cursor.execute("SELECT failure_start FROM feeds WHERE id = %s", (feed_id,))
            failure_start = cursor.fetchone()[0]
            if failure_start is None:
                cursor.execute("UPDATE feeds SET failure_start = %s WHERE id = %s", (now, feed_id))
            elif (now - failure_start) > timedelta(hours=48):
                cursor.execute("DELETE FROM feeds WHERE id = %s", (feed_id,))
                continue  # Skip further processing
        else:
            cursor.execute("UPDATE feeds SET failure_start = NULL, last_refresh = %s WHERE id = %s", (now, feed_id))
            entries = [e for e in feed.entries if 'published_parsed' in e]
            entries.sort(key=lambda e: datetime.fromtimestamp(time.mktime(e.published_parsed)))  # Ascending

            # Get all guild_feeds for this feed
            cursor.execute("SELECT id, guild_id, last_post_timestamp FROM guild_feeds WHERE feed_id = %s", (feed_id,))
            guild_feeds = cursor.fetchall()

            for gf_id, guild_id, last_ts in guild_feeds:
                if last_ts is None:
                    last_ts = datetime.min

                new_entries = [e for e in entries if datetime.fromtimestamp(time.mktime(e.published_parsed)) > last_ts]

                if not new_entries:
                    continue

                # Get channels
                cursor.execute("SELECT channel_id FROM guild_feed_channels WHERE guild_feed_id = %s", (gf_id,))
                channel_ids = [row[0] for row in cursor.fetchall()]

                guild = bot.get_guild(guild_id)
                if not guild:
                    continue

                new_max_ts = last_ts
                for entry in new_entries:
                    title = entry.title
                    summary = entry.summary if 'summary' in entry else ''
                    link = entry.link

                    view = discord.ui.View()
                    view.add_item(discord.ui.Button(label="Read More", url=link, style=discord.ButtonStyle.link))

                    message = f"# {title}\n{summary}"

                    sent_count = 0
                    for ch_id in channel_ids:
                        channel = guild.get_channel(ch_id)
                        if not channel:
                            cursor.execute("DELETE FROM guild_feed_channels WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                            continue

                        try:
                            await channel.send(message, view=view)
                            cursor.execute("UPDATE guild_feed_channels SET failure_count = 0 WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                            sent_count += 1
                        except discord.NotFound:
                            print(f"Channel {ch_id} not found in guild {guild_id}. Removing.")
                            cursor.execute("DELETE FROM guild_feed_channels WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                        except discord.Forbidden:
                            print(f"Forbidden to send to {ch_id} in guild {guild_id}.")
                            cursor.execute("UPDATE guild_feed_channels SET failure_count = failure_count + 1 WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                            cursor.execute("SELECT failure_count FROM guild_feed_channels WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                            fc = cursor.fetchone()
                            if fc and fc[0] >= 3:
                                print(f"Removing channel {ch_id} after 3 failures.")
                                cursor.execute("DELETE FROM guild_feed_channels WHERE guild_feed_id = %s AND channel_id = %s", (gf_id, ch_id))
                        except discord.HTTPException as e:
                            print(f"HTTP error sending to {ch_id}: {e}")
                            # Do not increment failure_count for transient errors
                        except Exception as e:
                            print(f"Unexpected error sending to {ch_id}: {e}")

                    if sent_count > 0:
                        entry_ts = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                        if entry_ts > new_max_ts:
                            new_max_ts = entry_ts

                # Update last_post_timestamp if advanced
                if new_max_ts > last_ts:
                    cursor.execute("UPDATE guild_feeds SET last_post_timestamp = %s WHERE id = %s", (new_max_ts, gf_id))

    conn.commit()
    cursor.close()
    conn.close()

bot.run(BOT_TOKEN)

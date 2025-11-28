import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import os
import random
import math
from dotenv import load_dotenv
from datetime import datetime, timedelta
import asyncio # For confirmation buttons
import json # For JSON file operations

# Load token from .env
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True # Required for message content for wait_for

# --- Global data storage and file persistence ---
DATA_FILE = "data.json"

# Default placeholder image for albums
DEFAULT_ALBUM_IMAGE = "https://placehold.co/128x128.png?text=Album"

STREAM_TICK_MINUTES = 60  # Interval for automated stream ticks
STREAM_HISTORY_LIMIT = 96  # Keep the most recent N ticks (4 days of hourly data)
GP_DECAY_RATE = 0.08  # Decay rate for general public interest
GP_INTEREST_STREAM_WEIGHT = 120  # Multiplier converting GP interest to streams
PROMO_STREAM_WEIGHT = 100  # Multiplier converting promo power to streams during active promo

# Initialize global dictionaries. These will be loaded from data.json on startup.
group_popularity = {} # group_name: popularity_score
company_funds = {} # company_name: funds
group_data = {}  # group_name: dict with 'company', 'albums', 'korean_name', 'wins', 'popularity', 'debut_date', 'is_disbanded'
album_data = {}  # album_name: dict with 'group', 'wins', 'release_date', 'streams', 'sales', 'image_url', 'is_active_promotion', 'promotion_end_date', 'charts_info', 'fanbase_size', 'gp_interest', 'promo_power', 'virality_potential', 'stream_history'
user_balances = {}  # user_id: balance
user_companies = {}  # user_id: [company_name1, company_name2, ...]
user_cooldowns = {} # user_id: {command_name: last_used_datetime}
user_daily_limits = {} # user_id: {command_name: {date: count}}
latest_chart_snapshot = {}  # Stores the last generated chart snapshot for reference

def _generate_album_attributes_for_group(group_name: str):
    group_popularity_score = group_data.get(group_name, {}).get('popularity', 100)
    return {
        'fanbase_size': max(1500, int(group_popularity_score * 25)),
        'gp_interest': max(40, int(group_popularity_score * 0.9)),
        'promo_power': max(10, int(group_popularity_score * 0.35)),
        'virality_potential': round(min(0.35, 0.05 + group_popularity_score / 5000), 3),
    }


def _normalize_promotion_state(album_entry: dict):
    promo_date_value = album_entry.get('promotion_end_date')
    if isinstance(promo_date_value, str):
        try:
            album_entry['promotion_end_date'] = datetime.fromisoformat(promo_date_value)
        except ValueError:
            album_entry['promotion_end_date'] = None
    elif promo_date_value is not None and not isinstance(promo_date_value, datetime):
        album_entry['promotion_end_date'] = None

    # Expire promotions that have passed their end date
    if album_entry.get('promotion_end_date') and datetime.now() > album_entry['promotion_end_date']:
        album_entry['is_active_promotion'] = False
        album_entry['promotion_end_date'] = None


def _ensure_album_defaults(album_name: str, album_entry: dict):
    group_name = album_entry.get('group')
    baseline_attrs = _generate_album_attributes_for_group(group_name) if group_name else {
        'fanbase_size': 2000,
        'gp_interest': 60,
        'promo_power': 25,
        'virality_potential': 0.08,
    }

    for key, value in baseline_attrs.items():
        album_entry.setdefault(key, value)

    album_entry.setdefault('stream_history', [])
    _normalize_promotion_state(album_entry)
    return album_entry

def load_data():
    """Loads data from data.json into global dictionaries."""
    global group_popularity, company_funds, group_data, album_data, user_balances, user_companies, user_cooldowns, user_daily_limits, latest_chart_snapshot

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            try:
                loaded_data = json.load(f)
                group_popularity.update(loaded_data.get('group_popularity', {}))
                company_funds.update(loaded_data.get('company_funds', {}))

                loaded_group_data = loaded_data.get('group_data', {})
                for group_name, data in loaded_group_data.items():
                    data.setdefault('is_disbanded', False) # Add is_disbanded field with default False
                    group_data[group_name] = data

                loaded_album_data = loaded_data.get('album_data', {})
                for album_name, data in loaded_album_data.items():
                    data.setdefault('streams', 0)
                    data.setdefault('sales', 0)
                    data.setdefault('image_url', DEFAULT_ALBUM_IMAGE)
                    data.setdefault('is_active_promotion', False)
                    _ensure_album_defaults(album_name, data)

                    # Ensure charts_info structure is always present
                    data.setdefault('charts_info', {})
                    for chart_type_key in ["MelOn", "Genie", "Bugs", "FLO"]:
                        data['charts_info'].setdefault(chart_type_key, {'rank': None, 'peak': None, 'prev_rank': None})

                    album_data[album_name] = data

                user_balances.update(loaded_data.get('user_balances', {}))
                
                latest_chart_snapshot.update(loaded_data.get('latest_chart_snapshot', {}))

                # Handle user_companies: convert single string to list if old format
                # Ensure user_companies is properly loaded, defaulting to an empty dict if not found
                user_companies.update(loaded_data.get('user_companies', {}))
                for user_id, companies in user_companies.items(): # Iterate over what was just loaded
                    if isinstance(companies, str):
                        user_companies[user_id] = [companies] # Convert old string format to list
                    else:
                        user_companies[user_id] = companies # Assume it's already a list or empty


                # Load cooldowns, converting ISO strings back to datetime objects
                loaded_cooldowns = loaded_data.get('user_cooldowns', {})
                for user_id, commands in loaded_cooldowns.items():
                    user_cooldowns[user_id] = {
                        cmd: datetime.fromisoformat(ts) for cmd, ts in commands.items()
                    }

                user_daily_limits.update(loaded_data.get('user_daily_limits', {}))

                print("Data loaded from data.json successfully!")
            except json.JSONDecodeError:
                print("Error decoding data.json. Starting with empty data.")
    else:
        print("data.json not found. Starting with empty data.")

def save_data():
    """Saves global dictionaries to data.json."""
    data_to_save = {
        'group_popularity': group_popularity,
        'company_funds': company_funds,
        'group_data': group_data,
        'album_data': album_data,
        'user_balances': user_balances,
        'user_cooldowns': user_cooldowns,
        'user_daily_limits': user_daily_limits,
        'user_companies': user_companies, # Ensure user_companies is always saved
        'latest_chart_snapshot': latest_chart_snapshot
    }
    # Custom encoder for datetime objects
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return json.JSONEncoder.default(self, obj)

    with open(DATA_FILE, 'w') as f:
        json.dump(data_to_save, f, indent=4, cls=DateTimeEncoder)
    print("Data saved to data.json.")

# --- Bot Setup ---
class MyBot(commands.Bot):
    async def setup_hook(self):
        print("Setting up bot...")
        load_data()
        await self.tree.sync()
        print(f'Bot {bot.user} has synced slash commands.')

bot = MyBot(command_prefix="/", intents=intents)

# --- EVENTS ---
@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    if not stream_tick_loop.is_running():
        stream_tick_loop.start()

# === UTILS ===
def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"

def format_number(num):
    if num >= 1_000_000_000: return f"{num / 1_000_000_000:.1f}B"
    if num >= 1_000_000: return f"{num / 1_000_000:.1f}M"
    if num >= 1_000: return f"{num / 1_000:.1f}K"
    return str(num)

def get_user_companies(user_id: str):
    """Returns a list of company names owned by the user."""
    return user_companies.get(user_id, [])

def is_user_company_owner(user_id: str, company_name: str):
    """Checks if a user owns a specific company."""
    return company_name in get_user_companies(user_id)

def get_group_owner_company(group_name: str):
    """Returns the company name that owns the given group."""
    return group_data.get(group_name, {}).get('company')

def is_user_group_owner(user_id: str, group_name: str):
    """Checks if a user owns the company that manages a specific group."""
    group_owner_company = get_group_owner_company(group_name)
    return group_owner_company and is_user_company_owner(user_id, group_owner_company)

def check_cooldown(user_id: str, command_name: str, cooldown_minutes: int):
    """Checks if a user is on cooldown for a specific command."""
    last_used = user_cooldowns.get(user_id, {}).get(command_name)
    if last_used and (datetime.now() - last_used) < timedelta(minutes=cooldown_minutes):
        remaining = timedelta(minutes=cooldown_minutes) - (datetime.now() - last_used)
        return True, remaining
    return False, None

def update_cooldown(user_id: str, command_name: str):
    """Updates the cooldown for a user and command."""
    if user_id not in user_cooldowns:
        user_cooldowns[user_id] = {}
    user_cooldowns[user_id][command_name] = datetime.now()
    save_data()

def check_daily_limit(user_id: str, command_name: str, max_uses: int):
    """Checks and updates daily command usage."""
    today = datetime.now().strftime("%Y-%m-%d")
    user_data = user_daily_limits.setdefault(user_id, {})
    command_data = user_data.setdefault(command_name, {})

    current_uses = command_data.get(today, 0)

    if current_uses >= max_uses:
        return True, max_uses - current_uses # True for limited, 0 remaining

    command_data[today] = current_uses + 1
    save_data()
    return False, max_uses - (current_uses + 1) # False for not limited, remaining uses

    def _get_release_date(album_entry: dict):
    release_date_raw = album_entry.get('release_date')
    if isinstance(release_date_raw, datetime):
        return release_date_raw
    try:
        return datetime.fromisoformat(release_date_raw)
    except (TypeError, ValueError):
        return datetime.now()


def _calculate_stream_tick(album_name: str, album_entry: dict):
    _ensure_album_defaults(album_name, album_entry)

    group_name = album_entry.get('group')
    if group_name and group_data.get(group_name, {}).get('is_disbanded'):
        return 0, {}

    release_dt = _get_release_date(album_entry)
    days_since_release = max(0, (datetime.now() - release_dt).days)

    fanbase_core = int(album_entry.get('fanbase_size', 0) * random.uniform(0.8, 1.15))

    gp_interest = album_entry.get('gp_interest', 0)
    gp_decay = gp_interest * GP_INTEREST_STREAM_WEIGHT * math.exp(-GP_DECAY_RATE * days_since_release)
    gp_decay_streams = int(gp_decay)

    promo_boost = 0
    if album_entry.get('is_active_promotion') and album_entry.get('promotion_end_date'):
        if datetime.now() <= album_entry['promotion_end_date']:
            promo_boost = int(album_entry.get('promo_power', 0) * PROMO_STREAM_WEIGHT * random.uniform(0.8, 1.2))

    virality_spike = 0
    virality_triggered = False
    virality_chance = album_entry.get('virality_potential', 0)
    if random.random() < virality_chance:
        virality_triggered = True
        max_spike = max(5000, album_entry.get('fanbase_size', 0) * 20)
        virality_spike = min(max_spike, int((fanbase_core + gp_decay_streams + promo_boost) * random.uniform(0.5, 2.0)))

    total_streams_added = fanbase_core + gp_decay_streams + promo_boost + virality_spike

    return total_streams_added, {
        'fanbase_core': fanbase_core,
        'gp_decay': gp_decay_streams,
        'promo_boost': promo_boost,
        'virality_spike': virality_spike,
        'virality_triggered': virality_triggered
    }


@tasks.loop(minutes=STREAM_TICK_MINUTES)
async def stream_tick_loop():
    for album_name, album_entry in album_data.items():
        _normalize_promotion_state(album_entry)
        streams_added, components = _calculate_stream_tick(album_name, album_entry)
        if streams_added <= 0:
            continue

        album_entry['streams'] = album_entry.get('streams', 0) + streams_added
        history = album_entry.setdefault('stream_history', [])
        history.append({
            'timestamp': datetime.now().isoformat(),
            'increment': streams_added,
            'components': components,
            'total_streams': album_entry['streams'],
        })
        if len(history) > STREAM_HISTORY_LIMIT:
            album_entry['stream_history'] = history[-STREAM_HISTORY_LIMIT:]
        _ensure_album_defaults(album_name, album_entry)

    save_data()


@stream_tick_loop.before_loop
async def before_stream_tick_loop():
    await bot.wait_until_ready()

# === DECORATORS ===
def is_admin():
    async def predicate(interaction: discord.Interaction):

        admin_user_ids = [123456789012345678, 987654321098765432] # Replace with actual admin user IDs
        if interaction.user.id == bot.owner_id or interaction.user.id in admin_user_ids:
            return True
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return False
    return app_commands.check(predicate)

# === BASIC ECONOMY ===
@bot.tree.command(description="Check your balance")
async def balance(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    bal = user_balances.get(user_id, 0)
    await interaction.response.send_message(f"💰 {interaction.user.display_name}, your balance is <:MonthlyPeso:1338642658436059239>{bal:,}.")

@bot.tree.command(description="Work to earn money")
async def work(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    current_bal = user_balances.get(user_id, 0)
    pay = random.randint(10000, 50000)
    new_bal = current_bal + pay
    user_balances[user_id] = new_bal
    save_data() 
    await interaction.response.send_message(f"💼 {interaction.user.display_name}, you worked and earned <:MonthlyPeso:1338642658436059239>{pay:,}!")

@bot.tree.command(description="Claim your daily money reward")
async def daily(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    on_cooldown, remaining_time = check_cooldown(user_id, "daily", 24 * 60) # 24 hours cooldown
    if on_cooldown:
        hours, remainder = divmod(remaining_time.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        await interaction.response.send_message(f"⏳ You can claim your daily reward again in {hours}h {minutes}m {seconds}s.", ephemeral=True)
        return

    current_bal = user_balances.get(user_id, 0)
    pay = 100_000
    new_bal = current_bal + pay
    user_balances[user_id] = new_bal
    update_cooldown(user_id, "daily") # Update cooldown for daily
    save_data() 
    await interaction.response.send_message(f"🖖 {interaction.user.display_name}, you claimed your daily <:MonthlyPeso:1338642658436059239>{pay:,}!")

@bot.tree.command(description="Invest in a company")
async def invest(interaction: discord.Interaction, company_name: str, amount: int):
    user_id = str(interaction.user.id)
    user_bal = user_balances.get(user_id, 0)

    if user_bal < amount:
        await interaction.response.send_message("❌ Not enough funds to invest.")
        return

    company_name_upper = company_name.upper()

    if company_name_upper not in company_funds:
        await interaction.response.send_message("❌ Company not found.")
        return

    # Check if the user actually owns this company
    if not is_user_company_owner(user_id, company_name_upper):
        await interaction.response.send_message(f"❌ You do not own the company `{company_name}`.", ephemeral=True)
        return

    current_company_funds = company_funds.get(company_name_upper, 0)

    user_balances[user_id] = user_bal - amount
    company_funds[company_name_upper] = current_company_funds + amount
    save_data() 

    await interaction.response.send_message(f"📉 You invested <:MonthlyPeso:1338642658436059239>{amount:,} in {company_name}!")

@bot.tree.command(description="Check a company's funds")
async def companyfunds(interaction: discord.Interaction, company_name: str):
    company_name_upper = company_name.upper()

    if company_name_upper not in company_funds:
        await interaction.response.send_message(f"❌ Company `{company_name}` not found.")
    else:
        funds = company_funds.get(company_name_upper, 0)
        await interaction.response.send_message(f"🏢 {company_name}'s Funds: <:MonthlyPeso:1338642658436059239>{funds:,}")

@bot.tree.command(description="Buy an album!")
async def sales(interaction: discord.Interaction, album_name: str):
    user_id = str(interaction.user.id)
    on_cooldown, remaining_time = check_cooldown(user_id, "sales", 2) # 2 minutes cooldown
    if on_cooldown:
        await interaction.response.send_message(f"⏳ You are on cooldown for sales. Please wait {remaining_time.seconds} seconds.", ephemeral=True)
        return

    if album_name not in album_data:
        await interaction.response.send_message("❌ Album not found. Are you sure you typed the name correctly?")
        return

    current_album_data = album_data[album_name]
    _ensure_album_defaults(album_name, current_album_data)
    group_name = current_album_data.get('group')
    if not group_name:
        await interaction.response.send_message("❌ Album does not have an associated group.")
        return

    if group_name not in group_data:
        await interaction.response.send_message("❌ Associated group not found. Cannot calculate sales bonus.")
        return

    if group_data[group_name].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot buy sales for {group_name} as they are disbanded.", ephemeral=True)
        return

    group_current_popularity = group_data[group_name].get('popularity', 0)

    # Increased sales amount
    base_sales = group_current_popularity * 50 # Increased from 10
    sales_to_add = max(50, int(random.gauss(mu=base_sales, sigma=group_current_popularity * 25))) # Increased sigma

    current_album_data['sales'] = current_album_data.get('sales', 0) + sales_to_add
    album_data[album_name] = current_album_data 

    # Add money to the company's bank (1 Monthly Peso per sale)
    company_name = group_data[group_name]['company']
    if company_name in company_funds:
        company_funds[company_name] += sales_to_add # Add sales amount as funds
    else:
        company_funds[company_name] = sales_to_add # Initialize if not exists (shouldn't happen with checks)

    update_cooldown(user_id, "sales")
    save_data() 
    embed = discord.Embed(
        title=f"📈 You bought copies of '{album_name}'!",
        description=f"**{group_name}** • Album",
        color=discord.Color.green()
    )
    embed.set_thumbnail(url=current_album_data.get('image_url', DEFAULT_ALBUM_IMAGE))
    embed.add_field(name="Copies Added", value=f"{format_number(sales_to_add)}", inline=True)
    embed.set_footer(text=f"Total Sales: {format_number(current_album_data['sales'])}")

    await interaction.response.send_message(embed=embed)

    # Check for low sales and send public message
    if sales_to_add < 10000:
        try:
            await interaction.channel.send("someone tag nugupromoter <:lmfaooo:1162576419486974022>")
        except discord.errors.Forbidden:
            print(f"ERROR: Missing permissions to send public 'nugupromoter' message in channel {interaction.channel.id}")


@bot.tree.command(description="Add a music show win to a group and album")
async def addwin(interaction: discord.Interaction, group_name: str, show_name: str, album_name: str):
    group_name_upper = group_name.upper()

    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.")
        return
    if album_name not in album_data or album_data[album_name].get('group') != group_name_upper:
        await interaction.response.send_message(f"❌ Album `{album_name}` not found or does not belong to `{group_name}`.")
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot add a win for {group_name_upper} as they are disbanded.", ephemeral=True)
        return


    group_entry = group_data[group_name_upper]
    album_entry = album_data[album_name]

    # Increment album's specific wins
    album_entry['wins'] = album_entry.get('wins', 0) + 1

    # Calculate group's total wins dynamically from all its albums
    group_total_wins = 0
    for alb_n in group_entry.get('albums', []):
        if alb_n in album_data:
            group_total_wins += album_data[alb_n].get('wins', 0)

    # Update the group's total wins (for consistency in group_data, even if not directly incremented)
    group_entry['wins'] = group_total_wins 

    # Increase group popularity
    group_entry['popularity'] = group_entry.get('popularity', 0) + 5  

    save_data() 

    # Generate hashtags with ordinal numbers
    group_hashtag = f"#{group_name_upper.replace(' ', '')}{ordinal(group_total_wins)}Win"
    album_hashtag = f"#{album_name.replace(' ', '')}{ordinal(album_entry['wins'])}Win"

    await interaction.response.send_message(
        f"🎉 {group_name_upper} takes 1st Place on {show_name} with '{album_name}'!\n\n"
        f"**{group_hashtag}** **{album_hashtag}**"
    )

@bot.tree.command(description="Simulate a group posting on social media")
async def newpost(interaction: discord.Interaction, group_name: str):
    group_name_upper = group_name.upper()
    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.")
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot create a new post for {group_name_upper} as they are disbanded.", ephemeral=True)
        return

    group_entry = group_data[group_name_upper]
    base_pop = group_entry.get('popularity', 0)

    # Adjusted likes and comments calculation for more realistic scaling
    # Increased sensitivity to popularity for likes and comments
    likes = max(100, int(random.gauss(mu=base_pop * 30, sigma=base_pop * 3))) 
    comments = max(10, int(random.gauss(mu=base_pop * 3, sigma=base_pop * 0.3)))

    # Adjusted popularity gain
    popularity_gain = random.randint(1, 5) # Smaller base popularity gain
    message_suffix = ""

    # Check for viral event
    if random.random() < 0.15: # 15% chance to go viral
        viral_popularity_boost = random.randint(15, 30) # Smaller viral boost but still significant
        popularity_gain += viral_popularity_boost
        message_suffix = f"\n✨ Your post went **VIRAL**! Popularity boosted by an additional {viral_popularity_boost}!"

    group_entry['popularity'] = group_entry.get('popularity', 0) + popularity_gain

    likes_display = format_number(likes)
    comments_display = format_number(comments)

    save_data() # Save data after modification

    # This message is ephemeral (response to interaction), so it's only seen by the user who invoked it
    await interaction.response.send_message(
        f"❤️💬 {likes_display} Likes | {comments_display} Comments",
    )
    await interaction.followup.send(
        f"**Popularity increased by {popularity_gain}.**{message_suffix}",
        ephemeral=True
    )

# === FEATURES IMPLEMENTATION ===

@bot.tree.command(description="Stream an album and add to its count!")
async def streams(interaction: discord.Interaction, album_name: str):
    user_id = str(interaction.user.id)
    on_cooldown, remaining_time = check_cooldown(user_id, "streams", 2) # 2 minutes cooldown
    if on_cooldown:
        await interaction.response.send_message(f"⏳ You are on cooldown for sales. Please wait {remaining_time.seconds} seconds.", ephemeral=True)
        return

    if album_name not in album_data:
        await interaction.response.send_message(f"❌ Album '{album_name}' not found. Are you sure you typed the name correctly?")
        return

    current_album_data = album_data[album_name]
    group_name = current_album_data.get('group')
    if not group_name:
        await interaction.response.send_message("❌ Album does not have an associated group.")
        return

    if group_name not in group_data:
        await interaction.response.send_message("❌ Associated group not found. Cannot calculate stream bonus.")
        return

    if group_data[group_name].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot stream for {group_name} as they are disbanded.", ephemeral=True)
        return

    group_current_popularity = group_data[group_name].get('popularity', 0)

    base_streams = group_current_popularity * 100
    streams_to_add = max(100, int(random.gauss(mu=base_streams, sigma=group_current_popularity * 50)))

    current_album_data['streams'] = current_album_data.get('streams', 0) + streams_to_add
    album_data[album_name] = current_album_data 

    update_cooldown(user_id, "streams")
    save_data() 
    embed = discord.Embed(
        title=album_name, 
        description=f"**{group_name}** • Album",
        color=discord.Color.blue()
    )
    embed.set_thumbnail(url=current_album_data.get('image_url', DEFAULT_ALBUM_IMAGE))
    embed.add_field(name="Streams Added", value=f"{format_number(streams_to_add)}", inline=True)
    embed.set_footer(text=f"Total Streams: {format_number(current_album_data['streams'])}") 

    await interaction.response.send_message(embed=embed)

    # Check for low streams and send public message
    if streams_to_add < 10000:
        try:
            await interaction.channel.send("someone tag nugupromoter <:lmfaooo:1162576419486974022>")
        except discord.errors.Forbidden:
            print(f"ERROR: Missing permissions to send public 'nugupromoter' message in channel {interaction.channel.id}")


@bot.tree.command(description="Make your group perform to gain popularity!")
@app_commands.describe(group_name="The name of your group.")
async def perform(interaction: discord.Interaction, group_name: str):
    user_id = str(interaction.user.id)
    group_name_upper = group_name.upper()

    is_limited, remaining_uses = check_daily_limit(user_id, "perform", 10) # Increased to 10 uses
    if is_limited:
        await interaction.response.send_message(f"❌ You have reached your daily limit of 10 performances. Remaining uses today: {remaining_uses}.", ephemeral=True)
        return

    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.", ephemeral=True)
        return

    if not is_user_group_owner(user_id, group_name_upper):
        await interaction.response.send_message(f"❌ You do not manage the company that owns '{group_name}'.", ephemeral=True)
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot perform with {group_name_upper} as they are disbanded.", ephemeral=True)
        return

    group_entry = group_data[group_name_upper]
    popularity_gain = random.randint(10, 30) # Small popularity gain

    group_entry['popularity'] = group_entry.get('popularity', 0) + popularity_gain
    save_data()

    await interaction.response.send_message(
        f"🎤 **{group_name_upper}** performed live! Their popularity increased by **{popularity_gain}**."
        f" (New popularity: {group_entry['popularity']}).\n"
        f"You have {remaining_uses} performances remaining today.",
        ephemeral=False # Public message for general announcement
    )


# --- Sponsorship Gamble Logic ---

SPONSORSHIP_DEALS = {
    "COCA_COLA": {
        "min_popularity": 150,
        "base_amount": 350_000, # Reduced from 500k
        "popularity_gain": (5, 25), # min, max
        "stream_gain": (5000, 15000),
        "sales_gain": (500, 1500),
        "description": "A refreshing partnership for rising stars!"
    },
    "MIU_MIU": {
        "min_popularity": 300,
        "base_amount": 1_400_000, # Reduced from 2M
        "popularity_gain": (20, 40),
        "stream_gain": (10000, 30000),
        "sales_gain": (100, 300),
        "description": "Elegance meets artistry in this high-end collaboration."
    },
    "SAMSUNG": {
        "min_popularity": 500,
        "base_amount": 3_500_000, # Reduced from 5M
        "popularity_gain": (40, 70),
        "stream_gain": (20000, 50000),
        "sales_gain": (200, 500),
        "description": "Innovate your image with a leading tech brand."
    },
    # --- New Exclusive Brands (also reduced) ---
    "PEPSI": {
        "min_popularity": 200,
        "base_amount": 500_000, # Reduced from 750k
        "popularity_gain": (10, 30),
        "stream_gain": (7000, 20000),
        "sales_gain": (700, 2000),
        "description": "A new generation of pop with an iconic beverage brand."
    },
    "SKYY": {
        "min_popularity": 250,
        "base_amount": 700_000, # Reduced from 1M
        "popularity_gain": (15, 35),
        "stream_gain": (8000, 22000),
        "sales_gain": (800, 2200),
        "description": "Shine brighter with this premium spirit partnership."
    },
    "ALLURE": {
        "min_popularity": 400,
        "base_amount": 2_000_000, # Reduced from 3M
        "popularity_gain": (25, 45),
        "stream_gain": (12000, 35000),
        "sales_gain": (120, 350),
        "description": "Capture the essence of beauty and trendsetting."
    },
    "INNISFREE": {
        "min_popularity": 450,
        "base_amount": 2_500_000, # Reduced from 3.5M
        "popularity_gain": (30, 50),
        "stream_gain": (15000, 40000),
        "sales_gain": (150, 400),
        "description": "Natural beauty, global appeal. A perfect skincare match."
    },
    "PRADA": {
        "min_popularity": 600,
        "base_amount": 5_000_000, # Reduced from 7M
        "popularity_gain": (50, 80),
        "stream_gain": (25000, 60000),
        "sales_gain": (250, 600),
        "description": "Define luxury. A prestigious collaboration."
    },
    "GIVENCHY": {
        "min_popularity": 650,
        "base_amount": 5_200_000, # Reduced from 7.5M
        "popularity_gain": (55, 85),
        "stream_gain": (27000, 65000),
        "sales_gain": (270, 650),
        "description": "Elegance and edge combined in high fashion."
    },
    "GUCCI": {
        "min_popularity": 700,
        "base_amount": 5_500_000, # Reduced from 8M
        "popularity_gain": (60, 90),
        "stream_gain": (30000, 70000),
        "sales_gain": (300, 700),
        "description": "Iconic, daring, and universally desired."
    },
    "CHANEL": {
        "min_popularity": 750,
        "base_amount": 6_000_000, # Reduced from 8.5M
        "popularity_gain": (65, 95),
        "stream_gain": (32000, 75000),
        "sales_gain": (320, 750),
        "description": "Timeless sophistication meets modern artistry."
    },
    "BULGARI": {
        "min_popularity": 800,
        "base_amount": 6_500_000, # Reduced from 9M
        "popularity_gain": (70, 100),
        "stream_gain": (35000, 80000),
        "sales_gain": (350, 800),
        "description": "Italian craftsmanship, global glamour."
    },
    "TIFFANY_AND_CO": {
        "min_popularity": 850,
        "base_amount": 7_000_000, # Reduced from 9.5M
        "popularity_gain": (75, 105),
        "stream_gain": (38000, 85000),
        "sales_gain": (380, 850),
        "description": "The ultimate symbol of luxury and refinement."
    },
    "APPLE": {
        "min_popularity": 1000,
        "base_amount": 8_000_000, # Reduced from 12M
        "popularity_gain": (100, 150),
        "stream_gain": (50000, 100000),
        "sales_gain": (500, 1000),
        "description": "Innovate. Elevate. Dominate. The peak of global influence."
    }
}

class SponsorshipDealView(ui.View):
    def __init__(self, original_interaction: discord.Interaction, group_name: str, investment: int, available_deals: list):
        super().__init__(timeout=60) # Timeout after 60 seconds
        self.original_interaction = original_interaction
        self.group_name = group_name.upper()
        self.investment = investment
        self.chosen_brand_name = None

        # Add buttons for eligible deals
        for brand_name in available_deals:
            details = SPONSORSHIP_DEALS[brand_name]
            button = ui.Button(
                label=f"{brand_name.replace('_', ' ').title()} (Pop: {details['min_popularity']}+)",
                style=discord.ButtonStyle.blurple,
                custom_id=f"sponsorship_{brand_name}"
            )
            button.callback = self.button_callback
            self.add_item(button)

        self.add_item(ui.Button(label="Cancel", style=discord.ButtonStyle.red, custom_id="sponsorship_cancel"))


    async def button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_interaction.user.id:
            await interaction.response.send_message("❌ This is not your sponsorship selection!", ephemeral=True)
            return

        # Correctly extract the brand name by joining parts after the first underscore
        self.chosen_brand_name = '_'.join(interaction.data['custom_id'].split('_')[1:])

        if self.chosen_brand_name == "cancel":
            await interaction.response.edit_message(content="Sponsorship selection cancelled.", view=None)
            self.stop()
            return

        await interaction.response.defer() 

        group_entry = group_data.get(self.group_name)
        if not group_entry:
            await self.original_interaction.edit_original_response(content="❌ Group not found during processing. Please try again.", view=None)
            self.stop()
            return

        if group_entry.get('is_disbanded'):
            await self.original_interaction.edit_original_response(content=f"❌ Cannot seek sponsorship for {self.group_name} as they are disbanded.", view=None)
            self.stop()
            return


        deal_details = SPONSORSHIP_DEALS.get(self.chosen_brand_name)
        if not deal_details:
            await self.original_interaction.edit_original_response(content="❌ Selected brand not found. Please try again.", view=None)
            self.stop()
            return

        group_popularity_val = group_entry.get('popularity', 0)
        base_success_chance = 0.4 # 40% base chance
        popularity_factor = (group_popularity_val / deal_details["min_popularity"]) 
        investment_bonus = self.investment / 500_000 # Example: 1% bonus per 500k invested

        # Total success chance (capped at 95%)
        success_chance = min(0.95, base_success_chance * popularity_factor + investment_bonus)

        outcome_embed = discord.Embed(title="🤝 Sponsorship Outcome", color=discord.Color.light_grey())

        if random.random() < success_chance: # Success!
            outcome_embed.color = discord.Color.green()
            outcome_embed.title = f"🎉 Sponsorship Deal Secured with {self.chosen_brand_name.replace('_', ' ').title()}!"
            outcome_embed.description = f"{self.group_name} has successfully landed the deal!"

            # Apply rewards
            sponsorship_amount = deal_details["base_amount"]
            popularity_gain = random.randint(*deal_details["popularity_gain"])
            stream_gain = random.randint(*deal_details["stream_gain"])
            sales_gain = random.randint(*deal_details["sales_gain"])

            group_entry['popularity'] += popularity_gain

            # Apply streams/sales to a random active album of the group if available
            active_albums = [a_name for a_name, a_data in album_data.items() if a_data.get('group') == self.group_name and a_data.get('is_active_promotion')]
            if active_albums:
                random_album_name = random.choice(active_albums) # Pick an active album
                album_entry = album_data.get(random_album_name)
                if album_entry:
                    album_entry['streams'] = album_entry.get('streams', 0) + stream_gain
                    album_entry['sales'] = album_entry.get('sales', 0) + sales_gain
                    album_data[random_album_name] = album_entry
                    outcome_embed.add_field(name="Album Gains", value=f"• {format_number(stream_gain)} Streams to '{random_album_name}'\n• {format_number(sales_gain)} Sales to '{random_album_name}'", inline=False)
            else:
                outcome_embed.add_field(name="Bonus Streams/Sales", value=f"No active albums to attribute to, but still a success!", inline=False)


            company_name = group_entry.get('company')
            if company_name and company_name in company_funds:
                company_funds[company_name] = company_funds.get(company_name, 0) + sponsorship_amount
                outcome_embed.add_field(name="Funds Gained", value=f"<:MonthlyPeso:1338642658436059239>{format_number(sponsorship_amount)} (New company funds: <:MonthlyPeso:1338642658436059239>{format_number(company_funds[company_name])})", inline=False)
            else:
                outcome_embed.add_field(name="Funds Gained", value=f"<:MonthlyPeso:1338642658436059239>{format_number(sponsorship_amount)} (Company funds not tracked)", inline=False)

            outcome_embed.add_field(name="Popularity Gained", value=f"+{popularity_gain} (New popularity: {group_entry['popularity']})", inline=False)

        else: # Failure
            outcome_embed.color = discord.Color.red()
            outcome_embed.title = f"💔 Sponsorship Deal Failed with {self.chosen_brand_name.replace('_', ' ').title()}"
            outcome_embed.description = f"Unfortunately, {self.group_name} couldn't secure the deal this time. Better luck next time!"
            outcome_embed.add_field(name="Chance of Success", value=f"{success_chance * 100:.2f}%", inline=False)
            if self.investment > 0:
                outcome_embed.add_field(name="Investment Cost", value=f"<:MonthlyPeso:1338642658436059239>{format_number(self.investment)}", inline=False)

        save_data() 

        await self.original_interaction.edit_original_response(embed=outcome_embed, view=None)
        self.stop()

    async def on_timeout(self):
        # Disable all buttons on timeout
        for item in self.children:
            item.disabled = True
        await self.original_interaction.edit_original_response(content="Sponsorship selection timed out.", view=self)


@bot.tree.command(description="Seek a new sponsorship deal for your group!")
@app_commands.describe(
    group_name="The name of your group.",
    investment="Optional: Funds to invest for better chances. Will be deducted from company funds."
)
async def sponsorship(interaction: discord.Interaction, group_name: str, investment: int = 0):
    user_id = str(interaction.user.id)
    group_name_upper = group_name.upper()

    # Apply daily limit for sponsorship
    is_limited, remaining_uses = check_daily_limit(user_id, "sponsorship", 3) # Increased to 3 uses
    if is_limited:
        await interaction.response.send_message(f"❌ You have reached your daily limit of 3 sponsorship attempts. Remaining uses today: {remaining_uses}.", ephemeral=True)
        return

    if not is_user_group_owner(user_id, group_name_upper):
        await interaction.response.send_message(f"❌ Group `{group_name}` not found or does not belong to your company.", ephemeral=True)
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot seek sponsorship for {group_name_upper} as they are disbanded.", ephemeral=True)
        return

    company_name = get_group_owner_company(group_name_upper)
    current_company_funds = company_funds.get(company_name, 0)

    if investment < 0:
        await interaction.response.send_message("❌ Investment cannot be negative.", ephemeral=True)
        return
    if investment > 0:
        if current_company_funds < investment:
            await interaction.response.send_message(f"❌ Your company `{company_name}` only has <:MonthlyPeso:1338642658436059239>{format_number(current_company_funds)}. Not enough funds for that investment.", ephemeral=True)
            return
        company_funds[company_name] -= investment
        save_data() 

    sponsorship_embed = discord.Embed(
        title=f"✨ Sponsorship Opportunities for {group_name_upper} ✨",
        description="Select a brand below to try and secure a sponsorship deal!",
        color=discord.Color.gold()
    )
    sponsorship_embed.add_field(name="Current Popularity", value=group_data[group_name_upper].get('popularity', 0), inline=True)
    if investment > 0:
        sponsorship_embed.add_field(name="Investment in Deal", value=f"<:MonthlyPeso:1338642658436059239>{format_number(investment)}", inline=True)
        sponsorship_embed.set_footer(text=f"You have {remaining_uses} sponsorship attempts left today. Your investment will increase your chances!")
    else:
        sponsorship_embed.set_footer(text=f"You have {remaining_uses} sponsorship attempts left today. No investment made for this attempt.")


    # Filter eligible deals based on group popularity
    eligible_deals_pool = [
        brand_name for brand_name, details in SPONSORSHIP_DEALS.items() 
        if group_data[group_name_upper].get('popularity', 0) >= details["min_popularity"]
    ]

    # Select a random subset of eligible deals, up to a maximum of 5
    num_to_display = min(5, len(eligible_deals_pool))
    random_deals_to_display = random.sample(eligible_deals_pool, num_to_display)

    eligible_deals_str = ""
    # Sort selected random deals by min_popularity for better display order
    sorted_random_deals = sorted(random_deals_to_display, key=lambda brand_name: SPONSORSHIP_DEALS[brand_name]['min_popularity'])


    for brand_name in sorted_random_deals:
        details = SPONSORSHIP_DEALS[brand_name]
        eligible_deals_str += (
            f"**{brand_name.replace('_', ' ').title()}** (Target Pop: {details['min_popularity']}+)\n"
            f"  - *{details['description']}*\n"
            f"  - Potential Earnings: <:MonthlyPeso:1338642658436059239>{format_number(details['base_amount'])}\n"
            f"  - Pop Gain: {details['popularity_gain'][0]}-{details['popularity_gain'][1]}\n"
            f"  - Streams Gain: {format_number(details['stream_gain'][0])}-{format_number(details['stream_gain'][1])}\n"
            f"  - Sales Gain: {format_number(details['sales_gain'][0])}-{format_number(details['sales_gain'][1])}\n\n"
        )

    if len(eligible_deals_str) > 1024:
        eligible_deals_str = eligible_deals_str[:1021] + "..." # Truncate and add ellipsis


    view = SponsorshipDealView(interaction, group_name, investment, sorted_random_deals)
    if not eligible_deals_str:
        sponsorship_embed.add_field(
            name="No Eligible Deals Currently", 
            value=f"Your group's popularity ({group_data[group_name_upper].get('popularity', 0)}) is currently too low for any available sponsorships. Keep working on boosting their popularity!", 
            inline=False
        )

        for item in view.children:
            if item.custom_id and item.custom_id.startswith("sponsorship_") and item.custom_id != "sponsorship_cancel":
                item.disabled = True
    else:
        sponsorship_embed.add_field(name="Available Deals", value=eligible_deals_str, inline=False)

    await interaction.response.send_message(embed=sponsorship_embed, view=view, ephemeral=False)

# List of cities for concerts
CONCERT_CITIES = [
    # Korea
    "Seoul", "Busan", "Daegu", "Incheon",
    # Japan
    "Tokyo", "Osaka", "Kyoto",
    # Americas
    "New York", "Los Angeles", "Toronto", "Mexico City", "Buenos Aires", "São Paulo",
    # Europe
    "London", "Paris", "Berlin", "Rome", "Madrid", "Amsterdam",
    # Asia (excluding Korea/Japan for variety)
    "Shanghai", "Bangkok", "Singapore", "Mumbai",
    # Australia
    "Sydney", "Melbourne"
]

@bot.tree.command(description="Announce an upcoming concert!")
@app_commands.describe(
    group_name="The name of your group.",
    city="The city where the concert will be held."
)
async def concert(interaction: discord.Interaction, group_name: str, city: str):
    user_id = str(interaction.user.id)
    group_name_upper = group_name.upper()

    # Apply daily limit for concert
    is_limited, remaining_uses = check_daily_limit(user_id, "concert", 1)
    if is_limited:
        await interaction.response.send_message(f"❌ You have reached your daily limit of 1 concert announcement. Remaining uses today: {remaining_uses}.", ephemeral=True)
        return

    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.")
        return

    if city not in CONCERT_CITIES:
        await interaction.response.send_message(f"❌ Invalid city. Please choose from: {', '.join(CONCERT_CITIES)}")
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot announce a concert for {group_name_upper} as they are disbanded.", ephemeral=True)
        return

    group_entry = group_data[group_name_upper]
    group_popularity_for_tickets = group_entry.get('popularity', 0)

    # Calculate tickets sold based on popularity
    # Base tickets: 10000 + popularity * 100, with some random variation
    base_tickets = 10000 + (group_popularity_for_tickets * 100)
    tickets_sold = max(1000, int(random.gauss(mu=base_tickets, sigma=group_popularity_for_tickets * 50)))

    popularity_boost = tickets_sold // 10000 # Example: 1 pop per 10k tickets
    group_entry['popularity'] = group_entry.get('popularity', 0) + popularity_boost

    save_data() # Save data after modification

    # Creating an embed for concert
    embed = discord.Embed(
        title=f"🎤 {group_name_upper} Concert Announcement!",
        description=f"Get ready! **{group_name_upper}** is coming to **{city}**!",
        color=discord.Color.purple()
    )
    embed.add_field(name="City", value=city, inline=True)
    embed.add_field(name="Date", value=datetime.now().strftime("%Y-%m-%d"), inline=True) # Concert date is today for simplicity
    embed.add_field(name="Tickets Sold", value=format_number(tickets_sold), inline=True)
    embed.add_field(name="Popularity Boost", value=f"+{popularity_boost}", inline=True)
    embed.add_field(name="New Popularity", value=f"{group_entry['popularity']}", inline=True)
    embed.set_footer(text=f"Don't miss out on {group_name_upper}'s performance! You have {remaining_uses} concert announcements left today.")
    await interaction.response.send_message(embed=embed)


# 🏢 COMPANY & GROUP MANAGEMENT:
@bot.tree.command(description="Register a new company.")
async def addcompany(interaction: discord.Interaction, name: str):
    company_name_upper = name.strip().upper()
    user_id = str(interaction.user.id)

    if company_name_upper in company_funds:
        await interaction.response.send_message(f"❌ Company `{name}` already exists.")
        return

    # User can now own multiple companies
    if user_id not in user_companies:
        user_companies[user_id] = []

    if company_name_upper in user_companies[user_id]:
        await interaction.response.send_message(f"❌ You already own a company named `{name}`.", ephemeral=True)
        return

    # Create company document
    company_funds[company_name_upper] = 0
    user_companies[user_id].append(company_name_upper) # Add to list of owned companies
    save_data() # Save data after modification

    embed = discord.Embed(
        title=f"🏢 Company Registered: {name}",
        description=f"Congratulations, {interaction.user.display_name}! You are now the CEO of **{name}**!",
        color=discord.Color.dark_green()
    )
    embed.add_field(name="Initial Funds", value=f"<:MonthlyPeso:1338642658436059239>0", inline=False)
    embed.set_footer(text="Start debuting groups and earning money!")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Add a new group to an existing company.")
async def addgroup(
    interaction: discord.Interaction, 
    group_name: str, 
    korean_name: str, 
    company_name: str, # Now explicitly pass company name
    initial_wins: int = 0
):
    user_id = str(interaction.user.id)
    company_name_upper = company_name.upper()

    if not is_user_company_owner(user_id, company_name_upper):
        await interaction.response.send_message(f"❌ You do not own the company `{company_name}`.", ephemeral=True)
        return

    group_name_upper = group_name.upper()
    if group_name_upper in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` already exists. Please choose a different name.")
        return

    if initial_wins < 0:
        await interaction.response.send_message("❌ Initial wins cannot be negative.")
        return

    # Create group entry
    new_group_data = {
        'company': company_name_upper, # Use the passed company name
        'albums': [], 
        'korean_name': korean_name,
        'wins': initial_wins, 
        'popularity': 100, 
        'debut_date': datetime.now().strftime("%Y-%m-%d"),
        'is_disbanded': False # Newly created group is not disbanded
    }
    group_data[group_name_upper] = new_group_data
    group_popularity[group_name_upper] = new_group_data['popularity']
    save_data() # Save data after modification

    embed = discord.Embed(
        title=f"🎤 New Group Added: {group_name}",
        description=f"**{group_name}** ({korean_name}) has been added to **{company_name_upper}**!",
        color=discord.Color.dark_teal()
    )
    embed.add_field(name="Initial Wins", value=initial_wins, inline=True)
    embed.add_field(name="Initial Popularity", value=new_group_data['popularity'], inline=True)
    embed.set_footer(text=f"Ready for their debut!")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Debut a new group, including their first album.")
async def debut(
    interaction: discord.Interaction, 
    group_name: str, 
    korean_name: str, 
    album_name: str, 
    company_name: str, # Now explicitly pass company name
    investment: int,
    image_url: str = DEFAULT_ALBUM_IMAGE # Optional image URL for album cover
):
    user_id = str(interaction.user.id)
    company_name_upper = company_name.upper()

    if not is_user_company_owner(user_id, company_name_upper):
        await interaction.response.send_message(f"❌ You do not own the company `{company_name}`.", ephemeral=True)
        return

    group_name_upper = group_name.upper()
    if group_name_upper in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` already exists. Please choose a different name or use `/comeback`.")
        return

    if album_name in album_data:
        await interaction.response.send_message(f"❌ Album `{album_name}` already exists. Please choose a different name.")
        return

    current_company_funds = company_funds.get(company_name_upper, 0)

    if investment <= 0:
        await interaction.response.send_message("❌ Investment must be a positive number.")
        return
    if current_company_funds < investment:
        await interaction.response.send_message(f"❌ Your company `{company_name_upper}` only has <:MonthlyPeso:1338642658436059239>{current_company_funds:,}. Not enough funds.")
        return

    # Deduct investment from company funds
    company_funds[company_name_upper] = current_company_funds - investment

    # Create group entry
    new_group_data = {
        'company': company_name_upper,
        'albums': [album_name],
        'korean_name': korean_name,
        'wins': 0,
        'popularity': 100 + (investment // 100000), 
        'debut_date': datetime.now().strftime("%Y-%m-%d"),
        'is_disbanded': False # Newly debuted group is not disbanded
    }
    group_data[group_name_upper] = new_group_data
    group_popularity[group_name_upper] = new_group_data['popularity']

 album_attributes = _generate_album_attributes_for_group(group_name_upper)

    # Create album entry
    new_album_data = {
        'group': group_name_upper,
        'wins': 0,
        'release_date': datetime.now().strftime("%Y-%m-%d"),
        'streams': 0,
        'sales': 0,
        'image_url': image_url, # Store the image URL
        'is_active_promotion': False, # Newly debuted album is not active until promoperiod
        'promotion_end_date': None,
        'charts_info': { # Initialize chart info for the new album
            "MelOn": {'rank': None, 'peak': None, 'prev_rank': None},
            "Genie": {'rank': None, 'peak': None, 'prev_rank': None},
            "Bugs": {'rank': None, 'peak': None, 'prev_rank': None},
            "FLO": {'rank': None, 'peak': None, 'prev_rank': None}
       },
        'stream_history': [],
        **album_attributes
    }
    album_data[album_name] = new_album_data
    save_data() # Save data after modification

    embed = discord.Embed(
        title=f"✨ DEBUT! {group_name}!",
        description=f"Congratulations! **{group_name}** ({korean_name}) has officially debuted under **{company_name_upper}** with their album '{album_name}'!",
        color=discord.Color.green()
    )
    embed.set_thumbnail(url=image_url)
    embed.add_field(name="Initial Popularity", value=new_group_data['popularity'], inline=True)
    embed.add_field(name="Investment", value=f"<:MonthlyPeso:1338642658436059239>{investment:,}", inline=True)
    embed.add_field(name="Company Funds Remaining", value=f"<:MonthlyPeso:1338642658436059239>{company_funds[company_name_upper]:,}", inline=True)
    embed.set_footer(text=f"Debut Date: {new_group_data['debut_date']}")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Announce a group's comeback with a new album.")
async def comeback(
    interaction: discord.Interaction, 
    group_name: str, 
    album_name: str, 
    investment: int,
    image_url: str = DEFAULT_ALBUM_IMAGE # Optional image URL for album cover
):
    user_id = str(interaction.user.id)
    group_name_upper = group_name.upper()

    if not is_user_group_owner(user_id, group_name_upper):
        await interaction.response.send_message(f"❌ Group `{group_name}` not found or does not belong to your company.", ephemeral=True)
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ This group is disbanded and cannot make a comeback.", ephemeral=True)
        return

    if album_name in album_data:
        await interaction.response.send_message(f"❌ Album `{album_name}` already exists. Please choose a different name.")
        return

    company_name = get_group_owner_company(group_name_upper)
    current_company_funds = company_funds.get(company_name, 0)

    if investment <= 0:
        await interaction.response.send_message("❌ Investment must be a positive number.")
        return
    if current_company_funds < investment:
        await interaction.response.send_message(f"❌ Your company `{company_name}` only has <:MonthlyPeso:1338642658436059239>{current_company_funds:,}. Not enough funds.")
        return

    # Deduct investment from company funds
    company_funds[company_name] = current_company_funds - investment

    group_entry = group_data[group_name_upper]
    group_entry['albums'].append(album_name)
    group_entry['popularity'] = group_entry.get('popularity', 0) + (investment // 200000) 

 album_attributes = _generate_album_attributes_for_group(group_name_upper)

    # Create album entry
    new_album_data = {
        'group': group_name_upper,
        'wins': 0,
        'release_date': datetime.now().strftime("%Y-%m-%d"),
        'streams': 0,
        'sales': 0,
        'image_url': image_url, # Store the image URL
        'is_active_promotion': False, # Newly released album is not active until promoperiod
        'promotion_end_date': None,
        'charts_info': { # Initialize chart info for the new album
            "MelOn": {'rank': None, 'peak': None, 'prev_rank': None},
            "Genie": {'rank': None, 'peak': None, 'prev_rank': None},
            "Bugs": {'rank': None, 'peak': None, 'prev_rank': None},
            "FLO": {'rank': None, 'peak': None, 'prev_rank': None}
          },
        'stream_history': [],
        **album_attributes
    }
    album_data[album_name] = new_album_data
    save_data() # Save data after modification

    await interaction.response.send_message(
        f"🎉 **{group_name}** is making a comeback with their new album '{album_name}'! "
        f"Their popularity is now {group_entry['popularity']} and your company spent <:MonthlyPeso:1338642658436059239>{investment:,}."
    )

@bot.tree.command(description="Disband a group (requires confirmation).")
async def disband(interaction: discord.Interaction, group_name: str):
    group_name_upper = group_name.upper()
    user_id = str(interaction.user.id)

    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.")
        return

    group_entry = group_data[group_name_upper]
    company_name_of_group = group_entry.get('company')

    if not is_user_company_owner(user_id, company_name_of_group):
        await interaction.response.send_message(f"❌ You can only disband groups belonging to your company `{company_name_of_group}`.", ephemeral=True)
        return

    if group_entry.get('is_disbanded'):
        await interaction.response.send_message(f"❌ Group `{group_name_upper}` is already disbanded.", ephemeral=True)
        return

    class DisbandConfirmView(ui.View):
        def __init__(self, group_name_to_disband, interaction_original):
            super().__init__()
            self.group_name_to_disband = group_name_to_disband
            self.original_interaction = interaction_original # Correctly store the original interaction

        @ui.button(label="Confirm Disband", style=discord.ButtonStyle.red)
        async def confirm_callback(self, interaction: discord.Interaction, button: ui.Button):
            if interaction.user.id != self.original_interaction.user.id: # Check against original interaction user
                await interaction.response.send_message("❌ This confirmation is not for you.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False) 

            # Mark group as disbanded
            if self.group_name_to_disband in group_data:
                group_data[self.group_name_to_disband]['is_disbanded'] = True
                group_data[self.group_name_to_disband]['popularity'] = 0 # Set popularity to 0 upon disbandment

            # Albums remain, but are no longer actively promoted and won't chart.
            # No need to delete album data, just deactivate promotions.
            for album_name in group_data[self.group_name_to_disband].get('albums', []):
                if album_name in album_data:
                    album_data[album_name]['is_active_promotion'] = False
                    album_data[album_name]['promotion_end_date'] = None
                    for chart_key in album_data[album_name]['charts_info']:
                        album_data[album_name]['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}

            save_data() # Save data after modifications

            await self.original_interaction.edit_original_response(content=f"💀 Group **{self.group_name_to_disband}** has been disbanded. Their albums are no longer actively promoted.", view=None)
            self.stop() 

        @ui.button(label="Cancel", style=discord.ButtonStyle.grey)
        async def cancel_callback(self, interaction: discord.Interaction, button: ui.Button):
            if interaction.user.id != self.original_interaction.user.id: # Check against original interaction user
                await interaction.response.send_message("❌ This cancellation is not for you.", ephemeral=True)
                return
            await self.original_interaction.edit_original_response(content=f"Disbanding of **{self.group_name_to_disband}** cancelled.", view=None)
            self.stop() 

    view = DisbandConfirmView(group_name_upper, interaction)
    await interaction.response.send_message(
        f"Are you sure you want to disband **{group_name}**? This action will mark the group as inactive and prevent future activities, but will **not** delete their historical data.",
        view=view,
        ephemeral=True
    )


@bot.tree.command(description="Show the leaderboard of most popular groups.")
async def groups(interaction: discord.Interaction):
    if not group_data:
        await interaction.response.send_message("No groups registered yet.")
        return

    # Filter out disbanded groups for the leaderboard display
    active_groups = {name: data for name, data in group_data.items() if not data.get('is_disbanded')}
    if not active_groups:
        await interaction.response.send_message("No active groups to display on the leaderboard.")
        return

    sorted_groups = sorted(active_groups.items(), key=lambda item: item[1].get('popularity', 0), reverse=True)

    embed = discord.Embed(title="🏆 Most Popular Groups Leaderboard (Active)", color=discord.Color.gold())

    for i, (group_name, data) in enumerate(sorted_groups[:10]): 
        company = data.get('company', 'N/A')
        popularity = data.get('popularity', 0)
        embed.add_field(name=f"{i+1}. {group_name}", value=f"Popularity: {popularity}\nCompany: {company}", inline=False) 

    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Show leaderboards for companies (richest).")
async def companies(interaction: discord.Interaction):
    if not company_funds:
        await interaction.response.send_message("No companies registered yet.")
        return

    # Richest Companies
    richest_companies = sorted(company_funds.items(), key=lambda item: item[1], reverse=True)
    company_embed = discord.Embed(title="💰 Richest Companies Leaderboard", color=discord.Color.blue())
    if richest_companies:
        for i, (company_name, funds) in enumerate(richest_companies[:10]):
            company_embed.add_field(name=f"{i+1}. {company_name}", value=f"Funds: <:MonthlyPeso:1338642658436059239>{funds:,}", inline=False)
    else:
        company_embed.add_field(name="No companies", value="No companies registered yet.", inline=False)

    await interaction.response.send_message(embed=company_embed) # Changed 'embed' to 'company_embed'


# === Chart Logic and Command ===

# CHART_CONFIG for realism - Adjusted based on user's feedback for more achievable charting
CHART_CONFIG = {
    "MelOn": {"max_rank": 200, "stream_sensitivity": 0.0000003, "charting_threshold": 120, "recency_weight": 15, "virality_weight": 5}, # Hardest #1, but achievable
    "Genie": {"max_rank": 150, "stream_sensitivity": 0.0000006, "charting_threshold": 90, "recency_weight": 12, "virality_weight": 5},
    # Adjusted Bugs: Lower sensitivity (harder to chart high), higher max_rank and charting_threshold for more spread
  "Bugs": {"max_rank": 100, "stream_sensitivity": 0.0000005, "charting_threshold": 70, "recency_weight": 10, "virality_weight": 4},
    "FLO": {"max_rank": 80, "stream_sensitivity": 0.000001, "charting_threshold": 60, "recency_weight": 8, "virality_weight": 3},
}


def _calculate_chart_rank(album_streams: int, chart_settings: dict):
   """Calculates a deterministic charting score for ranking."""
    return album_streams * chart_settings['stream_sensitivity']
def _is_album_promotion_active(album_entry: dict):
    """Checks if an album's promotion is still active and clears chart data when it ends."""
    promo_end_date_obj = album_entry.get('promotion_end_date')
 if album_entry.get('is_active_promotion') and promo_end_date_obj and datetime.now() > promo_end_date_obj:
        album_entry['is_active_promotion'] = False
        album_entry['promotion_end_date'] = None
        for chart_key in album_entry.get('charts_info', {}):
            album_entry['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}
        return False

    return album_entry.get('is_active_promotion') and (not promo_end_date_obj or datetime.now() <= promo_end_date_obj)


def _gather_active_albums():
    """Returns a dictionary of active album entries keyed by album name."""
    active_albums = {}

    for album_name, album_entry in album_data.items():
        if _is_album_promotion_active(album_entry):
            active_albums[album_name] = album_entry

    return active_albums

def _generate_chart_rankings(active_albums: dict):
    """Generates chart rankings for all platforms based on active albums and their streams."""
    platform_rankings = {platform_name: {} for platform_name in CHART_CONFIG}

    for platform_name, settings in CHART_CONFIG.items():
        eligible_albums = []

        for album_name, album_entry in active_albums.items():
            album_streams = album_entry.get('streams', 0)
            chart_score = _calculate_chart_rank(album_streams, settings)

            if chart_score >= settings['charting_threshold']:
                eligible_albums.append((album_name, album_streams, chart_score))

        # Sort primarily by streams to reflect platform-specific popularity, using chart_score as a deterministic tiebreaker
        eligible_albums.sort(key=lambda item: (item[1], item[2]), reverse=True)
 for idx, (album_name, _streams, _score) in enumerate(eligible_albums[:settings['max_rank']]):
            platform_rankings[platform_name][album_name] = idx + 1

    return platform_rankings


def _refresh_all_chart_ranks():
    """Refreshes chart ranks for all active albums across all platforms."""
    active_albums = _gather_active_albums()
    platform_rankings = _generate_chart_rankings(active_albums)

    for album_name, album_entry in active_albums.items():
        for platform_name in CHART_CONFIG:
            assigned_rank = platform_rankings.get(platform_name, {}).get(album_name)
            _update_and_format_chart_line(album_entry, platform_name, assigned_rank, format_only=False)

    save_data()


def _get_chart_info(album_entry: dict, chart_type: str):
    """Retrieves chart information for a specific album and chart type, ensuring structure exists."""
    if 'charts_info' not in album_entry:
        album_entry['charts_info'] = {}

    if chart_type not in album_entry['charts_info']:
        album_entry['charts_info'][chart_type] = {'rank': None, 'peak': None, 'prev_rank': None}

    return album_entry['charts_info'][chart_type]

def _deactivate_album(album_entry: dict):
    """Deactivate an album promotion and clear its chart data."""
    album_entry['is_active_promotion'] = False
    album_entry['promotion_end_date'] = None
    if 'charts_info' not in album_entry:
        album_entry['charts_info'] = {}
    for chart_key in CHART_CONFIG.keys():
        album_entry['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}


def _is_album_promotion_active(album_entry: dict):
    """Check if an album is still within its promotion window."""
    if not album_entry.get('is_active_promotion'):
        return False

    promo_end_date = album_entry.get('promotion_end_date')
    if promo_end_date and datetime.now() > promo_end_date:
        _deactivate_album(album_entry)
        return False

    return True


def _compute_recency_score(release_date_str: str, now: datetime):
    """Compute a recency score that favors newer releases within a 6-month window."""
    try:
        release_date = datetime.fromisoformat(release_date_str)
    except (TypeError, ValueError):
        return 0

    days_since_release = (now - release_date).days
    recency_window_days = 180
    if days_since_release < 0:
        days_since_release = 0

    if days_since_release >= recency_window_days:
        return 0

    return (recency_window_days - days_since_release) / recency_window_days


def _compute_virality_bonus(identifier: str):
    """Deterministic virality bonus used as a stable tie-breaker."""
    return (abs(hash(identifier)) % 1000) / 1000


def _update_chart_info(album_entry: dict, chart_name: str, new_rank: int):
    """Update chart fields for an album with the freshly calculated rank."""
    chart_info = _get_chart_info(album_entry, chart_name)
        chart_info['prev_rank'] = chart_info.get('rank')
    chart_info['rank'] = new_rank

    if new_rank is not None:
        if chart_info['peak'] is None or new_rank < chart_info['peak']:
            chart_info['peak'] = new_rank


def refresh_chart_snapshot():
    """Recalculate chart standings for all active albums and persist a snapshot."""
    now = datetime.now()
    active_albums = {}

     for album_name, album_entry in album_data.items():
        if _is_album_promotion_active(album_entry):
            active_albums[album_name] = album_entry

    snapshot = {"generated_at": now.isoformat(), "platforms": {}}

  if not format_only:
        return None

  for platform_name, settings in CHART_CONFIG.items():
        platform_results = []
        for album_name, album_entry in active_albums.items():
            streams = album_entry.get('streams', 0)
            recency_score = _compute_recency_score(album_entry.get('release_date'), now)
            virality_score = _compute_virality_bonus(f"{album_name}:{album_entry.get('group', '')}")

            stream_component = streams * settings['stream_sensitivity']
            recency_component = recency_score * settings.get('recency_weight', 0)
            virality_component = virality_score * settings.get('virality_weight', 0)
            total_score = stream_component + recency_component + virality_component

            platform_results.append({
                'album': album_name,
                'group': album_entry.get('group'),
                'score': total_score,
                'streams': streams,
                'recency_score': recency_score,
                'virality_score': virality_score
            })

        sorted_results = sorted(
            platform_results,
            key=lambda entry: (
                -entry['score'],
                -entry['streams'],
                -entry['recency_score'],
                -entry['virality_score'],
                entry['album']
            )
        )

        chart_cutoff = settings.get('charting_threshold', settings['max_rank'])
        platform_snapshot = []
        for idx, entry in enumerate(sorted_results, start=1):
            rank_value = idx if idx <= chart_cutoff else None
            album_entry = album_data.get(entry['album'])
            _update_chart_info(album_entry, platform_name, rank_value)

            if rank_value is not None and idx <= settings['max_rank']:
                platform_snapshot.append({
                    'album': entry['album'],
                    'group': entry['group'],
                    'rank': rank_value,
                    'score': round(entry['score'], 4),
                    'streams': entry['streams']
                })

        snapshot['platforms'][platform_name] = platform_snapshot

    latest_chart_snapshot.clear()
    latest_chart_snapshot.update(snapshot)
    save_data()
    return snapshot

def _get_active_album_for_group(group_name_upper: str):
    """Return the active album for a group if one exists."""
    group_albums = group_data.get(group_name_upper, {}).get('albums', [])

    new_peak_text = ""
    # Check for new peak: if current rank is the lowest (best) rank achieved so far, and it's charting
    if calculated_rank is not None and calculated_rank == chart_info['peak']:
        # Only mark as "new peak" if it's actually an improvement or the first recorded rank that's charting
        if chart_info['prev_rank'] is None or calculated_rank < chart_info['prev_rank']:
            new_peak_text = "*new peak"

    return None


@bot.tree.command(description="Display music charts for a group's active album.")
@app_commands.describe(
    group_name="The name of the group whose active album charts you want to see."
)
async def charts(interaction: discord.Interaction, group_name: str):
    group_name_upper = group_name.upper()

    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found. Please check the name.", ephemeral=True)
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot show charts for {group_name_upper} as they are disbanded.", ephemeral=True)
        return

       _refresh_all_chart_ranks()

    refresh_chart_snapshot()
    active_album_name = _get_active_album_for_group(group_name_upper)

    if not active_album_name:
        await interaction.response.send_message(f"❌ No active album found for `{group_name}`. Please set a promotion period using `/promoperiod`.", ephemeral=True)
        return

    album_entry = album_data[active_album_name]
    _ensure_album_defaults(active_album_name, album_entry)
    group_korean_name = group_data[group_name_upper].get('korean_name', '')

    report_lines = []

    # Header line format: "<:SNS_Titter:1355910325115031642> GroupName AlbumName June 15th Update"
    current_date_formatted = f"{datetime.now().strftime('%B')} {ordinal(datetime.now().day)}"

    # Display group name and album name without "ULT" or quotes
    report_lines.append(f"<:SNS_Titter:1355910325115031642> **{group_name_upper} {active_album_name} {current_date_formatted} Update**\n")


    final_chart_display = []
    for platform_name, settings in CHART_CONFIG.items():
        chart_info = _get_chart_info(album_entry, platform_name)
        current_rank = chart_info.get('rank')

        if current_rank is not None: # Only add if it's charting
            rank_str = f"#{current_rank}"
            prev_rank = chart_info.get('prev_rank')
            peak_rank = chart_info.get('peak')

            rank_change_text = ""
            is_new_entry = (prev_rank is None)
            if is_new_entry:
                rank_change_text = "(NEW)"
            elif current_rank < prev_rank:
                rank_change_text = f"(+{prev_rank - current_rank})"
            elif current_rank > prev_rank:
                rank_change_text = f"(-{current_rank - prev_rank})"
            else:
                rank_change_text = "(=)"

            new_peak_text = ""
            if current_rank == peak_rank and (prev_rank is None or current_rank < prev_rank):
                new_peak_text = "*new peak"

            final_chart_display.append((current_rank, f"{rank_str} {platform_name} {rank_change_text} {new_peak_text}".strip()))

    # Sort by rank for display
    final_chart_display.sort(key=lambda x: x[0])

    if final_chart_display:
        report_lines.extend([line for rank, line in final_chart_display])
    else:
        report_lines.append(f"*{active_album_name} by {group_name_upper} is not currently charting on any major platform.*")

 recent_history = album_entry.get('stream_history', [])[-3:]
    if recent_history:
        report_lines.append("\n__Recent Streams__")
        for history_entry in recent_history:
            timestamp_raw = history_entry.get('timestamp')
            try:
                timestamp_fmt = datetime.fromisoformat(timestamp_raw).strftime('%b %d %H:%M') if timestamp_raw else ""
            except ValueError:
                timestamp_fmt = timestamp_raw or ""
            increment = history_entry.get('increment', 0)
            components = history_entry.get('components', {})
            flags = []
            if components.get('virality_triggered'):
                flags.append('VIRAL')
            if components.get('promo_boost'):
                flags.append('PROMO')
            flag_text = f" ({', '.join(flags)})" if flags else ""
            report_lines.append(f"{timestamp_fmt}: +{format_number(increment)}{flag_text}")

    # Add hashtags at the end
    group_hashtag_main = f"#{group_name_upper.replace(' ', '')}"
    group_korean_hashtag = f"#{group_korean_name.replace(' ', '')}" if group_korean_name else ""
    album_hashtag = f"#{active_album_name.replace(' ', '')}"

    final_hashtags_block = []
    # New chart hashtag format: **#(groupname) #(koreanname)**\n**#(albumname)**
    if group_korean_hashtag:
        final_hashtags_block.append(f"**{group_hashtag_main} {group_korean_hashtag}**")
    else:
        final_hashtags_block.append(f"**{group_hashtag_main}**")
    final_hashtags_block.append(f"**{album_hashtag}**")

    report_lines.append("\n" + "\n".join(final_hashtags_block)) # Add newline before hashtags

    await interaction.response.send_message("\n".join(report_lines))
@bot.tree.command(description="View streaming stats and momentum for an album.")
async def stats(interaction: discord.Interaction, album_name: str):
    if album_name not in album_data:
        await interaction.response.send_message(f"❌ Album `{album_name}` not found.", ephemeral=True)
        return

    album_entry = album_data[album_name]
    _ensure_album_defaults(album_name, album_entry)
    _normalize_promotion_state(album_entry)

    release_dt = _get_release_date(album_entry)
    days_since_release = max(0, (datetime.now() - release_dt).days)

    recent_history = album_entry.get('stream_history', [])[-5:]
    latest_components = recent_history[-1].get('components', {}) if recent_history else {}

    embed = discord.Embed(
        title=f"📈 {album_name} Stats",
        description=f"Group: **{album_entry.get('group', 'N/A')}**",
        color=discord.Color.purple()
    )
    embed.add_field(name="Release Date", value=release_dt.strftime('%Y-%m-%d'), inline=True)
    embed.add_field(name="Days Since Release", value=str(days_since_release), inline=True)
    embed.add_field(name="Total Streams", value=format_number(album_entry.get('streams', 0)), inline=True)

    embed.add_field(name="Fanbase Size", value=format_number(album_entry.get('fanbase_size', 0)), inline=True)
    embed.add_field(name="GP Interest", value=album_entry.get('gp_interest', 0), inline=True)
    embed.add_field(name="Promo Power", value=album_entry.get('promo_power', 0), inline=True)
    embed.add_field(name="Virality Potential", value=f"{album_entry.get('virality_potential', 0) * 100:.1f}%", inline=True)

    if latest_components:
        component_lines = [
            f"Fanbase: {format_number(latest_components.get('fanbase_core', 0))}",
            f"GP (decay): {format_number(latest_components.get('gp_decay', 0))}",
            f"Promo: {format_number(latest_components.get('promo_boost', 0))}",
            f"Virality: {format_number(latest_components.get('virality_spike', 0))}",
        ]
        if latest_components.get('virality_triggered'):
            component_lines.append("🚀 Latest tick had a virality spike!")
        embed.add_field(name="Last Tick Breakdown", value="\n".join(component_lines), inline=False)

    if recent_history:
        trend_lines = []
        for entry in recent_history:
            try:
                ts = datetime.fromisoformat(entry.get('timestamp', '')).strftime('%b %d %H:%M')
            except ValueError:
                ts = entry.get('timestamp', '')
            trend_lines.append(f"{ts}: +{format_number(entry.get('increment', 0))}")
        embed.add_field(name="Recent Stream Ticks", value="\n".join(trend_lines), inline=False)
    else:
        embed.add_field(name="Recent Stream Ticks", value="No automated stream history yet.", inline=False)

    embed.set_footer(text=f"Streaming ticks run every {STREAM_TICK_MINUTES} minutes.")
    save_data()
    await interaction.response.send_message(embed=embed)

# --- New Command: View Group Details ---
@bot.tree.command(description="View detailed information about a group.")
async def view_group(interaction: discord.Interaction, group_name: str):
    group_name_upper = group_name.upper()
    if group_name_upper not in group_data:
        await interaction.response.send_message(f"❌ Group `{group_name}` not found.", ephemeral=True)
        return

    group_info = group_data[group_name_upper]

    embed = discord.Embed(
        title=f"🎤 {group_name_upper} ({group_info.get('korean_name', 'N/A')})",
        description=f"Managed by **{group_info.get('company', 'N/A')}**",
        color=discord.Color.teal()
    )

    embed.add_field(name="Current Popularity", value=group_info.get('popularity', 0), inline=True)
    embed.add_field(name="Total Wins", value=group_info.get('wins', 0), inline=True)
    embed.add_field(name="Debut Date", value=group_info.get('debut_date', 'N/A'), inline=True)

    status = "Active"
    if group_info.get('is_disbanded'):
        status = "Disbanded"
    embed.add_field(name="Status", value=status, inline=True)


    # List albums
    albums_list = group_info.get('albums', [])
    if albums_list:
        albums_str = []
        for album in albums_list:
            album_detail = album_data.get(album, {})
            streams = format_number(album_detail.get('streams', 0))
            sales = format_number(album_detail.get('sales', 0))
            wins = album_detail.get('wins', 0)
            is_active = " (Active Promo)" if album_detail.get('is_active_promotion') else ""
            albums_str.append(f"**{album}**{is_active} (Streams: {streams}, Sales: {sales}, Wins: {wins})")
        embed.add_field(name="Albums", value="\n".join(albums_str), inline=False)
    else:
        embed.add_field(name="Albums", value="No albums released yet.", inline=False)

    embed.set_footer(text=f"Information last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    await interaction.response.send_message(embed=embed)


# --- New Command: Set Promotion Period ---
@bot.tree.command(description="Set or change the active promotion period for an album.")
@app_commands.describe(
    group_name="The group that owns the album.",
    album_name="The album to set as active for promotion.",
    duration_days="How many days the album will be actively promoted (charts, etc.). Set to 0 to deactivate."
)
async def promoperiod(interaction: discord.Interaction, group_name: str, album_name: str, duration_days: int):
    user_id = str(interaction.user.id)
    group_name_upper = group_name.upper()

    # Check ownership
    if not is_user_group_owner(user_id, group_name_upper):
        await interaction.response.send_message(f"❌ You do not manage the company that owns '{group_name}'.", ephemeral=True)
        return

    if group_data[group_name_upper].get('is_disbanded'):
        await interaction.response.send_message(f"❌ Cannot set promotion period for {group_name_upper} as they are disbanded.", ephemeral=True)
        return
    if album_name not in album_data or album_data[album_name].get('group') != group_name_upper:
        await interaction.response.send_message(f"❌ Album `{album_name}` not found or does not belong to `{group_name}`.", ephemeral=True)
        return

    # Deactivate any other active album for this group
    for alb_name in group_data[group_name_upper].get('albums', []):
        if alb_name != album_name and album_data.get(alb_name, {}).get('is_active_promotion'):
            album_data[alb_name]['is_active_promotion'] = False
            album_data[alb_name]['promotion_end_date'] = None
            # Reset chart info for deactivated album
            for chart_key in album_data[alb_name]['charts_info']:
                album_data[alb_name]['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}
            print(f"Deactivated promotion for {alb_name}")

    current_album_entry = album_data[album_name]
    korean_name = group_data[group_name_upper].get('korean_name', '')
    korean_name_display = f"({korean_name})" if korean_name else ""


    if duration_days > 0:
        current_album_entry['is_active_promotion'] = True
        current_album_entry['promotion_end_date'] = datetime.now() + timedelta(days=duration_days)

        start_date_str = datetime.now().strftime("%Y.%m.%d")
        end_date_str = current_album_entry['promotion_end_date'].strftime("%Y.%m.%d")

        message = (
            f"**{group_name_upper} {korean_name_display} '{album_name}'**\n"
            f"Inicio de promoción: {start_date_str}\n"
            f"Final de promoción:{end_date_str}"
        )

        # Reset charts info when a new promo period starts
        for chart_key in current_album_entry['charts_info']:
            current_album_entry['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}

    else: # duration_days <= 0 means deactivate
        current_album_entry['is_active_promotion'] = False
        current_album_entry['promotion_end_date'] = None

        message = (
            f"**{group_name_upper} {korean_name_display} '{album_name}'**\n"
            f"Promoción: No activo"
        )
        # Reset chart info when promotion ends
        for chart_key in current_album_entry['charts_info']:
            current_album_entry['charts_info'][chart_key] = {'rank': None, 'peak': None, 'prev_rank': None}

    save_data()
    await interaction.response.send_message(message)


# --- PAYOLA SHOP ---
PAYOLA_SHOP_ITEMS = {
    "POP POTION": {
        "cost": 500000,
        "popularity_boost_range": (50, 150),
        "description": "Give your group a sudden surge in popularity!"
    },
    "MEDIA BUY": { # New item for buying streams/sales (ads)
        "cost": 1_000_000,
        "streams_to_add_range": (100_000, 500_000),
        "sales_to_add_range": (1000, 5000),
        "description": "Purchase direct streams and sales for an album. No revenue generated for your company."
    },
    "SCANDAL MACHINE": { # New item for sabotage
        "cost": 2_500_000,
        "popularity_reduction_range": (100, 300),
        "backfire_chance": 0.20, # 20% chance to backfire
        "description": "Attempt to create a scandal for another group, reducing their popularity. May backfire!"
    }
}

class PayolaShopView(ui.View):
    def __init__(self, original_interaction: discord.Interaction, item_name: str, group_name: str | None, user_id: str, target_album_name: str = None, target_group_name: str = None):
        super().__init__(timeout=30)
        self.original_interaction = original_interaction
        self.item_name = item_name.upper()
        # Safely handle group_name being None
        self.group_name = group_name.upper() if group_name else "" 
        self.user_id = user_id
        self.target_album_name = target_album_name
        self.target_group_name = target_group_name # For sabotage


    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) != self.user_id:
            await interaction.response.send_message("❌ This purchase confirmation is not for you.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        try:
            await self.original_interaction.edit_original_response(content="Purchase timed out.", view=self)
        except discord.NotFound:
            pass # Message might have been deleted

    @ui.button(label="Confirm Purchase", style=discord.ButtonStyle.green, custom_id="confirm_payola_purchase")
    async def confirm_payola_purchase_callback(self, interaction: discord.Interaction, button: ui.Button):
        item_details = PAYOLA_SHOP_ITEMS.get(self.item_name)
        if not item_details:
            await interaction.response.edit_message(content="❌ Item not found.", view=None)
            self.stop()
            return

        # Ensure latest data
        load_data() 

        user_bal = user_balances.get(self.user_id, 0)

        if user_bal < item_details['cost']:
            await interaction.response.edit_message(content=f"❌ You don't have enough <:MonthlyPeso:1338642658436059239> to purchase '{self.item_name}'. You need {format_number(item_details['cost'])}.", view=None)
            self.stop()
            return

        # Deduct cost immediately
        user_balances[self.user_id] -= item_details['cost']

        outcome_message = f"✅ You successfully purchased **'{self.item_name}'**!\n"

        # --- Apply item effects ---
        if self.item_name == "POP POTION":
            if not self.group_name or self.group_name not in group_data:
                await interaction.response.edit_message(content=f"❌ Group '{self.group_name}' not found or not specified for Pop Potion.", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund if group is invalid
                save_data()
                self.stop()
                return
            if group_data[self.group_name].get('is_disbanded'):
                await interaction.response.edit_message(content=f"❌ Cannot use Pop Potion on disbanded group ({self.group_name}).", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            # Removed the ownership check for Pop Potion
            # if not is_user_group_owner(self.user_id, self.group_name):
            #      await interaction.response.edit_message(content=f"❌ You do not manage the company that owns '{self.group_name}'.", view=None)
            #      user_balances[self.user_id] += item_details['cost'] # Refund
            #      save_data()
            #      self.stop()
            #      return


            group_entry = group_data.get(self.group_name)
            popularity_boost = random.randint(*item_details['popularity_boost_range'])
            group_entry['popularity'] = group_entry.get('popularity', 0) + popularity_boost
            outcome_message += (
                f"**{self.group_name}**'s popularity increased by **{popularity_boost}** "
                f"(New popularity: {group_entry['popularity']})."
            )

        elif self.item_name == "MEDIA BUY":
            if not self.target_album_name or self.target_album_name not in album_data:
                await interaction.response.edit_message(content=f"❌ Album '{self.target_album_name}' not found or not specified for Media Buy.", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            target_album_entry = album_data.get(self.target_album_name)
            target_group_name_for_album = target_album_entry.get('group') # Renamed to avoid clash

            if target_group_name_for_album and group_data[target_group_name_for_album].get('is_disbanded'):
                await interaction.response.edit_message(content=f"❌ Cannot use Media Buy on an album of a disbanded group ({target_group_name_for_album}).", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            # Removed the ownership check for Media Buy
            # if not is_user_group_owner(self.user_id, target_group_name_for_album):
            #      await interaction.response.edit_message(content=f"❌ You do not manage the company that owns the group for '{self.target_album_name}'.", view=None)
            #      user_balances[self.user_id] += item_details['cost'] # Refund
            #      save_data()
            #      self.stop()
            #      return


            streams_added = random.randint(*item_details['streams_to_add_range'])
            sales_added = random.randint(*item_details['sales_to_add_range'])

            target_album_entry['streams'] = target_album_entry.get('streams', 0) + streams_added
            target_album_entry['sales'] = target_album_entry.get('sales', 0) + sales_added

            outcome_message += (
                f"Added {format_number(streams_added)} streams and {format_number(sales_added)} sales to album "
                f"**'{self.target_album_name}'**. (No revenue for company from these sales/streams)."
            )

        elif self.item_name == "SCANDAL MACHINE":
            if not self.target_group_name or self.target_group_name not in group_data:
                await interaction.response.edit_message(content=f"❌ Target group '{self.target_group_name}' not found or not specified for Scandal Machine.", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            target_group_entry = group_data.get(self.target_group_name)
            if target_group_entry.get('is_disbanded'):
                await interaction.response.edit_message(content=f"❌ Cannot sabotage disbanded group ({self.target_group_name}).", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            # Prevent sabotaging one's own group
            if is_user_group_owner(self.user_id, self.target_group_name):
                await interaction.response.edit_message(content=f"❌ You cannot sabotage your own group '{self.target_group_name}'.", view=None)
                user_balances[self.user_id] += item_details['cost'] # Refund
                save_data()
                self.stop()
                return

            # Check for backfire
            if random.random() < item_details['backfire_chance']:
                popularity_reduction = random.randint(*item_details['popularity_reduction_range'])
                # If backfire, reduce user's own group's popularity (if specified) or a random one from their company
                user_company_list = get_user_companies(self.user_id)
                affected_group_for_backfire = None # Initialize to None

                if user_company_list:
                    # Collect all active groups owned by the user's companies
                    user_owned_active_groups = []
                    for company in user_company_list:
                        for g_name, g_data in group_data.items():
                            if g_data.get('company') == company and not g_data.get('is_disbanded'):
                                user_owned_active_groups.append(g_name)

                    if user_owned_active_groups:
                        affected_group_for_backfire = random.choice(user_owned_active_groups)
                        group_data[affected_group_for_backfire]['popularity'] = max(0, group_data[affected_group_for_backfire].get('popularity', 0) - popularity_reduction)
                        outcome_message = (
                            f"💔 Oh no! The **Scandal Machine** backfired!\n"
                            f"Your group **{affected_group_for_backfire}**'s popularity decreased by **{popularity_reduction}** "
                            f"(New popularity: {group_data[affected_group_for_backfire]['popularity']})."
                        )
                        # Public message for backfire
                        try:
                            await self.original_interaction.channel.send(
                                f"🚨 A scandal has hit **{affected_group_for_backfire}**! Their popularity has decreased by {popularity_reduction}."
                            )
                        except discord.errors.Forbidden:
                            print(f"ERROR: Missing permissions to send public scandal message in channel {self.original_interaction.channel.id}")
                    else:
                        outcome_message = "💔 Oh no! The **Scandal Machine** backfired, but you have no active groups to affect!"
                else:
                    outcome_message = "💔 Oh no! The **Scandal Machine** backfired, but you don't own a company to affect!"
            else:
                popularity_reduction = random.randint(*item_details['popularity_reduction_range'])
                target_group_entry['popularity'] = max(0, target_group_entry.get('popularity', 0) - popularity_reduction)
                outcome_message += (
                    f"**{self.target_group_name}**'s popularity decreased by **{popularity_reduction}** "
                    f"(New popularity: {target_group_entry['popularity']})."
                )
                # Public message for successful sabotage
                try:
                    await self.original_interaction.channel.send(
                        f"📰 Breaking News! A scandal has hit **{self.target_group_name}**! Their popularity has decreased by {popularity_reduction}."
                    )
                except discord.errors.Forbidden:
                    print(f"ERROR: Missing permissions to send public scandal message in channel {self.original_interaction.channel.id}")

        outcome_message += f"\nYour balance is now <:MonthlyPeso:1338642658436059239>{format_number(user_balances[self.user_id])}."
        save_data()

        await interaction.response.edit_message(content=outcome_message, view=None)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.red, custom_id="cancel_payola_purchase")
    async def cancel_payola_purchase_callback(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Purchase cancelled.", view=None)
        self.stop()


@bot.tree.command(description="Visit the Payola Shop to buy special items!")
@app_commands.describe(
    item_name="The name of the item you want to buy (optional, shows menu if empty).",
    group_name="Your group (for Pop Potion).",
    album_name="Your album (for Media Buy).",
    target_group_name="The group to target (for Scandal Machine)."
)
async def payolashop(
    interaction: discord.Interaction, 
    item_name: str = None, 
    group_name: str = None, 
    album_name: str = None, 
    target_group_name: str = None
):
    user_id = str(interaction.user.id)

    # If no item_name is provided, display the shop menu
    if item_name is None:
        if not PAYOLA_SHOP_ITEMS:
            await interaction.response.send_message("The Payola Shop currently has no items in stock.", ephemeral=True)
            return

        shop_menu = ["✨ **Welcome to the Payola Shop!** ✨\n\nAvailable items:\n"]
        for name, details in PAYOLA_ITEMS.items(): # Use PAYOLA_ITEMS to ensure all details are shown consistently
            description = details['description']
            cost = format_number(details['cost'])

            extra_info = []
            if "popularity_boost_range" in details:
                extra_info.append(f"Pop Boost: {details['popularity_boost_range'][0]}-{details['popularity_boost_range'][1]}")
            if "streams_to_add_range" in details:
                extra_info.append(f"Streams: {format_number(details['streams_to_add_range'][0])}-{format_number(details['streams_to_add_range'][1])}")
            if "sales_to_add_range" in details:
                extra_info.append(f"Sales: {format_number(details['sales_to_add_range'][0])}-{format_number(details['sales_to_add_range'][1])}")
            if "popularity_reduction_range" in details:
                extra_info.append(f"Pop Reduction: {details['popularity_reduction_range'][0]}-{details['popularity_reduction_range'][1]}")
            if "backfire_chance" in details:
                extra_info.append(f"Backfire Chance: {int(details['backfire_chance'] * 100)}%")

            shop_menu.append(f"- **{name.title()}**: {description} (Cost: <:MonthlyPeso:1338642658436059239>{cost})")
            if extra_info:
                shop_menu.append(f"  ({', '.join(extra_info)})")

        shop_menu.append("\nTo purchase, use `/payolashop item_name:<item> [group_name:<group>] [album_name:<album>] [target_group_name:<target>]`")

        await interaction.response.send_message("\n".join(shop_menu), ephemeral=True)
        return

    # If an item_name is provided, proceed with the purchase logic
    item_name_upper = item_name.upper()
    item_details = PAYOLA_SHOP_ITEMS.get(item_name_upper)

    if not item_details:
        available_items = "\n".join([f"- **{name.title()}**: {details['description']} (Cost: <:MonthlyPeso:1338642658436059239>{format_number(details['cost'])})" for name, details in PAYOLA_SHOP_ITEMS.items()])
        await interaction.response.send_message(
            f"❌ Item '{item_name}' not found. Available items:\n{available_items}",
            ephemeral=True
        )
        return

    user_bal = user_balances.get(user_id, 0)

    if user_bal < item_details['cost']:
        await interaction.response.send_message(
            f"❌ You need <:MonthlyPeso:1338642658436059239>{format_number(item_details['cost'])} to buy '{item_name}'. You only have <:MonthlyPeso:1338642658436059239>{format_number(user_bal)}.",
            ephemeral=True
        )
        return

    # Validate arguments based on item type
    if item_name_upper == "POP POTION":
        if not group_name:
            await interaction.response.send_message(f"❌ '{item_name}' requires a `group_name`.", ephemeral=True)
            return
        group_name_upper = group_name.upper()
        if group_name_upper not in group_data or group_data[group_name_upper].get('is_disbanded'):
            await interaction.response.send_message(f"❌ Group '{group_name}' not found or is disbanded.", ephemeral=True)
            return

        # Removed ownership check here

        purchase_message = (
            f"Are you sure you want to purchase **'{item_name}'** for **{group_name}** "
            f"for <:MonthlyPeso:1338642658436059239>{format_number(item_details['cost'])}?\n"
            f"This will: {item_details['description']}"
        )
        view = PayolaShopView(interaction, item_name, group_name, user_id)

    elif item_name_upper == "MEDIA BUY":
        if not album_name:
            await interaction.response.send_message(f"❌ '{item_name}' requires an `album_name`.", ephemeral=True)
            return

        if album_name not in album_data:
            await interaction.response.send_message(f"❌ Album '{album_name}' not found.", ephemeral=True)
            return

        album_group = album_data[album_name].get('group')
        if album_group and group_data[album_group].get('is_disbanded'):
            await interaction.response.send_message(f"❌ Cannot buy media for an album of a disbanded group ({album_group}).", ephemeral=True)
            return

        # Removed ownership check here

        purchase_message = (
            f"Are you sure you want to purchase **'{item_name}'** for album **'{album_name}'** "
            f"for <:MonthlyPeso:1338642658436059239>{format_number(item_details['cost'])}?\n"
            f"This will: {item_details['description']}"
        )
        view = PayolaShopView(interaction, item_name, None, user_id, target_album_name=album_name)

    elif item_name_upper == "SCANDAL MACHINE":
        if not target_group_name:
            await interaction.response.send_message(f"❌ '{item_name}' requires a `target_group_name`.", ephemeral=True)
            return

        target_group_name_upper = target_group_name.upper()
        if target_group_name_upper not in group_data or group_data[target_group_name_upper].get('is_disbanded'):
            await interaction.response.send_message(f"❌ Target group '{target_group_name}' not found or is disbanded.", ephemeral=True)
            return

        # Prevent sabotaging one's own group
        user_company_list = get_user_companies(user_id)
        target_group_company = get_group_owner_company(target_group_name_upper)
        if user_company_list and target_group_company in user_company_list:
            await interaction.response.send_message(f"❌ You cannot sabotage your own group '{target_group_name}'.", ephemeral=True)
            return

        purchase_message = (
            f"Are you sure you want to purchase **'{item_name}'** to target **{target_group_name}** "
            f"for <:MonthlyPeso:1338642658436059239>{format_number(item_details['cost'])}?\n"
            f"This will: {item_details['description']}"
        )
        view = PayolaShopView(interaction, item_name, None, user_id, target_group_name=target_group_name_upper)

    else: # Fallback for other items that might not require specific targets, or error
        await interaction.response.send_message(f"❌ The item '{item_name}' could not be processed. Please check arguments.", ephemeral=True)
        return


    await interaction.response.send_message(purchase_message, view=view, ephemeral=True)

# New command to set company ownership (Admin only)
@bot.tree.command(description="ADMIN: Set the owner of a company.")
@app_commands.describe(
    user_id_str="The Discord User ID of the owner.",
    company_name="The name of the company to assign."
)
@is_admin() # Only allows users with is_admin role to use this command.
async def setcompanyowner(interaction: discord.Interaction, user_id_str: str, company_name: str):
    company_name_upper = company_name.upper()

    if company_name_upper not in company_funds:
        await interaction.response.send_message(f"❌ Company `{company_name}` does not exist.", ephemeral=True)
        return

    # Ensure user_id_str is a valid user ID (optional, but good practice)
    try:
        user_id = str(int(user_id_str)) # Convert to int and back to str to validate
    except ValueError:
        await interaction.response.send_message("❌ Invalid user ID provided.", ephemeral=True)
        return

    # Remove company from any previous owner
    for existing_user_id, companies_owned in user_companies.items():
        if company_name_upper in companies_owned:
            user_companies[existing_user_id].remove(company_name_upper)
            if not user_companies[existing_user_id]: # Clean up empty lists
                del user_companies[existing_user_id]
            break # Company can only have one owner

    # Assign company to new owner
    if user_id not in user_companies:
        user_companies[user_id] = []
    user_companies[user_id].append(company_name_upper)
    save_data()

    await interaction.response.send_message(f"✅ Company **{company_name_upper}** has been assigned to user with ID **{user_id}**.", ephemeral=True)



# For the payolashop command menu display, create a separate dictionary that doesn't include the specific ranges in the key names, just for display purposes.
PAYOLA_ITEMS = {
    "POP POTION": {
        "cost": 500000,
        "description": "Give your group a sudden surge in popularity!",
        "popularity_boost_range": (50, 150)
    },
    "MEDIA BUY": { 
        "cost": 1_000_000,
        "description": "Purchase direct streams and sales for an album. No revenue generated for your company.",
        "streams_to_add_range": (100_000, 500_000),
        "sales_to_add_range": (1000, 5000)
    },
    "SCANDAL MACHINE": {
        "cost": 2_500_000,
        "description": "Attempt to create a scandal for another group, reducing their popularity. May backfire!",
        "popularity_reduction_range": (100, 300),
        "backfire_chance": 0.20
    }
}


# === RUN ===
bot.run(TOKEN)

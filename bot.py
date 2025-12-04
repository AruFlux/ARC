import discord
from discord import app_commands
from discord.ext import tasks, commands
import asyncpg
import asyncio
from datetime import datetime, timedelta
import random
import logging

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] %(asctime)s - %(message)s')

logger = logging.getLogger("ARCgambler")

# -------------------- Bot Setup --------------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)
TOKEN = discord.environ.get("DISCORD_TOKEN")
DATABASE_URL = discord.environ.get("DATABASE_URL")
ADMIN_ID = 123456789012345678  # replace with your Discord user ID

db_pool: asyncpg.pool.Pool = None

# -------------------- Database Setup --------------------
async def create_tables():
    async with db_pool.acquire() as conn:
        # Users Table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            wallet BIGINT DEFAULT 0,
            bank BIGINT DEFAULT 0,
            streak INT DEFAULT 0,
            xp BIGINT DEFAULT 0,
            level INT DEFAULT 1,
            prestige INT DEFAULT 0,
            xp_multiplier FLOAT DEFAULT 1.0,
            daily_give BIGINT DEFAULT 0,
            last_give_reset TIMESTAMP DEFAULT NOW()
        )
        """)
        # Registration Table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS registrations(
            user_id BIGINT PRIMARY KEY,
            username TEXT NOT NULL,
            discriminator TEXT NOT NULL,
            registered_at TIMESTAMP DEFAULT NOW(),
            guild_id BIGINT
        )
        """)
        # Stocks Table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks(
            symbol TEXT PRIMARY KEY,
            price FLOAT DEFAULT 100
        )
        """)
        # Lottery Table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS lottery(
            user_id BIGINT,
            numbers TEXT,
            date TIMESTAMP DEFAULT NOW()
        )
        """)

async def get_db_pool():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
    return db_pool

# -------------------- User Helpers --------------------
async def create_user(user_id: int):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchrow("SELECT 1 FROM users WHERE user_id=$1", user_id)
        if not exists:
            await conn.execute("INSERT INTO users(user_id) VALUES($1)", user_id)

async def get_user(user_id: int):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        return user

# -------------------- XP & Level System --------------------
async def add_xp(user_id: int, base_amount: int = 5):
    async with db_pool.acquire() as conn:
        user = await get_user(user_id)
        if not user:
            await create_user(user_id)
            user = await get_user(user_id)

        # Apply prestige multiplier
        amount = int(base_amount * user['xp_multiplier'])
        new_xp = user['xp'] + amount
        level = user['level']
        leveled_up = False

        if level < 100:
            next_level_xp = 50 + (level * 100)
            if new_xp >= next_level_xp:
                level += 1
                new_xp -= next_level_xp
                leveled_up = True
        else:
            # Prestige automatically
            user_prestige = user['prestige'] + 1
            xp_multiplier = min(1 + 0.1 * user_prestige, 2.0)
            level = 1
            new_xp = 0
            leveled_up = True
            await conn.execute(
                "UPDATE users SET prestige=$1, xp_multiplier=$2 WHERE user_id=$3",
                user_prestige, xp_multiplier, user_id
            )
            embed = discord.Embed(
                title="Prestige Achieved!",
                description=f"Congratulations <@{user_id}>, you prestiged!\n"
                            f"XP multiplier is now **{xp_multiplier:.1f}×**!",
                color=discord.Color.purple()
            )
            # Optional: replace with your announcement channel
            # channel = bot.get_channel(YOUR_ANNOUNCEMENT_CHANNEL_ID)
            # await channel.send(embed=embed)

        await conn.execute(
            "UPDATE users SET xp=$1, level=$2 WHERE user_id=$3",
            new_xp, level, user_id
        )
        return leveled_up, level

# -------------------- Registration --------------------
async def register_user(user: discord.User, guild_id: int):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchrow("SELECT 1 FROM registrations WHERE user_id=$1", user.id)
        if exists:
            return False
        await conn.execute(
            "INSERT INTO registrations(user_id, username, discriminator, guild_id) VALUES($1, $2, $3, $4)",
            user.id, user.name, user.discriminator, guild_id
        )
        await create_user(user.id)
        return True

# -------------------- Command Event XP --------------------
@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type == discord.InteractionType.application_command:
        # Add XP for command usage
        xp_map = {
            "slot": 10,
            "coinflip": 10,
            "roulette": 10,
            "daily": 20,
            "buy": 10,
            "sell": 10,
            "stocks": 5,
            "graph": 5,
            "give": 5,
            "lottery": 5,
            "bank": 2,
            "leaderboard": 2,
        }
        xp_amount = xp_map.get(interaction.command.name, 5)
        leveled_up, new_level = await add_xp(interaction.user.id, xp_amount)
        if leveled_up:
            embed = discord.Embed(
                title="Level Up!",
                description=f"Congratulations {interaction.user.mention}, you reached level **{new_level}**!",
                color=discord.Color.gold()
            )
            await interaction.channel.send(embed=embed)

# -------------------- Daily Command --------------------
@bot.tree.command(name="daily", description="Claim daily ARC reward.")
async def daily(interaction: discord.Interaction):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user:
        await create_user(interaction.user.id)
        user = await get_user(interaction.user.id)

    last_claim = user['streak']  # can use actual timestamp logic if needed
    reward = 150 + user['streak']*10
    new_streak = user['streak'] + 1

    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET wallet=wallet+$1, streak=$2 WHERE user_id=$3",
            reward, new_streak, interaction.user.id
        )
    await add_xp(interaction.user.id, 20)
    await interaction.followup.send(f"You claimed your daily reward: **{reward} ARC**. Current streak: {new_streak}.")

# -------------------- Gambling Commands --------------------
@bot.tree.command(name="slot", description="Play slot machine.")
async def slot(interaction: discord.Interaction, bet: int):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user or bet <= 0 or bet > user['wallet']:
        await interaction.followup.send("Invalid bet amount.")
        return

    symbols = ["🍒", "🍋", "🍊", "7️⃣"]
    result = [random.choice(symbols) for _ in range(3)]
    win = result.count(result[0]) == 3
    payout = min(bet*2, 500000) if win else -bet

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", payout, interaction.user.id)

    await add_xp(interaction.user.id, 10)
    await interaction.followup.send(f"Result: {' '.join(result)}\n{'You won!' if win else 'You lost!'} {abs(payout)} ARC")

@bot.tree.command(name="coinflip", description="Flip a coin and bet ARC.")
async def coinflip(interaction: discord.Interaction, bet: int, choice: str):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user or bet <= 0 or bet > user['wallet']:
        await interaction.followup.send("Invalid bet amount.")
        return
    choice = choice.lower()
    result = random.choice(["heads", "tails"])
    win = choice == result
    payout = min(bet*2, 500000) if win else -bet

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", payout, interaction.user.id)

    await add_xp(interaction.user.id, 10)
    await interaction.followup.send(f"The coin landed on **{result}**.\n{'You won!' if win else 'You lost!'} {abs(payout)} ARC")

# -------------------- Give ARC Command --------------------
@bot.tree.command(name="give", description="Give ARC to another user.")
@app_commands.describe(user="Recipient", amount="Amount to give")
async def give(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.defer()
    giver = await get_user(interaction.user.id)
    receiver = await get_user(user.id)
    if not receiver:
        await create_user(user.id)
        receiver = await get_user(user.id)

    # Reset daily_give if 24h passed
    now = datetime.utcnow()
    if giver['last_give_reset'] + timedelta(hours=24) < now:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET daily_give=0, last_give_reset=$1 WHERE user_id=$2", now, giver['user_id'])
        giver = await get_user(interaction.user.id)

    max_give = giver['level'] * 1000
    if amount <= 0 or amount > max_give or giver['wallet'] < amount or (amount + giver['daily_give'] > max_give):
        await interaction.followup.send(f"You cannot give {amount} ARC. Max allowed today: {max_give - giver['daily_give']}.")
        return

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet-$1, daily_give=daily_give+$1 WHERE user_id=$2", amount, giver['user_id'])
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", amount, receiver['user_id'])

    await add_xp(interaction.user.id, 5)
    await interaction.followup.send(f"{interaction.user.mention} gave {amount} ARC to {user.mention}.")

# -------------------- Bot Startup --------------------
async def main():
    global db_pool
    db_pool = await get_db_pool()
    await create_tables()
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())

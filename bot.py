import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncpg
import asyncio
from datetime import datetime, timedelta
import random
import logging
import os
import matplotlib.pyplot as plt
import io

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
logger = logging.getLogger("ARCgambler")

# -------------------- Bot Setup --------------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)
TOKEN = os.environ.get("DISCORD_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = 123456789012345678  # replace with your Discord user ID

db_pool: asyncpg.pool.Pool = None

# -------------------- Database --------------------
async def create_tables():
    async with db_pool.acquire() as conn:
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
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS registrations(
            user_id BIGINT PRIMARY KEY,
            username TEXT NOT NULL,
            discriminator TEXT NOT NULL,
            registered_at TIMESTAMP DEFAULT NOW(),
            guild_id BIGINT
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks(
            symbol TEXT PRIMARY KEY,
            price FLOAT DEFAULT 100
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS lottery(
            user_id BIGINT,
            numbers TEXT,
            date TIMESTAMP DEFAULT NOW()
        )""")

async def get_db_pool():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database pool created.")
    return db_pool

# -------------------- User Helpers --------------------
async def create_user(user_id: int):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchrow("SELECT 1 FROM users WHERE user_id=$1", user_id)
        if not exists:
            await conn.execute("INSERT INTO users(user_id) VALUES($1)", user_id)
            logger.info(f"User {user_id} created.")

async def get_user(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

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

# -------------------- XP & Level + Prestige --------------------
async def add_xp(user_id: int, base_amount: int = 5):
    async with db_pool.acquire() as conn:
        user = await get_user(user_id)
        if not user:
            await create_user(user_id)
            user = await get_user(user_id)

        amount = int(base_amount * user['xp_multiplier'])
        new_xp = user['xp'] + amount
        leveled_up = False
        prestige_up = False
        level = user['level']
        prestige = user['prestige']
        xp_multiplier = user['xp_multiplier']

        if level < 100:
            next_xp = 50 + (level * 100)
            if new_xp >= next_xp:
                level += 1
                new_xp -= next_xp
                leveled_up = True
        else:
            prestige += 1
            xp_multiplier = min(1 + 0.1 * prestige, 2.0)
            level = 1
            new_xp = 0
            prestige_up = True
            leveled_up = True
            await conn.execute(
                "UPDATE users SET prestige=$1, xp_multiplier=$2 WHERE user_id=$3",
                prestige, xp_multiplier, user_id
            )

        await conn.execute("UPDATE users SET xp=$1, level=$2 WHERE user_id=$3", new_xp, level, user_id)

        # Send DM embed
        user_obj = bot.get_user(user_id)
        if leveled_up:
            if prestige_up:
                embed = discord.Embed(
                    title="Prestige Achieved!",
                    description=f"Congrats {user_obj.mention}! You prestiged!\nXP multiplier is now **{xp_multiplier:.1f}×**!",
                    color=discord.Color.purple()
                )
            else:
                embed = discord.Embed(
                    title="Level Up!",
                    description=f"Congrats {user_obj.mention}, you reached level **{level}**!",
                    color=discord.Color.gold()
                )
            try:
                await user_obj.send(embed=embed)
            except:
                pass

        return leveled_up, level, prestige_up, prestige

# -------------------- Interaction XP --------------------
@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type == discord.InteractionType.application_command:
        xp_map = {
            "slot": 10, "coinflip": 10, "roulette": 10, "daily": 20,
            "buy": 10, "sell": 10, "stocks": 5, "graph": 5,
            "give": 5, "lottery": 5, "bank": 2, "leaderboard": 2,
        }
        xp = xp_map.get(interaction.command.name, 5)
        leveled, new_level, prestige_up, prestige = await add_xp(interaction.user.id, xp)
        if leveled and not prestige_up:
            embed = discord.Embed(
                title="Level Up!",
                description=f"Congrats {interaction.user.mention}, you reached level **{new_level}**!",
                color=discord.Color.gold()
            )
            try:
                await interaction.channel.send(embed=embed)
            except:
                pass

# -------------------- Daily Command --------------------
@bot.tree.command(name="daily", description="Claim daily ARC reward.")
async def daily(interaction: discord.Interaction):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user:
        await create_user(interaction.user.id)
        user = await get_user(interaction.user.id)

    now = datetime.utcnow()
    last_claim = user['last_give_reset'] or datetime(2000,1,1)
    streak = user['streak']

    if last_claim.date() == now.date():
        await interaction.followup.send("You already claimed your daily today!")
        return

    new_streak = streak + 1
    reward = 150 + (new_streak * 10)
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1, streak=$2, last_give_reset=$3 WHERE user_id=$4",
                           reward, new_streak, now, interaction.user.id)

    await add_xp(interaction.user.id, 20)
    embed = discord.Embed(
        title="Daily Claimed!",
        description=f"You received **{reward} ARC**.\nCurrent streak: **{new_streak}**",
        color=discord.Color.blue()
    )
    await interaction.followup.send(embed=embed)

# -------------------- Gambling Commands --------------------
@bot.tree.command(name="slot", description="Play slot machine.")
async def slot(interaction: discord.Interaction, bet: int):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user or bet <= 0 or bet > user['wallet']:
        await interaction.followup.send("Invalid bet.")
        return
    symbols = ["🍒", "🍋", "🍊", "7️⃣"]
    result = [random.choice(symbols) for _ in range(3)]
    win = result.count(result[0]) == 3
    payout = min(bet*2, 500000) if win else -bet
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", payout, interaction.user.id)
    await add_xp(interaction.user.id, 10)
    await interaction.followup.send(f"{' '.join(result)}\n{'You won!' if win else 'You lost!'} {abs(payout)} ARC")

@bot.tree.command(name="coinflip", description="Flip a coin and bet ARC.")
async def coinflip(interaction: discord.Interaction, bet: int, choice: str):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user or bet <= 0 or bet > user['wallet']:
        await interaction.followup.send("Invalid bet.")
        return
    choice = choice.lower()
    result = random.choice(["heads", "tails"])
    win = choice == result
    payout = min(bet*2, 500000) if win else -bet
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", payout, interaction.user.id)
    await add_xp(interaction.user.id, 10)
    await interaction.followup.send(f"The coin landed on **{result}**.\n{'You won!' if win else 'You lost!'} {abs(payout)} ARC")

@bot.tree.command(name="roulette", description="Play roulette and bet ARC.")
async def roulette(interaction: discord.Interaction, bet: int, color: str):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if not user or bet <= 0 or bet > user['wallet']:
        await interaction.followup.send("Invalid bet.")
        return
    color = color.lower()
    result = random.choice(["red", "black"])
    win = color == result
    payout = min(bet*2, 500000) if win else -bet
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", payout, interaction.user.id)
    await add_xp(interaction.user.id, 10)
    await interaction.followup.send(f"Roulette landed on **{result}**.\n{'You won!' if win else 'You lost!'} {abs(payout)} ARC")

@bot.tree.command(name="lottery", description="Buy a lottery ticket (5 numbers).")
async def lottery(interaction: discord.Interaction, numbers: str):
    await interaction.response.defer()
    numbers_list = numbers.split(',')
    if len(numbers_list) != 5:
        await interaction.followup.send("Enter exactly 5 numbers separated by commas.")
        return
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO lottery(user_id, numbers) VALUES($1,$2)", interaction.user.id, numbers)
    await add_xp(interaction.user.id, 5)
    await interaction.followup.send("Lottery ticket bought!")

# -------------------- Give Command --------------------
@bot.tree.command(name="give", description="Give ARC to another user.")
@app_commands.describe(user="Recipient", amount="Amount to give")
async def give(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.defer()
    giver = await get_user(interaction.user.id)
    receiver = await get_user(user.id)
    if not receiver:
        await create_user(user.id)
        receiver = await get_user(user.id)

    now = datetime.utcnow()
    if giver['last_give_reset'] + timedelta(hours=24) < now:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET daily_give=0, last_give_reset=$1 WHERE user_id=$2", now, giver['user_id'])
        giver = await get_user(interaction.user.id)

    max_give = giver['level'] * 1000
    if amount <= 0 or amount > max_give or giver['wallet'] < amount or (amount + giver['daily_give'] > max_give):
        await interaction.followup.send(f"Cannot give {amount}. Max today: {max_give - giver['daily_give']}")
        return

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet-$1, daily_give=daily_give+$1 WHERE user_id=$2", amount, giver['user_id'])
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", amount, receiver['user_id'])

    await add_xp(interaction.user.id, 5)
    await interaction.followup.send(f"{interaction.user.mention} gave {amount} ARC to {user.mention}.")

# -------------------- Stocks + Graph --------------------
@bot.tree.command(name="stocks", description="Show all stocks with prices and graph.")
async def stocks(interaction: discord.Interaction):
    await interaction.response.defer()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, price FROM stocks")
        symbols = [r['symbol'] for r in rows]
        prices = [r['price'] for r in rows]

    plt.figure(figsize=(6,4))
    plt.bar(symbols, prices, color='green')
    plt.title("Stock Prices")
    plt.ylabel("Price (ARC)")
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    file = discord.File(buf, filename="stocks.png")
    embed = discord.Embed(title="Stocks")
    embed.set_image(url="attachment://stocks.png")
    await interaction.followup.send(embed=embed, file=file)
    buf.close()
    plt.close()

# -------------------- Leaderboards --------------------
@bot.tree.command(name="leaderboard", description="Show worldwide leaderboard for ARC.")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, wallet+bank as total FROM users ORDER BY total DESC LIMIT 10")
    desc = ""
    for i, r in enumerate(rows, 1):
        user_obj = bot.get_user(r['user_id'])
        desc += f"**{i}. {user_obj}** - {r['total']} ARC\n"
    embed = discord.Embed(title="Worldwide Leaderboard", description=desc)
    await interaction.followup.send(embed=embed)

# -------------------- Bot Startup --------------------
async def main():
    global db_pool
    db_pool = await get_db_pool()
    await create_tables()
    logger.info("Tables ready. Starting bot...")
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())

import discord
from discord import app_commands
import asyncpg
import os
import random
import time
import asyncio
import logging
import matplotlib.pyplot as plt
import io
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ARCgambler")

intents = discord.Intents.all()
db_pool = None

# --- Database helpers ---
async def get_db_pool():
    global db_pool
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database pool created successfully.")
        return pool
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise e

async def get_user(user_id):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def create_user(user_id):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", user_id
        )

# --- Database setup ---
async def setup_database():
    async with db_pool.acquire() as conn:
        # Users table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            wallet BIGINT DEFAULT 1000,
            bank BIGINT DEFAULT 0,
            last_daily BIGINT DEFAULT 0,
            streak INT DEFAULT 0
        )
        """)
        # Stocks table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            price BIGINT DEFAULT 100
        )
        """)
        # User stocks table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_stocks (
            user_id BIGINT,
            symbol TEXT,
            amount BIGINT DEFAULT 0,
            PRIMARY KEY(user_id, symbol),
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(symbol) REFERENCES stocks(symbol)
        )
        """)
        # Stock history table
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_history (
            symbol TEXT,
            price BIGINT,
            timestamp BIGINT
        )
        """)
        # Insert default stocks
        await conn.execute("""
        INSERT INTO stocks(symbol, price) VALUES
        ('ARC', 100),
        ('BTC', 500),
        ('ETH', 300)
        ON CONFLICT(symbol) DO NOTHING
        """)
    logger.info("Database tables setup completed.")

# --- Stock updater task ---
async def update_stocks():
    await bot.wait_until_ready()
    logger.info("Stock updater task running.")
    while True:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol, price FROM stocks")
                for row in rows:
                    change = random.randint(-10, 15)
                    new_price = max(1, row['price'] + change)
                    await conn.execute(
                        "UPDATE stocks SET price=$1 WHERE symbol=$2", new_price, row['symbol']
                    )
                    await conn.execute(
                        "INSERT INTO stock_history(symbol, price, timestamp) VALUES($1,$2,$3)",
                        row['symbol'], new_price, int(time.time())
                    )
        except Exception as e:
            logger.error(f"Error in update_stocks: {e}")
        await asyncio.sleep(20)

# --- Mini events ---
async def market_events():
    await bot.wait_until_ready()
    while True:
        await asyncio.sleep(3600)  # hourly
        try:
            event = random.choice(["boom", "crash", "nothing"])
            async with db_pool.acquire() as conn:
                if event == "boom":
                    await conn.execute("UPDATE stocks SET price = price + (price/5)")
                    logger.info("Market boom event triggered (+20%)")
                elif event == "crash":
                    await conn.execute("UPDATE stocks SET price = price - (price/4)")
                    logger.info("Market crash event triggered (-25%)")
        except Exception as e:
            logger.error(f"Market event error: {e}")

# --- Bank interest task ---
async def bank_interest():
    await bot.wait_until_ready()
    while True:
        await asyncio.sleep(86400)  # daily
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE users SET bank = bank + (bank/100)")
                logger.info("Daily bank interest applied (+1%)")
        except Exception as e:
            logger.error(f"Bank interest error: {e}")

# --- Bot class ---
class MyBot(discord.Client):
    def __init__(self, *, intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.token = TOKEN

    async def setup_hook(self):
        global db_pool
        db_pool = await get_db_pool()
        await setup_database()
        self.loop.create_task(update_stocks())
        self.loop.create_task(market_events())
        self.loop.create_task(bank_interest())
        await self.tree.sync()
        logger.info("Slash commands synced.")

bot = MyBot(intents=intents)

# --- Admin check ---
async def is_admin(interaction: discord.Interaction):
    return interaction.user.id == ADMIN_ID

# --- Commands ---

@bot.tree.command(name="register", description="Create an ARC account.")
async def register(interaction: discord.Interaction):
    await interaction.response.defer()
    await create_user(interaction.user.id)
    await interaction.followup.send("ARC account created. Wallet: 1000 ARC, Bank: 0 ARC.")

@bot.tree.command(name="bank", description="Check your wallet and bank balances.")
async def bank(interaction: discord.Interaction):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    embed = discord.Embed(
        title=f"{interaction.user.name}'s Bank Summary", 
        color=discord.Color.blue()
    )
    embed.add_field(name="Wallet", value=f"{user['wallet']} ARC")
    embed.add_field(name="Bank", value=f"{user['bank']} ARC")
    embed.add_field(name="Daily Streak", value=f"{user['streak']} days")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="leaderboard", description="Show top 10 ARC users worldwide.")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, wallet+bank AS total FROM users ORDER BY total DESC LIMIT 10"
        )
    embed = discord.Embed(title="ARC Leaderboard 🌎", color=discord.Color.gold())
    for i, row in enumerate(rows, start=1):
        member = bot.get_user(row['user_id'])
        name = member.name if member else str(row['user_id'])
        embed.add_field(name=f"#{i} {name}", value=f"{row['total']} ARC", inline=False)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="daily", description="Claim daily ARC reward with streaks.")
async def daily(interaction: discord.Interaction):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    now = int(time.time())
    if now - user['last_daily'] < 86400:
        await interaction.followup.send("Daily reward already claimed. Try later.")
        return
    reward = 150 + user['streak']*10
    streak = user['streak'] + 1
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET wallet=wallet+$1, last_daily=$2, streak=$3 WHERE user_id=$4",
            reward, now, streak, interaction.user.id
        )
    await interaction.followup.send(f"Claimed {reward} ARC! Current streak: {streak} days.")

@bot.tree.command(name="stocks", description="View all stock prices.")
async def stocks(interaction: discord.Interaction):
    await interaction.response.defer()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, price FROM stocks")
    embed = discord.Embed(title="Stock Prices", color=discord.Color.green())
    for row in rows:
        arrow = "📈" if random.choice([True, False]) else "📉"
        embed.add_field(name=row['symbol'], value=f"{row['price']} ARC {arrow}", inline=False)
    await interaction.followup.send(embed=embed)

# --- Graph command ---
async def plot_stock(symbol):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT price FROM stock_history WHERE symbol=$1 ORDER BY timestamp DESC LIMIT 20",
            symbol
        )
    if not rows:
        return None
    prices = [row['price'] for row in rows][::-1]
    plt.figure(figsize=(5,2))
    plt.plot(prices, marker='o')
    plt.title(f"{symbol} Price History")
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='PNG')
    buf.seek(0)
    plt.close()
    return buf

@bot.tree.command(name="graph", description="Show stock price graph.")
@app_commands.describe(symbol="Stock symbol")
async def graph(interaction: discord.Interaction, symbol: str):
    await interaction.response.defer()
    buf = await plot_stock(symbol.upper())
    if not buf:
        await interaction.followup.send("No stock history found.")
        return
    file = discord.File(buf, filename="graph.png")
    embed = discord.Embed(title=f"{symbol.upper()} Price Graph", color=discord.Color.green())
    embed.set_image(url="attachment://graph.png")
    await interaction.followup.send(embed=embed, file=file)

# --- Buy stock ---
@bot.tree.command(name="buy", description="Buy shares from the stock market.")
@app_commands.describe(symbol="Stock symbol", amount="Amount to buy")
async def buy(interaction: discord.Interaction, symbol: str, amount: int):
    await interaction.response.defer()
    symbol = symbol.upper()
    user = await get_user(interaction.user.id)
    async with db_pool.acquire() as conn:
        stock = await conn.fetchrow("SELECT * FROM stocks WHERE symbol=$1", symbol)
        if not stock:
            await interaction.followup.send("Stock does not exist.")
            return
        total = stock['price'] * amount
        if amount <= 0 or user['wallet'] < total:
            await interaction.followup.send("Insufficient wallet balance.")
            return
        await conn.execute("UPDATE users SET wallet=wallet-$1 WHERE user_id=$2", total, interaction.user.id)
        await conn.execute("""
            INSERT INTO user_stocks(user_id, symbol, amount)
            VALUES($1,$2,$3)
            ON CONFLICT(user_id,symbol) DO UPDATE SET amount=user_stocks.amount+$3
        """, interaction.user.id, symbol, amount)
    await interaction.followup.send(f"Bought {amount} shares of {symbol} for {total} ARC.")

# --- Slot machine ---
@bot.tree.command(name="slot", description="Play a slot machine with emojis.")
@app_commands.describe(bet="Amount of ARC to bet")
async def slot(interaction: discord.Interaction, bet: int):
    await interaction.response.defer()
    user = await get_user(interaction.user.id)
    if bet <= 0 or user['wallet'] < bet:
        await interaction.followup.send("Invalid bet.")
        return
    emojis = ["🍒", "🍋", "🍇", "💎", "⭐"]
    result = [random.choice(emojis) for _ in range(3)]
    winnings = 0
    if result[0] == result[1] == result[2]:
        winnings = bet*5
    elif result[0]==result[1] or result[1]==result[2] or result[0]==result[2]:
        winnings = bet*2
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet-$1+$2 WHERE user_id=$3", bet, winnings, interaction.user.id)
    embed = discord.Embed(title="Slot Result", description=" | ".join(result), color=discord.Color.purple())
    if winnings>0:
        embed.add_field(name="Outcome", value=f"You won {winnings} ARC!")
    else:
        embed.add_field(name="Outcome", value=f"You lost {bet} ARC.")
    await interaction.followup.send(embed=embed)

# --- Admin command ---
@bot.tree.command(name="addarc", description="Add ARC to a user (Admin only).")
@app_commands.describe(user="User", amount="Amount to add")
async def addarc(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.defer(ephemeral=True)
    if not await is_admin(interaction):
        await interaction.followup.send("You do not have permission.", ephemeral=True)
        return
    await create_user(user.id)
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", amount, user.id)
    await interaction.followup.send(f"Added {amount} ARC to {user.name}.", ephemeral=True)

# --- Ready event ---
@bot.event
async def on_ready():
    logger.info(f"{bot.user} has connected to Discord!")

# --- Run bot ---
if __name__ == "__main__":
    bot.run(TOKEN)

# bot.py
import discord
from discord import app_commands
import asyncpg
import os
import random
import time
import asyncio
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

intents = discord.Intents.all()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

# --- Database connection ---
async def get_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)

db_pool = None

@bot.event
async def on_ready():
    global db_pool
    if db_pool is None:
        db_pool = await get_db_pool()
    await tree.sync()
    print(f"Logged in as {bot.user}")

# --- Admin check ---
async def is_admin(interaction: discord.Interaction):
    return interaction.user.id == ADMIN_ID

# --- User functions ---
async def get_user(user_id):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        return user

async def create_user(user_id):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", user_id)

# --- Register Command ---
@tree.command(name="register", description="Create an ARC account.")
async def register(interaction: discord.Interaction):
    await create_user(interaction.user.id)
    await interaction.response.send_message("ARC account created. Wallet: 1000 ARC, Bank: 0 ARC.")

# --- Bank & Wallet ---
@tree.command(name="bank", description="Check your wallet and bank balances.")
async def bank(interaction: discord.Interaction):
    user = await get_user(interaction.user.id)
    wallet = user['wallet']
    bank_bal = user['bank']
    embed = discord.Embed(title=f"{interaction.user.name}'s Bank Summary", color=discord.Color.blue())
    embed.add_field(name="Wallet", value=f"{wallet} ARC")
    embed.add_field(name="Bank", value=f"{bank_bal} ARC")
    await interaction.response.send_message(embed=embed)

@tree.command(name="deposit", description="Deposit ARC from wallet to bank.")
@app_commands.describe(amount="Amount to deposit")
async def deposit(interaction: discord.Interaction, amount: int):
    user = await get_user(interaction.user.id)
    if amount <= 0 or amount > user['wallet']:
        await interaction.response.send_message("Invalid deposit amount.")
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet-$1, bank=bank+$1 WHERE user_id=$2", amount, interaction.user.id)
    await interaction.response.send_message(f"Deposited {amount} ARC to bank.")

@tree.command(name="withdraw", description="Withdraw ARC from bank to wallet.")
@app_commands.describe(amount="Amount to withdraw")
async def withdraw(interaction: discord.Interaction, amount: int):
    user = await get_user(interaction.user.id)
    if amount <= 0 or amount > user['bank']:
        await interaction.response.send_message("Invalid withdraw amount.")
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1, bank=bank-$1 WHERE user_id=$2", amount, interaction.user.id)
    await interaction.response.send_message(f"Withdrew {amount} ARC to wallet.")

# --- Daily reward ---
@tree.command(name="daily", description="Claim daily ARC reward.")
async def daily(interaction: discord.Interaction):
    user = await get_user(interaction.user.id)
    now = int(time.time())
    last = user['last_daily']
    if now - last < 86400:
        await interaction.response.send_message("Daily reward already claimed. Try later.")
        return
    reward = 150
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1, last_daily=$2 WHERE user_id=$3", reward, now, interaction.user.id)
    await interaction.response.send_message(f"Claimed {reward} ARC daily reward.")

# --- Stock Market ---
stock_prices = {}  # in-memory cache
price_history = {}  # in-memory cache

async def update_stocks():
    await bot.wait_until_ready()
    while True:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol, price FROM stocks")
            for row in rows:
                change = random.randint(-10, 15)
                new_price = max(1, row['price'] + change)
                await conn.execute("UPDATE stocks SET price=$1 WHERE symbol=$2", new_price, row['symbol'])
                if row['symbol'] not in price_history:
                    price_history[row['symbol']] = []
                price_history[row['symbol']].append((int(time.time()), new_price))
        await asyncio.sleep(20)

bot.loop.create_task(update_stocks())

@tree.command(name="stocks", description="View all stock prices.")
async def stocks(interaction: discord.Interaction):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, price FROM stocks")
    embed = discord.Embed(title="Stock Prices", color=discord.Color.blue())
    for row in rows:
        embed.add_field(name=row['symbol'], value=f"{row['price']} ARC", inline=False)
    await interaction.response.send_message(embed=embed)

# --- Buy Stock ---
@tree.command(name="buy", description="Buy shares from the stock market.")
@app_commands.describe(symbol="Stock symbol", amount="Amount to buy")
async def buy(interaction: discord.Interaction, symbol: str, amount: int):
    symbol = symbol.upper()
    user = await get_user(interaction.user.id)
    async with db_pool.acquire() as conn:
        stock = await conn.fetchrow("SELECT * FROM stocks WHERE symbol=$1", symbol)
        if not stock:
            await interaction.response.send_message("Stock does not exist.")
            return
        total = stock['price'] * amount
        if amount <= 0 or user['wallet'] < total:
            await interaction.response.send_message("Insufficient wallet balance.")
            return
        await conn.execute("UPDATE users SET wallet=wallet-$1 WHERE user_id=$2", total, interaction.user.id)
        await conn.execute("""
            INSERT INTO user_stocks(user_id, symbol, amount)
            VALUES($1,$2,$3)
            ON CONFLICT(user_id,symbol) DO UPDATE SET amount=user_stocks.amount+$3
        """, interaction.user.id, symbol, amount)
    await interaction.response.send_message(f"Bought {amount} shares of {symbol} for {total} ARC.")

# --- Slot Machine ---
@tree.command(name="slot", description="Play a slot machine.")
@app_commands.describe(bet="Amount of ARC to bet")
async def slot(interaction: discord.Interaction, bet: int):
    user = await get_user(interaction.user.id)
    if bet <= 0 or user['wallet'] < bet:
        await interaction.response.send_message("Invalid bet.")
        return
    symbols = ["A","B","C","D","E"]
    result = [random.choice(symbols) for _ in range(3)]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet-$1 WHERE user_id=$2", bet, interaction.user.id)
    winnings = 0
    if result[0] == result[1] == result[2]:
        winnings = bet*5
    elif result[0]==result[1] or result[1]==result[2] or result[0]==result[2]:
        winnings = bet*2
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", winnings, interaction.user.id)
    embed = discord.Embed(title="Slot Result", description=" | ".join(result), color=discord.Color.blue())
    if winnings>0:
        embed.add_field(name="Outcome", value=f"You won {winnings} ARC!")
    else:
        embed.add_field(name="Outcome", value=f"You lost {bet} ARC.")
    await interaction.response.send_message(embed=embed)

# --- Admin Command Example ---
@tree.command(name="addarc", description="Add ARC to a user (Admin only).")
@app_commands.describe(user="User", amount="Amount to add")
async def addarc(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not await is_admin(interaction):
        await interaction.response.send_message("You do not have permission.", ephemeral=True)
        return
    async with db_pool.acquire() as conn:
        await create_user(user.id)
        await conn.execute("UPDATE users SET wallet=wallet+$1 WHERE user_id=$2", amount, user.id)
    await interaction.response.send_message(f"Added {amount} ARC to {user.name}.", ephemeral=True)

# --- Run Bot ---
async def run_bot():
    await bot.start(TOKEN)

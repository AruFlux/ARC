# main.py
import asyncio
from bot import bot

async def run_bot():
    await bot.start(bot.token)

asyncio.run(run_bot())

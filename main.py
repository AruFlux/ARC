import asyncio
from bot import bot  # your bot instance

import os

TOKEN = os.environ.get("DISCORD_TOKEN")  # Railway secret or local env variable

async def run_bot():
    await bot.start(TOKEN)

asyncio.run(run_bot())

import discord
from discord import app_commands
from discord.ext import commands
import requests
import json
import base64
import time
import random
import os
import re
import asyncio
import sqlite3
from urllib.parse import urlparse, parse_qs, unquote
from typing import Optional, Tuple, Dict, List, Any, Set
from datetime import datetime, timedelta, UTC
import aiohttp
import logging
from dataclasses import dataclass
from enum import Enum
import hashlib
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('linkvertise_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

LOG_SERVER_ID = 1440296531814252607
LOG_CHANNEL_ID = 1468205714672586834
YOUR_USER_ID = 783542452756938813
AUTO_BYPASS_CHANNEL_ID = 1468205740064772199
AUTO_BYPASS_SERVER_ID = 1440296531814252607
PING_ROLE_ID = 1450481024814415944

class BypassMethod(Enum):
    DYNAMIC_DECODE = "dynamic_decode"
    API_BYPASS = "api_bypass"
    FALLBACK_SERVICE = "fallback_service"
    CACHED = "cached"
    PAGE_EXTRACTION = "page_extraction"

@dataclass
class BypassResult:
    success: bool
    url: Optional[str]
    message: str
    method: BypassMethod
    execution_time: float
    original_url: str
    user_id: int = 0
    guild_id: Optional[int] = None
    additional_info: Optional[Dict[str, Any]] = None

class EnhancedLinkvertiseBypassBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True

        super().__init__(command_prefix='!', intents=intents)

        self.db_conn: Optional[sqlite3.Connection] = None
        self.request_count: Dict[int, List[float]] = {}
        self.RATE_LIMIT_WINDOW = 60
        self.MAX_REQUESTS_PER_WINDOW = 20
        self.DAILY_LIMIT = 100
        self.user_stats: Dict[int, Dict] = {}
        self.cache: Dict[str, Tuple[str, float, BypassMethod]] = {}
        self.cache_order: List[str] = []
        self.MAX_CACHE_SIZE = 1000
        self.CACHE_TTL = 7200

        self.fallback_services = [
            {"url": "https://api.bypass.vip/bypass2", "method": "POST", "priority": 1, "timeout": 15},
            {"url": "https://bypass.pm/bypass2", "method": "GET", "priority": 2, "timeout": 15},
            {"url": "https://bypass.bot.nu/bypass2", "method": "GET", "priority": 3, "timeout": 15},
            {"url": "https://bypass.city/bypass2", "method": "GET", "priority": 4, "timeout": 15},
            {"url": "https://api.bypassfor.me/bypass", "method": "GET", "priority": 5, "timeout": 15},
        ]

        self.linkvertise_domains = [
            "linkvertise.com", "linkvertise.net", "linkvertise.io", "linkvertise.co",
            "up-to-down.net", "link-to.net", "linkv.to", "lv.to",
            "linkvertise.download", "linkvertise.pw", "linkvertise.xyz",
            "link-center.net", "linkvertise.tk", "linkvertise.ml",
            "linkvertise.ga", "linkvertise.cf", "linkvertise.gq"
        ]

        self.linkvertise_patterns = [
            r'(?:https?://)?(?:www\.)?(?:linkvertise\.(?:com|net|io|co|download|pw|xyz|tk|ml|ga|cf|gq)|up-to-down\.net|link-to\.net|linkv\.to|lv\.to|link-center\.net)/[^\s<>"\']+',
            r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)*(?:linkvertise\.(?:com|net|io|co)|up-to-down\.net|link-to\.net)/[^\s<>"\']+',
        ]

        self.session: Optional[aiohttp.ClientSession] = None
        self.method_stats = {method.value: 0 for method in BypassMethod}
        self.blocked_urls = set()
        self.log_channel: Optional[discord.TextChannel] = None
        self.auto_bypass_channel: Optional[discord.TextChannel] = None
        self.auto_bypass_allowed_users: Set[int] = {YOUR_USER_ID}
        self.ping_role: Optional[discord.Role] = None
        self.last_daily_stats_time: Optional[datetime] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        self.performance_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "avg_response_time": 0.0,
            "success_rate": 0.0
        }

    async def setup_hook(self):
        try:
            await self.init_database()

            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json, text/html, application/xhtml+xml, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Cache-Control": "max-age=0",
                }
            )

            await self.load_authorized_users()
            await self.load_cache_from_database()

            self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
            self.stats_task = asyncio.create_task(self.periodic_stats_log())
            self.health_check_task = asyncio.create_task(self.health_check_services())
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

    async def check_roblox_game_availability(self, roblox_url: str) -> Tuple[bool, str, Optional[int]]:
        try:
            patterns = [
                r'games/(\d+)',
                r'Place\.ashx\?id=(\d+)',
                r'GamePlaceId=(\d+)',
            ]

            game_id = None
            for pattern in patterns:
                match = re.search(pattern, roblox_url)
                if match:
                    game_id = int(match.group(1))
                    break

            if not game_id:
                return False, "Invalid Roblox URL format", None

            api_endpoints = [
                f"https://games.roblox.com/v1/games/multiget-place-details?placeIds={game_id}",
                f"https://games.roblox.com/v1/games?placeIds={game_id}",
            ]

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }

            for endpoint in api_endpoints:
                try:
                    async with self.session.get(endpoint, headers=headers, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()

                            if isinstance(data, list) and len(data) > 0:
                                game_data = data[0]
                                if "error" in game_data or game_data.get("isArchived", False):
                                    return False, "Game has been archived or removed", game_id
                                return True, "Game is accessible", game_id
                            elif isinstance(data, dict):
                                if "errors" in data or data.get("archived", False):
                                    return False, "Game is not available", game_id
                                return True, "Game is accessible", game_id
                        elif response.status == 404:
                            return False, "Game not found", game_id
                        elif response.status == 403:
                            return False, "Game access is restricted", game_id
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    continue

            try:
                async with self.session.head(
                    f"https://www.roblox.com/games/{game_id}/",
                    allow_redirects=True,
                    timeout=10
                ) as response:
                    if response.status == 404:
                        return False, "Game page not found", game_id
                    elif response.status == 403:
                        return False, "Access denied to game page", game_id
                    elif response.status in [200, 302, 301]:
                        final_url = str(response.url)
                        if "error" in final_url.lower() or "notfound" in final_url.lower():
                            return False, "Game may be unavailable", game_id
                        return True, "Game page is accessible", game_id
            except Exception as e:
                logger.debug(f"HEAD request failed: {e}")

            return True, "Unable to verify availability", game_id

        except Exception as e:
            logger.error(f"Roblox game verification error: {e}")
            return False, f"Verification error: {str(e)[:50]}", None

    async def health_check_services(self):
        while True:
            await asyncio.sleep(600)
            try:
                active_services = []
                for service in self.fallback_services:
                    try:
                        if service["method"] == "POST":
                            async with self.session.post(service["url"], json={"url": "https://linkvertise.com/1"}, timeout=5) as response:
                                if response.status == 200:
                                    active_services.append(service["url"])
                        else:
                            async with self.session.get(f"{service['url']}?url=https://linkvertise.com/1", timeout=5) as response:
                                if response.status == 200:
                                    active_services.append(service["url"])
                    except Exception as e:
                        logger.debug(f"Service {service['url']} check failed: {e}")
                        continue

                logger.info(f"Service health check: {len(active_services)}/{len(self.fallback_services)} services operational")

                for i, service in enumerate(self.fallback_services):
                    if service["url"] in active_services:
                        service["priority"] = i + 1
                    else:
                        service["priority"] = len(self.fallback_services) + i + 1

                self.fallback_services.sort(key=lambda x: x["priority"])

            except Exception as e:
                logger.error(f"Health check operation failed: {e}")

    async def load_cache_from_database(self):
        if not self.db_conn:
            return

        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                SELECT url_hash, bypassed_url, method 
                FROM url_cache 
                WHERE expires_at > CURRENT_TIMESTAMP
                ORDER BY access_count DESC
                LIMIT 500
            ''')

            loaded = 0
            for row in cursor.fetchall():
                cache_key = row[0]
                bypassed_url = row[1]
                method = BypassMethod(row[2])

                self.cache[cache_key] = (bypassed_url, time.time(), method)
                self.cache_order.append(cache_key)
                loaded += 1

            logger.info(f"Loaded {loaded} cache entries from database")

        except Exception as e:
            logger.error(f"Failed to load cache from database: {e}")

    async def save_to_cache_database(self, cache_key: str, original_url: str, bypassed_url: str, method: BypassMethod):
        if not self.db_conn:
            return

        try:
            expires_at = (datetime.now(UTC) + timedelta(seconds=self.CACHE_TTL)).strftime('%Y-%m-%d %H:%M:%S')

            cursor = self.db_conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO url_cache 
                (url_hash, original_url, bypassed_url, method, expires_at, access_count)
                VALUES (?, ?, ?, ?, ?, 
                    COALESCE((SELECT access_count FROM url_cache WHERE url_hash = ?), 0) + 1)
            ''', (cache_key, original_url, bypassed_url, method.value, expires_at, cache_key))

            self.db_conn.commit()

        except Exception as e:
            logger.error(f"Failed to save cache to database: {e}")

    async def load_authorized_users(self):
        if not self.db_conn:
            return

        try:
            cursor = self.db_conn.cursor()
            cursor.execute('SELECT user_id FROM authorized_users WHERE can_auto_bypass = 1')
            rows = cursor.fetchall()

            for row in rows:
                self.auto_bypass_allowed_users.add(row[0])

            logger.info(f"Loaded {len(rows)} authorized users from database")

        except Exception as e:
            logger.error(f"Failed to load authorized users: {e}")

    async def init_database(self):
        try:
            self.db_conn = sqlite3.connect('linkvertise_bot.db', check_same_thread=False)
            self.db_conn.row_factory = sqlite3.Row

            self.db_conn.execute('PRAGMA journal_mode = WAL')
            self.db_conn.execute('PRAGMA synchronous = NORMAL')
            self.db_conn.execute('PRAGMA cache_size = -2000')
            self.db_conn.execute('PRAGMA foreign_keys = ON')

            cursor = self.db_conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bypass_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    user_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    guild_id BIGINT,
                    guild_name TEXT,
                    original_url TEXT NOT NULL,
                    bypassed_url TEXT,
                    success BOOLEAN NOT NULL,
                    method TEXT NOT NULL,
                    execution_time FLOAT NOT NULL,
                    message TEXT NOT NULL
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_user ON bypass_logs(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_guild ON bypass_logs(guild_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON bypass_logs(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_success ON bypass_logs(success)')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    failed_requests INTEGER DEFAULT 0,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS server_stats (
                    guild_id BIGINT PRIMARY KEY,
                    guild_name TEXT NOT NULL,
                    total_requests INTEGER DEFAULT 0,
                    unique_users INTEGER DEFAULT 0,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date DATE PRIMARY KEY,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    unique_users INTEGER DEFAULT 0,
                    unique_servers INTEGER DEFAULT 0
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_cache (
                    url_hash TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    bypassed_url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    access_count INTEGER DEFAULT 1
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_expires ON url_cache(expires_at)')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS authorized_users (
                    user_id BIGINT PRIMARY KEY,
                    added_by BIGINT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    can_auto_bypass BOOLEAN DEFAULT 1,
                    can_use_batch BOOLEAN DEFAULT 0
                )
            ''')

            cursor.execute('''
                INSERT OR IGNORE INTO authorized_users (user_id, added_by, can_auto_bypass, can_use_batch)
                VALUES (?, ?, 1, 1)
            ''', (YOUR_USER_ID, YOUR_USER_ID))

            self.db_conn.commit()
            logger.info("Database initialization completed")

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            self.db_conn = None

    async def log_to_database(self, result: BypassResult, user: discord.User, guild: Optional[discord.Guild] = None):
        if not self.db_conn:
            return

        try:
            cursor = self.db_conn.cursor()
            cursor.execute('BEGIN TRANSACTION')

            cursor.execute('''
                INSERT INTO bypass_logs 
                (user_id, username, guild_id, guild_name, original_url, bypassed_url, 
                 success, method, execution_time, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user.id,
                str(user),
                guild.id if guild else None,
                guild.name if guild else 'Direct Message',
                result.original_url,
                result.url,
                result.success,
                result.method.value,
                result.execution_time,
                result.message
            ))

            cursor.execute('''
                INSERT OR REPLACE INTO user_stats 
                (user_id, username, total_requests, successful_requests, failed_requests, last_seen)
                VALUES (?, ?, 
                    COALESCE((SELECT total_requests FROM user_stats WHERE user_id = ?), 0) + 1,
                    COALESCE((SELECT successful_requests FROM user_stats WHERE user_id = ?), 0) + ?,
                    COALESCE((SELECT failed_requests FROM user_stats WHERE user_id = ?), 0) + ?,
                    CURRENT_TIMESTAMP)
            ''', (
                user.id,
                str(user),
                user.id,
                user.id,
                1 if result.success else 0,
                user.id,
                0 if result.success else 1
            ))

            if guild:
                cursor.execute('''
                    INSERT OR REPLACE INTO server_stats 
                    (guild_id, guild_name, total_requests, last_seen)
                    VALUES (?, ?, 
                        COALESCE((SELECT total_requests FROM server_stats WHERE guild_id = ?), 0) + 1,
                        CURRENT_TIMESTAMP)
                ''', (guild.id, guild.name, guild.id))

                cursor.execute('''
                    UPDATE server_stats 
                    SET unique_users = (
                        SELECT COUNT(DISTINCT user_id) 
                        FROM bypass_logs 
                        WHERE guild_id = ?
                    )
                    WHERE guild_id = ?
                ''', (guild.id, guild.id))

            today = datetime.now(UTC).date().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO daily_stats 
                (date, total_requests, successful_requests, unique_users, unique_servers)
                VALUES (?, 
                    COALESCE((SELECT total_requests FROM daily_stats WHERE date = ?), 0) + 1,
                    COALESCE((SELECT successful_requests FROM daily_stats WHERE date = ?), 0) + ?,
                    COALESCE((SELECT unique_users FROM daily_stats WHERE date = ?), 0),
                    COALESCE((SELECT unique_servers FROM daily_stats WHERE date = ?), 0))
            ''', (today, today, today, 1 if result.success else 0, today, today))

            cursor.execute('''
                UPDATE daily_stats 
                SET unique_users = (
                    SELECT COUNT(DISTINCT user_id) 
                    FROM bypass_logs 
                    WHERE DATE(timestamp) = ?
                ),
                unique_servers = (
                    SELECT COUNT(DISTINCT guild_id) 
                    FROM bypass_logs 
                    WHERE DATE(timestamp) = ? AND guild_id IS NOT NULL
                )
                WHERE date = ?
            ''', (today, today, today))

            self.db_conn.commit()

            self.performance_stats["total_requests"] += 1
            if result.success:
                self.performance_stats["successful_requests"] += 1
            self.performance_stats["success_rate"] = (
                self.performance_stats["successful_requests"] / max(self.performance_stats["total_requests"], 1)
            ) * 100

            alpha = 0.1
            old_avg = self.performance_stats["avg_response_time"]
            self.performance_stats["avg_response_time"] = (
                alpha * result.execution_time + (1 - alpha) * old_avg
            )

        except Exception as e:
            logger.error(f"Database logging failed: {e}")
            if self.db_conn:
                self.db_conn.rollback()

    async def send_log_to_discord(self, result: BypassResult, user: discord.User, guild: Optional[discord.Guild] = None):
        if not self.log_channel:
            guild_obj = self.get_guild(LOG_SERVER_ID)
            if guild_obj:
                self.log_channel = guild_obj.get_channel(LOG_CHANNEL_ID) or guild_obj.system_channel

        if not self.log_channel:
            return

        try:
            def truncate_url(url: str, max_len: int = 100) -> str:
                if len(url) <= max_len:
                    return url
                return url[:max_len-3] + "..."

            embed = discord.Embed(
                title="Bypass Operation Log",
                color=0x00FF00 if result.success else 0xFF0000,
                timestamp=datetime.now(UTC)
            )

            embed.add_field(name="User", value=f"{user} ({user.id})", inline=True)
            embed.add_field(name="Server", value=f"{guild.name if guild else 'Direct Message'} ({guild.id if guild else 'N/A'})", inline=True)
            embed.add_field(name="Status", value="Successful" if result.success else "Failed", inline=True)

            embed.add_field(name="Original URL", value=f"```{truncate_url(result.original_url)}```", inline=False)

            if result.success:
                embed.add_field(name="Bypassed URL", value=f"```{truncate_url(result.url)}```", inline=False)

            embed.add_field(name="Method", value=result.method.value.replace('_', ' ').title(), inline=True)
            embed.add_field(name="Processing Time", value=f"{result.execution_time:.2f}s", inline=True)

            if self.performance_stats["total_requests"] > 0:
                embed.add_field(
                    name="Performance Metrics",
                    value=f"Success Rate: {self.performance_stats['success_rate']:.1f}%\nAverage Response Time: {self.performance_stats['avg_response_time']:.2f}s",
                    inline=True
                )

            await self.log_channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to send log to Discord: {e}")

    async def auto_bypass_to_channel(self, urls: List[str], user: discord.User, source_guild: Optional[discord.Guild] = None):
        if not self.auto_bypass_channel:
            logger.error("Auto-bypass channel not available")
            return []

        if not urls:
            return []

        results = []
        successful_urls = []

        BATCH_SIZE = 5
        for i in range(0, len(urls), BATCH_SIZE):
            batch = urls[i:i + BATCH_SIZE]
            batch_tasks = []

            for url in batch:
                task = asyncio.create_task(self.bypass_linkvertise(
                    url=url,
                    user_id=user.id,
                    guild_id=source_guild.id if source_guild else None
                ))
                batch_tasks.append((url, task))

            for url, task in batch_tasks:
                try:
                    result = await asyncio.wait_for(task, timeout=30)
                    results.append(result)

                    if result.success and result.url:
                        successful_urls.append(result.url)
                        await self.log_to_database(result, user, source_guild)
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout while processing URL: {url}")
                except Exception as e:
                    logger.error(f"Error processing URL {url}: {e}")

            if i + BATCH_SIZE < len(urls):
                await asyncio.sleep(1)

        if successful_urls:
            await self.send_auto_bypass_results(successful_urls, user, len(urls))

        return successful_urls

    async def send_auto_bypass_results(self, successful_urls: List[str], user: discord.User, total_attempted: int):
        if not self.auto_bypass_channel:
            return

        try:
            ping_mention = f"<@&{PING_ROLE_ID}>" if self.ping_role else f"<@&{PING_ROLE_ID}>"

            if len(successful_urls) == 1:
                await self.auto_bypass_channel.send(f"{ping_mention} New game file bypassed")

                embed = discord.Embed(
                    title="Game File Bypassed",
                    description="File has been successfully bypassed",
                    color=0x00FF00,
                    timestamp=datetime.now(UTC)
                )

                final_url = successful_urls[0]
                display_url = final_url
                if len(display_url) > 200:
                    display_url = display_url[:197] + "..."

                embed.add_field(
                    name="Direct Link",
                    value=f"```{display_url}```",
                    inline=False
                )

                embed.add_field(
                    name="Bypassed By",
                    value=f"{user.mention} ({user.id})",
                    inline=True
                )

                embed.add_field(
                    name="Status",
                    value=f"1/{total_attempted} successful",
                    inline=True
                )

                view = discord.ui.View(timeout=None)
                view.add_item(discord.ui.Button(
                    label="Open Direct Link",
                    url=final_url,
                    style=discord.ButtonStyle.link
                ))

                await self.auto_bypass_channel.send(embed=embed, view=view)

            else:
                await self.auto_bypass_channel.send(f"{ping_mention} {len(successful_urls)} game files bypassed")

                for i in range(0, len(successful_urls), 3):
                    batch_urls = successful_urls[i:i + 3]
                    batch_num = i // 3 + 1
                    total_batches = (len(successful_urls) + 2) // 3

                    embed = discord.Embed(
                        title=f"Game Files Bypassed (Batch {batch_num}/{total_batches})",
                        description=f"Files {i+1}-{min(i+3, len(successful_urls))} of {len(successful_urls)}",
                        color=0x00FF00,
                        timestamp=datetime.now(UTC)
                    )

                    for j, final_url in enumerate(batch_urls, 1):
                        display_url = final_url
                        if len(display_url) > 150:
                            display_url = display_url[:147] + "..."

                        embed.add_field(
                            name=f"Direct Link {i + j}",
                            value=f"```{display_url}```",
                            inline=False
                        )

                    if batch_num == 1:
                        embed.add_field(
                            name="Bypassed By",
                            value=f"{user.mention} ({user.id})",
                            inline=True
                        )

                        embed.add_field(
                            name="Status",
                            value=f"{len(successful_urls)}/{total_attempted} successful",
                            inline=True
                        )

                    view = discord.ui.View(timeout=None)
                    for j, final_url in enumerate(batch_urls, 1):
                        view.add_item(discord.ui.Button(
                            label=f"Open Link {i + j}",
                            url=final_url,
                            style=discord.ButtonStyle.link
                        ))

                    await self.auto_bypass_channel.send(embed=embed, view=view)

            logger.info(f"Auto-bypass results sent to channel: {len(successful_urls)} URLs")

        except Exception as e:
            logger.error(f"Failed to send auto-bypass results: {e}")

    async def close(self):
        if self.session:
            await self.session.close()
        if self.db_conn:
            self.db_conn.close()
        if hasattr(self, 'cleanup_task'):
            self.cleanup_task.cancel()
        if hasattr(self, 'stats_task'):
            self.stats_task.cancel()
        if hasattr(self, 'health_check_task'):
            self.health_check_task.cancel()
        self.thread_pool.shutdown(wait=True)
        await super().close()

    async def periodic_cleanup(self):
        while True:
            await asyncio.sleep(300)
            try:
                self.cleanup_old_data()
                await self.cleanup_database_cache()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def cleanup_database_cache(self):
        if not self.db_conn:
            return

        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                DELETE FROM url_cache 
                WHERE expires_at < CURRENT_TIMESTAMP
                OR (julianday('now') - julianday(created_at)) > 7
            ''')

            if cursor.rowcount > 100:
                cursor.execute('VACUUM')

            self.db_conn.commit()
            logger.info(f"Cleaned {cursor.rowcount} expired cache entries from database")

        except Exception as e:
            logger.error(f"Database cache cleanup error: {e}")

    async def periodic_stats_log(self):
        await self.wait_until_ready()
        await asyncio.sleep(60)

        while True:
            now = datetime.now(UTC)
            if self.last_daily_stats_time is None or (now - self.last_daily_stats_time) >= timedelta(hours=24):
                await self.log_24hour_stats()
                self.last_daily_stats_time = now

            await asyncio.sleep(3600)

    async def log_24hour_stats(self):
        if not self.db_conn or not self.log_channel:
            return

        try:
            cursor = self.db_conn.cursor()
            twenty_four_hours_ago = (datetime.now(UTC) - timedelta(hours=24)).strftime('%Y-%m-d %H:%M:%S')

            cursor.execute('''
                SELECT 
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT guild_id) as unique_servers
                FROM bypass_logs 
                WHERE timestamp >= ?
            ''', (twenty_four_hours_ago,))

            row = cursor.fetchone()
            
            if row:
                total_requests = row[0] or 0
                successful_requests = row[1] or 0
                unique_users = row[2] or 0
                unique_servers = row[3] or 0
            else:
                total_requests = 0
                successful_requests = 0
                unique_users = 0
                unique_servers = 0

            cursor.execute('''
                SELECT username, COUNT(*) as request_count 
                FROM bypass_logs 
                WHERE timestamp >= ? 
                GROUP BY username 
                ORDER BY request_count DESC 
                LIMIT 5
            ''', (twenty_four_hours_ago,))
            top_users = cursor.fetchall()

            cursor.execute('''
                SELECT method, COUNT(*) as count 
                FROM bypass_logs 
                WHERE timestamp >= ? 
                GROUP BY method
                ORDER BY count DESC
            ''', (twenty_four_hours_ago,))
            method_counts = cursor.fetchall()

            embed = discord.Embed(
                title="24 Hour Statistics Report",
                color=0x7289DA,
                timestamp=datetime.now(UTC)
            )

            success_rate = 0
            if total_requests > 0:
                success_rate = (successful_requests / total_requests) * 100

            embed.add_field(name="Total Requests", value=f"{total_requests:,}", inline=True)
            embed.add_field(name="Successful", value=f"{successful_requests:,}", inline=True)
            embed.add_field(name="Success Rate", value=f"{success_rate:.1f}%", inline=True)

            embed.add_field(name="Unique Users", value=f"{unique_users:,}", inline=True)
            embed.add_field(name="Unique Servers", value=f"{unique_servers:,}", inline=True)
            embed.add_field(name="Memory Cache", value=f"{len(self.cache):,} entries", inline=True)

            if method_counts:
                methods_text = "\n".join([f"• {method_row[0]}: {method_row[1]:,}" for method_row in method_counts])
                embed.add_field(name="Methods Used", value=methods_text, inline=False)

            if top_users:
                top_users_text = "\n".join([f"• {user_row[0]}: {user_row[1]:,}" for user_row in top_users])
                embed.add_field(name="Top Users (24 Hours)", value=top_users_text, inline=False)

            embed.add_field(
                name="Performance Summary",
                value=f"• Average Response Time: {self.performance_stats['avg_response_time']:.2f}s\n• Overall Success Rate: {self.performance_stats['success_rate']:.1f}%\n• Active Services: {len([s for s in self.fallback_services if s['priority'] <= 3])}/{len(self.fallback_services)}",
                inline=False
            )

            await self.log_channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to log 24-hour stats: {e}")

    def cleanup_old_data(self):
        now = time.time()

        expired_keys = [
            key for key, (_, timestamp, _) in self.cache.items()
            if now - timestamp > self.CACHE_TTL
        ]

        for key in expired_keys:
            del self.cache[key]
            if key in self.cache_order:
                self.cache_order.remove(key)

        if len(self.cache) > self.MAX_CACHE_SIZE:
            keys_to_remove = self.cache_order[:len(self.cache) - self.MAX_CACHE_SIZE]
            for key in keys_to_remove:
                if key in self.cache:
                    del self.cache[key]
            self.cache_order = self.cache_order[len(keys_to_remove):]

        one_hour_ago = now - 3600
        for user_id in list(self.request_count.keys()):
            self.request_count[user_id] = [
                req_time for req_time in self.request_count[user_id]
                if req_time > one_hour_ago
            ]
            if not self.request_count[user_id]:
                del self.request_count[user_id]

    def update_cache_order(self, key: str):
        if key in self.cache_order:
            self.cache_order.remove(key)
            self.cache_order.append(key)

    async def on_ready(self):
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')

        try:
            synced = await self.tree.sync()
            logger.info(f'Synced {len(synced)} command(s)')
        except Exception as e:
            logger.error(f'Failed to sync commands: {e}')

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"/bypass | {len(self.guilds)} servers"
            )
        )

        logger.info(f"Bot started with {len(self.guilds)} guilds")

        try:
            guild = self.get_guild(AUTO_BYPASS_SERVER_ID)
            if guild:
                self.auto_bypass_channel = guild.get_channel(AUTO_BYPASS_CHANNEL_ID)
                if self.auto_bypass_channel:
                    logger.info(f"Auto-bypass channel loaded: #{self.auto_bypass_channel.name}")
                else:
                    logger.warning(f"Auto-bypass channel not found in guild. ID: {AUTO_BYPASS_CHANNEL_ID}")

                    try:
                        channel = await self.fetch_channel(AUTO_BYPASS_CHANNEL_ID)
                        if isinstance(channel, discord.TextChannel):
                            self.auto_bypass_channel = channel
                            logger.info(f"Force-fetched auto-bypass channel: #{channel.name}")
                    except Exception as fetch_error:
                        logger.error(f"Failed to fetch auto-bypass channel: {fetch_error}")
            else:
                logger.warning(f"Auto-bypass guild not found. ID: {AUTO_BYPASS_SERVER_ID}")
        except Exception as e:
            logger.error(f"Failed to load auto-bypass channel: {e}")

        if self.auto_bypass_channel and self.auto_bypass_channel.guild:
            self.ping_role = self.auto_bypass_channel.guild.get_role(PING_ROLE_ID)
            if self.ping_role:
                logger.info(f"Ping role loaded: @{self.ping_role.name}")
            else:
                logger.warning(f"Ping role not found. ID: {PING_ROLE_ID}")

        try:
            log_guild = self.get_guild(LOG_SERVER_ID)
            if log_guild:
                self.log_channel = log_guild.get_channel(LOG_CHANNEL_ID) or log_guild.system_channel
                if self.log_channel:
                    logger.info(f"Log channel loaded: #{self.log_channel.name}")
                else:
                    logger.warning(f"Log channel not found in guild. ID: {LOG_CHANNEL_ID}")
            else:
                logger.warning(f"Log guild not found. ID: {LOG_SERVER_ID}")
        except Exception as e:
            logger.error(f"Failed to load log channel: {e}")

        await self.send_startup_notification()

    async def send_startup_notification(self):
        if self.log_channel:
            embed = discord.Embed(
                title="Bot Started",
                description=f"Bot is now online in {len(self.guilds)} servers",
                color=0x00FF00,
                timestamp=datetime.now(UTC)
            )
            embed.add_field(name="Bot User", value=f"{self.user} ({self.user.id})", inline=True)
            embed.add_field(name="Database", value="Connected" if self.db_conn else "Disabled", inline=True)
            embed.add_field(name="Auto-bypass Channel", 
                          value=f"Connected: #{self.auto_bypass_channel.name}" if self.auto_bypass_channel else "Not found", 
                          inline=True)
            embed.add_field(name="Ping Role", 
                          value=f"Loaded: @{self.ping_role.name}" if self.ping_role else "Not found", 
                          inline=True)
            embed.add_field(name="Cache Size", value=f"{len(self.cache):,} entries", inline=True)
            embed.add_field(name="Authorized Users", value=f"{len(self.auto_bypass_allowed_users):,} users", inline=True)
            await self.log_channel.send(embed=embed)
        else:
            logger.warning("Log channel not available for startup notification")

    def get_cache_key(self, url: str) -> str:
        normalized = url.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()

    def is_valid_linkvertise_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False
            
            url_lower = url.lower()
            domain = parsed.netloc.lower()
            
            for lv_domain in self.linkvertise_domains:
                if domain == lv_domain or domain.endswith('.' + lv_domain):
                    return True
            
            if 'linkvertise' in url_lower:
                return True
            
            linkvertise_patterns = [
                r'linkvertise\.(com|net|io|co|download|pw|xyz|tk|ml|ga|cf|gq)',
                r'up-to-down\.net',
                r'link-to\.net',
                r'linkv\.to',
                r'lv\.to',
                r'link-center\.net',
                r'/linkvertise\.com/',
                r'linkvertise/[0-9]+',
                r'linkvertise/access/[0-9]+',
                r'linkvertise/[0-9]+/[0-9]+/dynamic',
            ]
            
            for pattern in linkvertise_patterns:
                if re.search(pattern, url_lower, re.IGNORECASE):
                    return True
            
            common_paths = ['/access/', '/download/', '/view/', '/file/', '/folder/', '/content/', '/media/', '/dynamic']
            for path in common_paths:
                if path in parsed.path:
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"URL validation error for {url}: {e}")
            return True

    def extract_all_linkvertise_urls(self, text: str) -> List[str]:
        patterns = self.linkvertise_patterns + [
            r'\[[^\]]*\]\s*\(\s*(https?://[^\s<>"\']*linkvertise[^\s<>"\']*)\s*\)',
            r'[\[\(]?\s*(https?://[^\s<>"\']*linkvertise[^\s<>"\']*)\s*[\]\)]?',
        ]
        
        all_urls = set()
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                url = match.strip('[]()<>"\'')
                if url and self.is_valid_linkvertise_url(url):
                    all_urls.add(url)
        
        return list(all_urls)

    def extract_linkvertise_urls(self, text: str) -> List[str]:
        return self.extract_all_linkvertise_urls(text)

    def check_rate_limit(self, user_id: int) -> Tuple[bool, int, Optional[str]]:
        now = time.time()
        
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {
                'total_requests': 0,
                'successful_bypasses': 0,
                'failed_bypasses': 0,
                'last_request': now,
                'daily_requests': 0,
                'reset_time': now + 86400
            }
        
        user_data = self.user_stats[user_id]
        if now > user_data['reset_time']:
            user_data['daily_requests'] = 0
            user_data['reset_time'] = now + 86400
        
        if user_data['daily_requests'] >= self.DAILY_LIMIT:
            reset_in = int(user_data['reset_time'] - now)
            hours = reset_in // 3600
            minutes = (reset_in % 3600) // 60
            return False, reset_in, f"Daily limit reached ({self.DAILY_LIMIT} requests). Resets in {hours}h {minutes}m"
        
        user_requests = self.request_count.get(user_id, [])
        recent_requests = [req for req in user_requests if now - req < self.RATE_LIMIT_WINDOW]
        
        if len(recent_requests) >= self.MAX_REQUESTS_PER_WINDOW:
            oldest = min(recent_requests)
            reset_in = int(oldest + self.RATE_LIMIT_WINDOW - now)
            return False, reset_in, f"Rate limit exceeded ({self.MAX_REQUESTS_PER_WINDOW}/min). Try again in {reset_in}s"
        
        recent_requests.append(now)
        self.request_count[user_id] = recent_requests[-50:]
        user_data['total_requests'] += 1
        user_data['daily_requests'] += 1
        user_data['last_request'] = now
        
        return True, 0, None

    def extract_dynamic_url(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            
            if 'r' in query_params:
                encoded = query_params['r'][0]
                
                for encoded_str in [encoded, unquote(encoded)]:
                    try:
                        padding = 4 - (len(encoded_str) % 4)
                        if padding != 4:
                            encoded_str += '=' * padding
                        
                        decoded = base64.b64decode(encoded_str).decode('utf-8', errors='ignore')
                        
                        if decoded.startswith(('http://', 'https://')):
                            return decoded
                    except:
                        continue
            
            for param in ['url', 'link', 'redirect', 'destination', 'target', 'dl', 'u']:
                if param in query_params:
                    potential_url = query_params[param][0]
                    if potential_url.startswith(('http://', 'https://')):
                        return unquote(potential_url)
            
            path_parts = parsed.path.split('/')
            for i, part in enumerate(path_parts):
                if part == 'dynamic' and i > 0:
                    try:
                        encoded_part = path_parts[i-1]
                        padding = 4 - (len(encoded_part) % 4)
                        if padding != 4:
                            encoded_part += '=' * padding
                        
                        decoded = base64.b64decode(encoded_part).decode('utf-8', errors='ignore')
                        if decoded.startswith(('http://', 'https://')):
                            return decoded
                    except:
                        continue
            
            return None
            
        except Exception as e:
            logger.error(f"Dynamic URL extraction failed: {e}")
            return None

    async def bypass_dynamic_link(self, url: str) -> BypassResult:
        start_time = time.time()
        
        try:
            extracted = self.extract_dynamic_url(url)
            if extracted:
                try:
                    async with self.session.head(
                        extracted, 
                        allow_redirects=True,
                        timeout=10
                    ) as response:
                        final_url = str(response.url)
                        
                        if not final_url.startswith(('http://', 'https://')):
                            final_url = extracted
                    
                    self.method_stats[BypassMethod.DYNAMIC_DECODE.value] += 1
                    return BypassResult(
                        success=True,
                        url=final_url,
                        message="Dynamic link decoded and resolved",
                        method=BypassMethod.DYNAMIC_DECODE,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
                except Exception as redirect_error:
                    logger.warning(f"Redirect failed for {url}: {redirect_error}")
                    self.method_stats[BypassMethod.DYNAMIC_DECODE.value] += 1
                    return BypassResult(
                        success=True,
                        url=extracted,
                        message="Dynamic link decoded successfully",
                        method=BypassMethod.DYNAMIC_DECODE,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
            else:
                return BypassResult(
                    success=False,
                    url=None,
                    message="Could not decode dynamic link",
                    method=BypassMethod.DYNAMIC_DECODE,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
                
        except Exception as e:
            logger.error(f"Dynamic bypass error: {e}")
            return BypassResult(
                success=False,
                url=None,
                message=f"Dynamic decode error: {str(e)[:100]}",
                method=BypassMethod.DYNAMIC_DECODE,
                execution_time=time.time() - start_time,
                original_url=url
            )

    async def bypass_api_link(self, url: str) -> BypassResult:
        start_time = time.time()
        
        try:
            cache_key = self.get_cache_key(url)
            
            if cache_key in self.cache:
                cached_url, _, method = self.cache[cache_key]
                self.update_cache_order(cache_key)
                self.method_stats[BypassMethod.CACHED.value] += 1
                return BypassResult(
                    success=True,
                    url=cached_url,
                    message="Retrieved from cache",
                    method=BypassMethod.CACHED,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            
            if not path:
                return BypassResult(
                    success=False,
                    url=None,
                    message="Invalid URL path",
                    method=BypassMethod.API_BYPASS,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            link_id = None
            id_patterns = [
                r'/(\d+)(?:/|$)',
                r'/access/(\d+)/',
                r'/download/(\d+)/',
                r'/view/(\d+)/',
                r'/file/(\d+)/',
                r'/folder/(\d+)/',
                r'/content/(\d+)/',
                r'/media/(\d+)/',
            ]
            
            for pattern in id_patterns:
                match = re.search(pattern, parsed.path)
                if match:
                    link_id = match.group(1)
                    break
            
            if not link_id:
                query_params = parse_qs(parsed.query)
                for param in ['id', 'link', 'file', 'download', 'content', 'media', 'u']:
                    if param in query_params:
                        value = query_params[param][0]
                        if value.isdigit():
                            link_id = value
                            break
            
            if not link_id:
                numeric_parts = re.findall(r'\d+', path)
                if numeric_parts:
                    link_id = max(numeric_parts, key=len)
            
            if not link_id:
                return BypassResult(
                    success=False,
                    url=None,
                    message="Could not extract link ID from URL",
                    method=BypassMethod.API_BYPASS,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            api_endpoints = [
                f"https://publisher.linkvertise.com/api/v1/redirect/link/{link_id}/target",
                f"https://publisher.linkvertise.com/api/v1/redirect/link/static/{link_id}",
                f"https://linkvertise.com/api/v1/redirect/link/{link_id}/target",
                f"https://linkvertise.net/api/v1/redirect/link/{link_id}/target",
                f"https://linkvertise.io/api/v1/redirect/link/{link_id}/target",
                f"https://linkvertise.co/api/v1/redirect/link/{link_id}/target",
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://linkvertise.com/',
                'Origin': 'https://linkvertise.com',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }
            
            random_num = random.randint(1000000, 9999999)
            serial_data = {
                "timestamp": int(time.time() * 1000),
                "random": str(random_num),
                "link_id": link_id
            }
            serial_json = json.dumps(serial_data, separators=(',', ':'))
            serial_base64 = base64.b64encode(serial_json.encode()).decode()
            
            final_url = None
            
            for api_url in api_endpoints:
                try:
                    target_url = f"{api_url}?serial={serial_base64}"
                    
                    async with self.session.get(
                        target_url, 
                        headers=headers,
                        timeout=10
                    ) as response:
                        if response.status == 200:
                            try:
                                target_data = await response.json()
                                if target_data.get("success", False):
                                    final_url = target_data.get("data", {}).get("target")
                                    if final_url:
                                        break
                            except json.JSONDecodeError:
                                text = await response.text()
                                url_patterns = [
                                    r'"target"\s*:\s*"([^"]+)"',
                                    r'"url"\s*:\s*"([^"]+)"',
                                    r'"destination"\s*:\s*"([^"]+)"',
                                    r'https?://[^\s<>"\']+(?:mega|mediafire|google|dropbox|drive)[^\s<>"\']*',
                                ]
                                for pattern in url_patterns:
                                    match = re.search(pattern, text)
                                    if match:
                                        final_url = match.group(1) if '"' in pattern else match.group(0)
                                        if final_url:
                                            break
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    continue
            
            if final_url:
                try:
                    async with self.session.head(
                        final_url, 
                        allow_redirects=True, 
                        timeout=10
                    ) as redirect_response:
                        actual_final_url = str(redirect_response.url)
                    
                    self.cache[cache_key] = (actual_final_url, time.time(), BypassMethod.API_BYPASS)
                    self.update_cache_order(cache_key)
                    
                    asyncio.create_task(
                        self.save_to_cache_database(cache_key, url, actual_final_url, BypassMethod.API_BYPASS)
                    )
                    
                    self.method_stats[BypassMethod.API_BYPASS.value] += 1
                    return BypassResult(
                        success=True,
                        url=actual_final_url,
                        message="API bypass successful",
                        method=BypassMethod.API_BYPASS,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
                except Exception as redirect_error:
                    logger.warning(f"Redirect failed: {redirect_error}")
                    self.cache[cache_key] = (final_url, time.time(), BypassMethod.API_BYPASS)
                    self.update_cache_order(cache_key)
                    
                    asyncio.create_task(
                        self.save_to_cache_database(cache_key, url, final_url, BypassMethod.API_BYPASS)
                    )
                    
                    self.method_stats[BypassMethod.API_BYPASS.value] += 1
                    return BypassResult(
                        success=True,
                        url=final_url,
                        message="API bypass successful",
                        method=BypassMethod.API_BYPASS,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
            
            return await self.extract_from_page_content(url, cache_key, start_time)
            
        except Exception as e:
            logger.error(f"API bypass error: {e}")
            return BypassResult(
                success=False,
                url=None,
                message=f"Unexpected error: {str(e)[:100]}",
                method=BypassMethod.API_BYPASS,
                execution_time=time.time() - start_time,
                original_url=url
            )

    async def extract_from_page_content(self, url: str, cache_key: str, start_time: float) -> BypassResult:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
            
            async with self.session.get(
                url, 
                headers=headers, 
                allow_redirects=True,
                timeout=15
            ) as response:
                content = await response.text()
                
                url_patterns = [
                    r'window\.location\.(?:href|replace)\s*=\s*["\']([^"\']+)["\']',
                    r'<meta[^>]*http-equiv\s*=\s*["\']refresh["\'][^>]*url\s*=\s*([^"\']+)["\']',
                    r'content\s*=\s*["\']\d+;\s*url\s*=\s*([^"\']+)["\']',
                    r'<a[^>]*href\s*=\s*["\']([^"\']+\.(?:mega|mediafire|google|dropbox|drive|zippyshare|1fichier|pixeldrain|anonfiles)\.(?:com|nz|net|org|eu|co|uk)[^"\']*)["\']',
                    r'https?://[^\s<>"\']+\.(?:rar|zip|7z|exe|apk|ipa|dmg|iso|tar\.gz|tar\.xz)[^\s<>"\']*',
                    r'https?://[^\s<>"\']+/d/[A-Za-z0-9_-]+',
                    r'https?://[^\s<>"\']+/file/[A-Za-z0-9_-]+',
                    r'https?://[^\s<>"\']+/folder/[A-Za-z0-9_-]+',
                    r'https?://[^\s<>"\']+/view/[A-Za-z0-9_-]+',
                    r'data-url\s*=\s*["\']([^"\']+)["\']',
                    r'data-redirect\s*=\s*["\']([^"\']+)["\']',
                    r'data-link\s*=\s*["\']([^"\']+)["\']',
                ]
                
                for pattern in url_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match_url in matches:
                        if match_url and 'linkvertise' not in match_url.lower():
                            try:
                                async with self.session.head(
                                    match_url, 
                                    allow_redirects=True, 
                                    timeout=10
                                ) as redirect_response:
                                    actual_final_url = str(redirect_response.url)
                                self.cache[cache_key] = (actual_final_url, time.time(), BypassMethod.PAGE_EXTRACTION)
                                self.update_cache_order(cache_key)
                                
                                asyncio.create_task(
                                    self.save_to_cache_database(cache_key, url, actual_final_url, BypassMethod.PAGE_EXTRACTION)
                                )
                                
                                self.method_stats[BypassMethod.PAGE_EXTRACTION.value] += 1
                                return BypassResult(
                                    success=True,
                                    url=actual_final_url,
                                    message="Extracted URL from page content",
                                    method=BypassMethod.PAGE_EXTRACTION,
                                    execution_time=time.time() - start_time,
                                    original_url=url
                                )
                            except:
                                self.cache[cache_key] = (match_url, time.time(), BypassMethod.PAGE_EXTRACTION)
                                self.update_cache_order(cache_key)
                                
                                asyncio.create_task(
                                    self.save_to_cache_database(cache_key, url, match_url, BypassMethod.PAGE_EXTRACTION)
                                )
                                
                                self.method_stats[BypassMethod.PAGE_EXTRACTION.value] += 1
                                return BypassResult(
                                    success=True,
                                    url=match_url,
                                    message="Extracted URL from page content",
                                    method=BypassMethod.PAGE_EXTRACTION,
                                    execution_time=time.time() - start_time,
                                    original_url=url
                                )
                
                return BypassResult(
                    success=False,
                    url=None,
                    message="Could not extract URL from page content",
                    method=BypassMethod.PAGE_EXTRACTION,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
                
        except Exception as e:
            logger.warning(f"Page content extraction failed: {e}")
            return BypassResult(
                success=False,
                url=None,
                message=f"Page extraction failed: {str(e)[:50]}",
                method=BypassMethod.PAGE_EXTRACTION,
                execution_time=time.time() - start_time,
                original_url=url
            )

    async def bypass_fallback(self, url: str) -> BypassResult:
        start_time = time.time()
        
        services = sorted(self.fallback_services, key=lambda x: x["priority"])
        
        for service in services:
            try:
                if service["method"] == "POST":
                    payload = {"url": url}
                    async with self.session.post(
                        service["url"], 
                        json=payload, 
                        timeout=service["timeout"]
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("success") and data.get("destination"):
                                try:
                                    final_url = data["destination"]
                                    async with self.session.head(
                                        final_url, 
                                        allow_redirects=True, 
                                        timeout=10
                                    ) as redirect_response:
                                        actual_final_url = str(redirect_response.url)
                                    self.method_stats[BypassMethod.FALLBACK_SERVICE.value] += 1
                                    return BypassResult(
                                        success=True,
                                        url=actual_final_url,
                                        message=f"Fallback successful via {service['url']}",
                                        method=BypassMethod.FALLBACK_SERVICE,
                                        execution_time=time.time() - start_time,
                                        original_url=url
                                    )
                                except:
                                    self.method_stats[BypassMethod.FALLBACK_SERVICE.value] += 1
                                    return BypassResult(
                                        success=True,
                                        url=data["destination"],
                                        message=f"Fallback successful via {service['url']}",
                                        method=BypassMethod.FALLBACK_SERVICE,
                                        execution_time=time.time() - start_time,
                                        original_url=url
                                    )
                else:
                    params = {"url": url}
                    async with self.session.get(
                        service["url"], 
                        params=params, 
                        timeout=service["timeout"]
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("success") and data.get("destination"):
                                try:
                                    final_url = data["destination"]
                                    async with self.session.head(
                                        final_url, 
                                        allow_redirects=True, 
                                        timeout=10
                                    ) as redirect_response:
                                        actual_final_url = str(redirect_response.url)
                                    self.method_stats[BypassMethod.FALLBACK_SERVICE.value] += 1
                                    return BypassResult(
                                        success=True,
                                        url=actual_final_url,
                                        message=f"Fallback successful via {service['url']}",
                                        method=BypassMethod.FALLBACK_SERVICE,
                                        execution_time=time.time() - start_time,
                                        original_url=url
                                    )
                                except:
                                    self.method_stats[BypassMethod.FALLBACK_SERVICE.value] += 1
                                    return BypassResult(
                                        success=True,
                                        url=data["destination"],
                                        message=f"Fallback successful via {service['url']}",
                                        method=BypassMethod.FALLBACK_SERVICE,
                                        execution_time=time.time() - start_time,
                                        original_url=url
                                    )
                
            except Exception as e:
                logger.warning(f"Fallback service {service['url']} failed: {e}")
                continue
        
        return BypassResult(
            success=False,
            url=None,
            message="All fallback services failed",
            method=BypassMethod.FALLBACK_SERVICE,
            execution_time=time.time() - start_time,
            original_url=url
        )

    async def bypass_linkvertise(self, url: str, user_id: int, guild_id: Optional[int] = None) -> BypassResult:
        logger.info(f"Processing bypass request for {url} from user {user_id}")
        
        if url in self.blocked_urls:
            return BypassResult(
                success=False,
                url=None,
                message="This URL is blocked",
                method=BypassMethod.API_BYPASS,
                execution_time=0,
                original_url=url,
                user_id=user_id,
                guild_id=guild_id
            )
        
        cache_key = self.get_cache_key(url)
        if cache_key in self.cache:
            cached_url, _, method = self.cache[cache_key]
            self.update_cache_order(cache_key)
            self.method_stats[BypassMethod.CACHED.value] += 1
            
            if "roblox.com/games/" in cached_url.lower():
                is_available, status_msg, game_id = await self.check_roblox_game_availability(cached_url)
                if not is_available:
                    del self.cache[cache_key]
                    if cache_key in self.cache_order:
                        self.cache_order.remove(cache_key)
                    
                    return BypassResult(
                        success=False,
                        url=cached_url,
                        message=f"Game is unavailable: {status_msg}",
                        method=BypassMethod.CACHED,
                        execution_time=0.01,
                        original_url=url,
                        user_id=user_id,
                        guild_id=guild_id,
                        additional_info={"game_id": game_id, "status": "unavailable"}
                    )
            
            result = BypassResult(
                success=True,
                url=cached_url,
                message="Retrieved from cache",
                method=BypassMethod.CACHED,
                execution_time=0.01,
                original_url=url,
                user_id=user_id,
                guild_id=guild_id
            )
            
            if user_id in self.user_stats:
                self.user_stats[user_id]['successful_bypasses'] += 1
            
            return result
        
        methods = [
            (self.bypass_dynamic_link, "Dynamic decode"),
            (self.bypass_api_link, "API bypass"),
            (self.bypass_fallback, "Fallback service"),
        ]
        
        all_results = []
        
        for method_func, method_name in methods:
            try:
                result = await method_func(url)
                all_results.append(result)
                
                if result.success and result.url:
                    if "roblox.com/games/" in result.url.lower():
                        is_available, status_msg, game_id = await self.check_roblox_game_availability(result.url)
                        
                        if not is_available:
                            result.success = False
                            result.message = f"Bypass succeeded but game is unavailable: {status_msg}"
                            result.additional_info = {"game_id": game_id, "status": "unavailable"}
                            
                            if user_id in self.user_stats:
                                self.user_stats[user_id]['failed_bypasses'] += 1
                            
                            return result
                        
                        result.additional_info = {
                            "game_id": game_id,
                            "status": "available",
                            "status_msg": status_msg
                        }
                    
                    result.user_id = user_id
                    result.guild_id = guild_id
                    
                    self.cache[cache_key] = (result.url, time.time(), result.method)
                    self.update_cache_order(cache_key)
                    
                    asyncio.create_task(
                        self.save_to_cache_database(cache_key, url, result.url, result.method)
                    )
                    
                    if user_id in self.user_stats:
                        self.user_stats[user_id]['successful_bypasses'] += 1
                    
                    return result
                    
            except Exception as e:
                logger.warning(f"{method_name} failed for {url}: {e}")
                continue
        
        best_result = None
        for result in all_results:
            if result.url and (best_result is None or result.execution_time < best_result.execution_time):
                best_result = result
        
        if best_result:
            best_result.user_id = user_id
            best_result.guild_id = guild_id
            if user_id in self.user_stats:
                self.user_stats[user_id]['failed_bypasses'] += 1
            return best_result
        
        result = BypassResult(
            success=False,
            url=None,
            message="All bypass methods failed",
            method=BypassMethod.FALLBACK_SERVICE,
            execution_time=time.time() - time.time(),
            original_url=url,
            user_id=user_id,
            guild_id=guild_id
        )
        
        if user_id in self.user_stats:
            self.user_stats[user_id]['failed_bypasses'] += 1
        
        return result

bot = EnhancedLinkvertiseBypassBot()

@bot.tree.command(name="bypass", description="Bypass a Linkvertise link")
@app_commands.describe(url="The Linkvertise URL to bypass")
@app_commands.checks.cooldown(1, 3.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def bypass_command(interaction: discord.Interaction, url: str):
    await interaction.response.defer(thinking=True)
    
    if not interaction.guild:
        embed = discord.Embed(
            title="Command Not Available",
            description="This command can only be used in servers, not in direct messages.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed)
        return
    
    if not bot.is_valid_linkvertise_url(url):
        embed = discord.Embed(
            title="Invalid URL",
            description="The provided URL is not a valid Linkvertise link.",
            color=0xFF0000
        )
        embed.add_field(
            name="Supported formats",
            value="All Linkvertise domains and formats are supported",
            inline=False
        )
        await interaction.followup.send(embed=embed)
        return
    
    allowed, reset_time, error_msg = bot.check_rate_limit(interaction.user.id)
    if not allowed:
        embed = discord.Embed(
            title="Rate Limited",
            description=error_msg or f"Please wait {reset_time} seconds.",
            color=0xFFA500
        )
        embed.set_footer(text=f"Daily limit: {bot.DAILY_LIMIT} requests")
        await interaction.followup.send(embed=embed)
        return
    
    result = await bot.bypass_linkvertise(
        url=url,
        user_id=interaction.user.id,
        guild_id=interaction.guild.id if interaction.guild else None
    )
    
    await bot.log_to_database(result, interaction.user, interaction.guild)
    await bot.send_log_to_discord(result, interaction.user, interaction.guild)
    
    game_status = None
    game_id = None
    if result.additional_info and "game_id" in result.additional_info:
        game_id = result.additional_info.get("game_id")
        game_status = result.additional_info.get("status_msg", result.additional_info.get("status"))
    
    if result.success:
        embed = discord.Embed(
            title="Bypass Successful",
            color=0x00FF00,
            timestamp=datetime.now(UTC)
        )
        
        if game_status:
            embed.add_field(
                name="Game Status",
                value=f"{game_status}",
                inline=True
            )
        
        embed.add_field(
            name="Original Link",
            value=f"```{url[:100] + '...' if len(url) > 100 else url}```",
            inline=False
        )
        
        embed.add_field(
            name="Direct Link",
            value=f"```{result.url[:100] + '...' if len(result.url) > 100 else result.url}```",
            inline=False
        )
        
        embed.add_field(
            name="Method",
            value=f"{result.method.value.replace('_', ' ').title()}",
            inline=True
        )
        
        embed.add_field(
            name="Time",
            value=f"{result.execution_time:.2f}s",
            inline=True
        )
        
        if interaction.user.id not in bot.auto_bypass_allowed_users:
            embed.add_field(
                name="Additional Features",
                value=f"Access to autobypass and batch commands available.\nUse request_access command to learn how.",
                inline=False
            )
        
        embed.set_footer(
            text=f"Requested by {interaction.user.name}",
            icon_url=interaction.user.display_avatar.url
        )
        
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(
            label="Open Direct Link",
            url=result.url,
            style=discord.ButtonStyle.link
        ))
        
        if interaction.user.id in bot.auto_bypass_allowed_users and bot.auto_bypass_channel:
            try:
                ping_mention = f"<@&{PING_ROLE_ID}>" if bot.ping_role else f"<@&{PING_ROLE_ID}>"
                await bot.auto_bypass_channel.send(f"{ping_mention} Game file bypassed via command")
                
                embed_channel = discord.Embed(
                    title="Game File Bypassed",
                    description="File has been successfully bypassed",
                    color=0x00FF00,
                    timestamp=datetime.now(UTC)
                )
                
                display_url = result.url
                if len(display_url) > 200:
                    display_url = display_url[:197] + "..."
                
                embed_channel.add_field(
                    name="Direct Link",
                    value=f"```{display_url}```",
                    inline=False
                )
                
                embed_channel.add_field(
                    name="Bypassed By",
                    value=f"{interaction.user.mention} ({interaction.user.id})",
                    inline=True
                )
                
                embed_channel.add_field(
                    name="Source",
                    value=f"Via bypass command in {interaction.guild.name}",
                    inline=True
                )
                
                view_channel = discord.ui.View(timeout=None)
                view_channel.add_item(discord.ui.Button(
                    label="Open Direct Link",
                    url=result.url,
                    style=discord.ButtonStyle.link
                ))
                
                await bot.auto_bypass_channel.send(embed=embed_channel, view=view_channel)
                
            except Exception as e:
                logger.error(f"Failed to send to auto-bypass channel: {e}")
        
        await interaction.followup.send(embed=embed, view=view)
        
    else:
        if "unavailable" in result.message.lower() or "terminated" in result.message.lower():
            embed = discord.Embed(
                title="Game Unavailable",
                description=result.message,
                color=0xFF0000,
                timestamp=datetime.now(UTC)
            )
            
            embed.add_field(
                name="Original Link",
                value=f"```{url[:150] if len(url) > 150 else url}```",
                inline=False
            )
            
            if game_id:
                embed.add_field(
                    name="Game ID",
                    value=f"{game_id}",
                    inline=True
                )
            
            embed.add_field(
                name="Status Details",
                value="• Game is banned or terminated\n• Game link is expired\n• Game is private or restricted",
                inline=False
            )
            
            embed.set_footer(text="Try a different game link")
        else:
            embed = discord.Embed(
                title="Bypass Failed",
                description=result.message,
                color=0xFF0000,
                timestamp=datetime.now(UTC)
            )
            
            embed.add_field(
                name="URL",
                value=f"```{url[:150] if len(url) > 150 else url}```",
                inline=False
            )
            
            embed.add_field(
                name="Method Tried",
                value=f"{result.method.value.replace('_', ' ').title()}",
                inline=True
            )
            
            embed.add_field(
                name="Troubleshooting",
                value="• Try a different Linkvertise URL\n• Check if the URL is valid\n• The link might be expired or private",
                inline=False
            )
            
            embed.set_footer(text="Try again with a different link")
        
        await interaction.followup.send(embed=embed)

@bot.tree.command(name="pbypass", description="Private bypass - Bypass links without sending to channel")
@app_commands.describe(url="The Linkvertise URL to bypass privately")
@app_commands.checks.cooldown(1, 3.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def pbypass_command(interaction: discord.Interaction, url: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    
    if not interaction.guild:
        embed = discord.Embed(
            title="Command Not Available",
            description="This command can only be used in servers, not in direct messages.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    if not bot.is_valid_linkvertise_url(url):
        embed = discord.Embed(
            title="Invalid URL",
            description="The provided URL is not a valid Linkvertise link.",
            color=0xFF0000
        )
        embed.add_field(
            name="Supported formats",
            value="All Linkvertise domains and formats are supported",
            inline=False
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    allowed, reset_time, error_msg = bot.check_rate_limit(interaction.user.id)
    if not allowed:
        embed = discord.Embed(
            title="Rate Limited",
            description=error_msg or f"Please wait {reset_time} seconds.",
            color=0xFFA500
        )
        embed.set_footer(text=f"Daily limit: {bot.DAILY_LIMIT} requests")
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    result = await bot.bypass_linkvertise(
        url=url,
        user_id=interaction.user.id,
        guild_id=interaction.guild.id if interaction.guild else None
    )
    
    await bot.log_to_database(result, interaction.user, interaction.guild)
    
    if result.success:
        embed = discord.Embed(
            title="Private Bypass Successful",
            color=0x00FF00,
            timestamp=datetime.now(UTC)
        )
        
        embed.add_field(
            name="Original Link",
            value=f"```{url[:100] + '...' if len(url) > 100 else url}```",
            inline=False
        )
        
        embed.add_field(
            name="Direct Link",
            value=f"```{result.url[:100] + '...' if len(result.url) > 100 else result.url}```",
            inline=False
        )
        
        embed.add_field(
            name="Method",
            value=f"{result.method.value.replace('_', ' ').title()}",
            inline=True
        )
        
        embed.add_field(
            name="Time",
            value=f"{result.execution_time:.2f}s",
            inline=True
        )
        
        embed.set_footer(
            text=f"Private request by {interaction.user.name}",
            icon_url=interaction.user.display_avatar.url
        )
        
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(
            label="Open Direct Link",
            url=result.url,
            style=discord.ButtonStyle.link
        ))
        
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
    else:
        embed = discord.Embed(
            title="Private Bypass Failed",
            description=result.message,
            color=0xFF0000,
            timestamp=datetime.now(UTC)
        )
        
        embed.add_field(
            name="URL",
            value=f"```{url[:150] if len(url) > 150 else url}```",
            inline=False
        )
        
        embed.add_field(
            name="Method Tried",
            value=f"{result.method.value.replace('_', ' ').title()}",
            inline=True
        )
        
        embed.add_field(
            name="Troubleshooting",
            value="• Try a different Linkvertise URL\n• Check if the URL is valid\n• The link might be expired or private",
            inline=False
        )
        
        embed.set_footer(text="Try again with a different link")
        
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="autobypass", description="Auto-bypass multiple links and send to results channel (Authorized users only)")
@app_commands.describe(
    links="Links separated by commas (e.g., link1, link2, link3, ...)"
)
async def autobypass_command(interaction: discord.Interaction, links: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    
    if interaction.user.id not in bot.auto_bypass_allowed_users:
        embed = discord.Embed(
            title="Access Denied",
            description="You are not authorized to use auto-bypass. Only authorized users can use this command.",
            color=0xFF0000
        )
        
        if interaction.guild and interaction.guild.id == AUTO_BYPASS_SERVER_ID:
            embed.add_field(
                name="Request Access",
                value=f"Ask {YOUR_USER_ID} to authorize you using autobypass_users add command",
                inline=False
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    extracted_urls = set(bot.extract_linkvertise_urls(links))
    
    for link in links.split(','):
        link = link.strip()
        if link and bot.is_valid_linkvertise_url(link):
            extracted_urls.add(link)
    
    url_list = list(extracted_urls)
    
    if not url_list:
        embed = discord.Embed(
            title="No Valid URLs",
            description="No valid Linkvertise URLs found in your input.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    MAX_AUTO_BYPASS = 100
    if len(url_list) > MAX_AUTO_BYPASS:
        url_list = url_list[:MAX_AUTO_BYPASS]
        warning = f"Limited to first {MAX_AUTO_BYPASS} URLs"
    else:
        warning = None
    
    successful_urls = await bot.auto_bypass_to_channel(
        urls=url_list,
        user=interaction.user,
        source_guild=interaction.guild
    )
    
    if successful_urls:
        embed = discord.Embed(
            title="Auto-Bypass Complete",
            description=f"Successfully bypassed {len(successful_urls)}/{len(url_list)} links.",
            color=0x00FF00,
            timestamp=datetime.now(UTC)
        )
        
        embed.add_field(
            name="Results Sent To",
            value=f"Channel: {AUTO_BYPASS_CHANNEL_ID}",
            inline=True
        )
        
        embed.add_field(
            name="Processing Time",
            value=f"Approximately {len(url_list) * 2}s",
            inline=True
        )
        
        if successful_urls:
            examples = []
            for i, url in enumerate(successful_urls[:5], 1):
                filename = url.split('/')[-1][:30]
                examples.append(f"{i}. {filename}...")
            
            if len(successful_urls) > 5:
                examples.append(f"... and {len(successful_urls) - 5} more")
            
            embed.add_field(
                name="Bypassed Files",
                value="\n".join(examples),
                inline=False
            )
        
        if warning:
            embed.set_footer(text=warning)
        
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(
            label="View Results",
            url=f"https://discord.com/channels/{AUTO_BYPASS_SERVER_ID}/{AUTO_BYPASS_CHANNEL_ID}",
            style=discord.ButtonStyle.link
        ))
        
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    else:
        embed = discord.Embed(
            title="Auto-Bypass Failed",
            description="No links were successfully bypassed.",
            color=0xFF0000
        )
        embed.add_field(
            name="Possible Issues",
            value="• All URLs might be invalid or expired\n• Rate limiting from Linkvertise\n• Temporary service issues",
            inline=False
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="batch", description="Bypass multiple links at once (Authorized users only)")
@app_commands.describe(
    links="Links separated by commas (e.g., link1, link2, link3)"
)
@app_commands.checks.cooldown(1, 30.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def batch_command(interaction: discord.Interaction, links: str):
    await interaction.response.defer(thinking=True)
    
    if not interaction.guild:
        embed = discord.Embed(
            title="Command Not Available",
            description="This command can only be used in servers, not in direct messages.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed)
        return
    
    cursor = bot.db_conn.cursor()
    cursor.execute('''
        SELECT can_use_batch FROM authorized_users WHERE user_id = ?
    ''', (interaction.user.id,))
    
    user_data = cursor.fetchone()
    
    if interaction.user.id != YOUR_USER_ID and (not user_data or not user_data[0]):
        embed = discord.Embed(
            title="Access Denied",
            description="You need authorization to use the batch command.",
            color=0xFFA500
        )
        
        embed.add_field(
            name="How to Get Access",
            value=f"Ask {YOUR_USER_ID} to grant you batch command access. Use request_access command for more information.",
            inline=False
        )
        
        await interaction.followup.send(embed=embed)
        return
    
    extracted_urls = set(bot.extract_linkvertise_urls(links))
    
    for link in links.split(','):
        link = link.strip()
        if link and bot.is_valid_linkvertise_url(link):
            extracted_urls.add(link)
    
    url_list = list(extracted_urls)
    
    if not url_list:
        embed = discord.Embed(
            title="No Valid URLs",
            description="No valid Linkvertise URLs found in your input.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed)
        return
    
    if len(url_list) > 15:
        url_list = url_list[:15]
        warning = "Limited to first 15 URLs"
    else:
        warning = None
    
    results = []
    successful = 0
    successful_urls = []
    failed_urls = []
    
    BATCH_SIZE = 3
    for i in range(0, len(url_list), BATCH_SIZE):
        batch = url_list[i:i + BATCH_SIZE]
        batch_tasks = []
        
        for url in batch:
            task = asyncio.create_task(bot.bypass_linkvertise(url, interaction.user.id, interaction.guild.id))
            batch_tasks.append((url, task))
        
        for url, task in batch_tasks:
            try:
                result = await asyncio.wait_for(task, timeout=15)
                results.append((url, result.success, result.message, result.url))
                if result.success:
                    successful += 1
                    successful_urls.append(result.url)
                else:
                    failed_urls.append(url)
            except asyncio.TimeoutError:
                results.append((url, False, "Timeout", None))
                failed_urls.append(url)
                logger.warning(f"Timeout while processing URL: {url}")
        
        if i + BATCH_SIZE < len(url_list):
            await asyncio.sleep(0.5)
    
    color = 0x00FF00 if successful > 0 else 0xFFA500
    
    embed = discord.Embed(
        title="Batch Bypass Results",
        color=color,
        timestamp=datetime.now(UTC)
    )
    
    embed.add_field(
        name="Summary",
        value=f"{successful}/{len(results)} successful bypasses",
        inline=False
    )
    
    if successful_urls:
        display_list = []
        for i, (original_url, success, _, direct_url) in enumerate(results):
            if success and i < 5:
                filename = original_url.split('/')[-1][:40]
                display_list.append(f"• {filename}...")
        
        display = "\n".join(display_list)
        
        if len(successful_urls) > 5:
            display += f"\n• ... and {len(successful_urls) - 5} more"
        
        embed.add_field(
            name="Successful Bypasses",
            value=display,
            inline=False
        )
    
    if failed_urls:
        display_list = []
        for url in failed_urls[:3]:
            filename = url.split('/')[-1][:40]
            display_list.append(f"• {filename}...")
        
        display = "\n".join(display_list)
        
        if len(failed_urls) > 3:
            display += f"\n• ... and {len(failed_urls) - 3} more"
        
        embed.add_field(
            name="Failed URLs",
            value=display,
            inline=False
        )
    
    if warning:
        embed.set_footer(text=warning)
    
    if successful_urls:
        view = discord.ui.View(timeout=180)
        for i, direct_url in enumerate(successful_urls[:5]):
            button_label = f"Link {i+1}" if i < len(successful_urls) else f"URL {i+1}"
            
            button = discord.ui.Button(
                label=button_label,
                url=direct_url,
                style=discord.ButtonStyle.link
            )
            view.add_item(button)
        
        await interaction.followup.send(embed=embed, view=view)
    else:
        await interaction.followup.send(embed=embed)

@bot.tree.command(name="stats", description="View your bypass statistics")
async def stats_command(interaction: discord.Interaction):
    user_id = interaction.user.id
    
    if user_id in bot.user_stats:
        stats = bot.user_stats[user_id]
        success_rate = (stats['successful_bypasses'] / max(stats['total_requests'], 1)) * 100
        
        embed = discord.Embed(
            title="Your Bypass Statistics",
            color=0x7289DA,
            timestamp=datetime.now(UTC)
        )
        
        embed.add_field(
            name="Total Requests",
            value=f"{stats['total_requests']}",
            inline=True
        )
        
        embed.add_field(
            name="Successful",
            value=f"{stats['successful_bypasses']}",
            inline=True
        )
        
        embed.add_field(
            name="Failed",
            value=f"{stats['failed_bypasses']}",
            inline=True
        )
        
        embed.add_field(
            name="Success Rate",
            value=f"{success_rate:.1f}%",
            inline=True
        )
        
        daily_used = stats.get('daily_requests', 0)
        daily_remaining = bot.DAILY_LIMIT - daily_used
        reset_time = stats.get('reset_time', time.time())
        
        embed.add_field(
            name="Daily Usage",
            value=f"{daily_used}/{bot.DAILY_LIMIT}",
            inline=True
        )
        
        embed.add_field(
            name="Daily Remaining",
            value=f"{max(0, daily_remaining)}",
            inline=True
        )
        
        now = time.time()
        user_requests = bot.request_count.get(user_id, [])
        recent_requests = [req for req in user_requests if now - req < bot.RATE_LIMIT_WINDOW]
        remaining = bot.MAX_REQUESTS_PER_WINDOW - len(recent_requests)
        
        embed.add_field(
            name="Current Minute",
            value=f"{len(recent_requests)}/{bot.MAX_REQUESTS_PER_WINDOW}",
            inline=True
        )
        
        embed.add_field(
            name="Remaining Requests",
            value=f"{remaining}",
            inline=True
        )
        
        last_request = stats.get('last_request', 0)
        if last_request:
            embed.add_field(
                name="Last Request",
                value=f"{int(last_request)}",
                inline=True
            )
        
        embed.set_footer(text=f"Statistics for {interaction.user.name}")
        
    else:
        embed = discord.Embed(
            title="Your Statistics",
            description="You haven't made any bypass requests yet.",
            color=0x7289DA
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="cache", description="View cache statistics")
async def cache_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Cache Statistics",
        color=0x7289DA,
        timestamp=datetime.now(UTC)
    )
    
    embed.add_field(
        name="Total Cached URLs",
        value=f"{len(bot.cache):,}",
        inline=True
    )
    
    now = time.time()
    recent = 0
    old = 0
    
    for _, timestamp, _ in bot.cache.values():
        age = now - timestamp
        if age < 3600:
            recent += 1
        else:
            old += 1
    
    embed.add_field(
        name="Less than 1 hour",
        value=f"{recent}",
        inline=True
    )
    
    embed.add_field(
        name="More than 1 hour",
        value=f"{old}",
        inline=True
    )
    
    embed.add_field(
        name="Database Cache",
        value=f"{bot.MAX_CACHE_SIZE:,} maximum entries",
        inline=True
    )
    
    embed.add_field(
        name="Cache TTL",
        value=f"{bot.CACHE_TTL//3600} hours",
        inline=True
    )
    
    method_stats_text = "\n".join([f"• {method}: {count:,}" for method, count in bot.method_stats.items() if count > 0])
    embed.add_field(
        name="Bypass Methods Used",
        value=method_stats_text or "No methods used yet",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="autobypass_users", description="Admin - Manage auto-bypass authorized users")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    action="Add or remove user",
    user="User to manage",
    permission_level="Permission level (auto_bypass, batch, both)"
)
async def autobypass_users_command(interaction: discord.Interaction, action: str, user: discord.User, 
                                  permission_level: str = "auto_bypass"):
    if interaction.user.id != YOUR_USER_ID:
        embed = discord.Embed(
            title="Permission Denied",
            description="Only the bot owner can manage authorized users.",
            color=0xFF0000
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    
    cursor = bot.db_conn.cursor()
    
    if action.lower() == "add":
        can_auto_bypass = 1 if permission_level in ["auto_bypass", "both"] else 0
        can_use_batch = 1 if permission_level in ["batch", "both"] else 0
        
        cursor.execute('''
            INSERT OR REPLACE INTO authorized_users 
            (user_id, added_by, can_auto_bypass, can_use_batch)
            VALUES (?, ?, ?, ?)
        ''', (user.id, interaction.user.id, can_auto_bypass, can_use_batch))
        
        bot.db_conn.commit()
        
        if can_auto_bypass:
            bot.auto_bypass_allowed_users.add(user.id)
        elif user.id in bot.auto_bypass_allowed_users:
            bot.auto_bypass_allowed_users.remove(user.id)
        
        embed = discord.Embed(
            title="User Authorized",
            description=f"Added {user.mention} to authorized users.",
            color=0x00FF00
        )
        
        permissions = []
        if can_auto_bypass:
            permissions.append("Auto-Bypass")
        if can_use_batch:
            permissions.append("Batch Command")
        
        if permissions:
            embed.add_field(
                name="Permissions Granted",
                value="\n".join([f"• {p}" for p in permissions]),
                inline=False
            )
    
    elif action.lower() == "remove":
        cursor.execute('DELETE FROM authorized_users WHERE user_id = ?', (user.id,))
        bot.db_conn.commit()
        
        if user.id in bot.auto_bypass_allowed_users:
            bot.auto_bypass_allowed_users.remove(user.id)
        
        embed = discord.Embed(
            title="User Removed",
            description=f"Removed {user.mention} from authorized users.",
            color=0x00FF00
        )
    
    elif action.lower() == "list":
        cursor.execute('''
            SELECT user_id, can_auto_bypass, can_use_batch 
            FROM authorized_users 
            ORDER BY added_at DESC
        ''')
        rows = cursor.fetchall()
        
        if rows:
            users_list = []
            for row in rows:
                user_obj = bot.get_user(row[0])
                username = user_obj.mention if user_obj else f"User {row[0]}"
                
                permissions = []
                if row[1]:
                    permissions.append("Auto-Bypass")
                if row[2]:
                    permissions.append("Batch")
                
                perm_text = ", ".join(permissions) if permissions else "None"
                users_list.append(f"• {username} - {perm_text}")
            
            embed = discord.Embed(
                title="Authorized Users List",
                description="\n".join(users_list),
                color=0x7289DA
            )
            embed.set_footer(text=f"Total: {len(rows)} users")
        else:
            embed = discord.Embed(
                title="Authorized Users List",
                description="No authorized users found.",
                color=0xFFA500
            )
    
    else:
        embed = discord.Embed(
            title="Invalid Action",
            description="Use add, remove, or list.",
            color=0xFF0000
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="request_access", description="Request access to premium features")
async def request_access_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Request Access to Premium Features",
        color=0x7289DA
    )
    
    embed.add_field(
        name="Available Features",
        value="• Auto-Bypass: Process unlimited links at once\n• Batch Command: Process multiple links with one command",
        inline=False
    )
    
    embed.add_field(
        name="How to Get Access",
        value=f"Ask {YOUR_USER_ID} to authorize you in the server. They can use: autobypass_users add command",
        inline=False
    )
    
    embed.add_field(
        name="Current Status",
        value="You have access" if interaction.user.id in bot.auto_bypass_allowed_users else "You don't have access",
        inline=False
    )
    
    if interaction.guild and interaction.guild.id == AUTO_BYPASS_SERVER_ID:
        if interaction.user.id not in bot.auto_bypass_allowed_users:
            embed.add_field(
                name="Quick Request",
                value=f"Contact {YOUR_USER_ID} in this server to request access",
                inline=False
            )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.event
async def on_message(message):
    if message.author == bot.user or message.author.bot:
        return
    
    if message.guild:
        urls = bot.extract_linkvertise_urls(message.content)
        
        if urls:
            if message.author.id in bot.auto_bypass_allowed_users:
                view = discord.ui.View(timeout=60)
                
                button_auto = discord.ui.Button(
                    label="Auto-Bypass All Links",
                    style=discord.ButtonStyle.green,
                    custom_id=f"auto_bypass_all_{message.id}"
                )
                
                button_single = discord.ui.Button(
                    label="Bypass Links Individually",
                    style=discord.ButtonStyle.blurple,
                    custom_id=f"bypass_single_{message.id}"
                )
                
                async def auto_bypass_callback(interaction: discord.Interaction):
                    if interaction.user.id != message.author.id:
                        await interaction.response.send_message(
                            "Only the original author can use this.", 
                            ephemeral=True
                        )
                        return
                    
                    await interaction.response.defer(thinking=True, ephemeral=True)
                    
                    successful_urls = await bot.auto_bypass_to_channel(
                        urls=urls,
                        user=message.author,
                        source_guild=message.guild
                    )
                    
                    if successful_urls:
                        embed = discord.Embed(
                            title="Auto-Bypass Complete",
                            description=f"Sent {len(successful_urls)} links to channel {AUTO_BYPASS_CHANNEL_ID}",
                            color=0x00FF00
                        )
                        await interaction.followup.send(embed=embed, ephemeral=True)
                        
                        try:
                            await message.delete()
                        except:
                            pass
                    else:
                        await interaction.followup.send(
                            "Auto-bypass failed. Try individual bypass.", 
                            ephemeral=True
                        )
                
                async def single_bypass_callback(interaction: discord.Interaction):
                    urls_single = urls[:3]
                    view_single = discord.ui.View(timeout=300)
                    
                    for i, url in enumerate(urls_single):
                        if len(url) > 50:
                            label = f"Bypass Link {i+1}"
                        else:
                            label = f"Bypass: {url[:30]}..."
                        
                        button = discord.ui.Button(
                            label=label,
                            style=discord.ButtonStyle.primary,
                            custom_id=f"auto_bypass_{i}_{message.id}"
                        )
                        
                        async def button_callback(interaction: discord.Interaction, target_url=url):
                            await bypass_command(interaction, target_url)
                        
                        button.callback = button_callback
                        view_single.add_item(button)
                    
                    await interaction.response.send_message(
                        f"Detected {len(urls)} Linkvertise link(s). Click below to bypass:",
                        view=view_single,
                        ephemeral=True
                    )
                
                button_auto.callback = auto_bypass_callback
                button_single.callback = single_bypass_callback
                
                view.add_item(button_auto)
                view.add_item(button_single)
                
                response_text = f"{message.author.mention}, detected {len(urls)} Linkvertise link(s). Choose an option:"
            
            else:
                urls = urls[:3]
                view = discord.ui.View(timeout=300)
                
                for i, url in enumerate(urls):
                    if len(url) > 50:
                        label = f"Bypass Link {i+1}"
                    else:
                        label = f"Bypass: {url[:30]}..."
                    
                    button = discord.ui.Button(
                        label=label,
                        style=discord.ButtonStyle.primary,
                        custom_id=f"auto_bypass_{i}"
                    )
                    
                    async def button_callback(interaction: discord.Interaction, target_url=url):
                        await bypass_command(interaction, target_url)
                    
                    button.callback = button_callback
                    view.add_item(button)
                
                response_text = f"Detected {len(urls)} Linkvertise link(s). Click below to bypass:"
            
            if message.channel.permissions_for(message.guild.me).send_messages:
                response = await message.reply(
                    response_text,
                    view=view,
                    mention_author=False
                )
                
                async def cleanup():
                    await asyncio.sleep(60 if message.author.id in bot.auto_bypass_allowed_users else 300)
                    try:
                        await response.delete()
                    except:
                        pass
                
                asyncio.create_task(cleanup())
    
    await bot.process_commands(message)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    token = os.getenv('D_TOKEN') or os.getenv('DISCORD_BOT_TOKEN')
    
    if not token:
        logger.error("No bot token found. Please set D_TOKEN or DISCORD_BOT_TOKEN in .env file")
        print("Error: No bot token found.")
        print("Create a .env file with: D_TOKEN=your_token_here")
        exit(1)
    
    try:
        bot.run(token)
    except KeyboardInterrupt:
        logger.info("Bot shutting down...")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

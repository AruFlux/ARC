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
            "linkvertise.com",
            "linkvertise.net",
            "linkvertise.io",
            "linkvertise.co",
            "up-to-down.net",
            "link-to.net",
            "linkv.to",
            "lv.to",
            "linkvertise.download",
            "linkvertise.pw",
            "linkvertise.xyz",
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
                cache_key = row['url_hash']
                bypassed_url = row['bypassed_url']
                method = BypassMethod(row['method'])

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
                self.auto_bypass_allowed_users.add(row['user_id'])

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
                    original_url TEXT NOT NULL,
                    bypassed_url TEXT,
                    method TEXT NOT NULL,
                    success BOOLEAN NOT NULL,
                    execution_time REAL,
                    error_message TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_cache (
                    url_hash TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    bypassed_url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    access_count INTEGER DEFAULT 0,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS authorized_users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    can_auto_bypass BOOLEAN DEFAULT 0,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    added_by BIGINT
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    failed_requests INTEGER DEFAULT 0,
                    last_request TIMESTAMP,
                    daily_request_count INTEGER DEFAULT 0,
                    last_daily_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocked_urls (
                    url_hash TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    reason TEXT,
                    blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    blocked_by BIGINT
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_bypass_logs_user_id ON bypass_logs(user_id)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_bypass_logs_timestamp ON bypass_logs(timestamp)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_url_cache_expires ON url_cache(expires_at)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_stats_last_reset ON user_stats(last_daily_reset)
            ''')

            self.db_conn.commit()
            logger.info("Database initialization completed successfully")

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    async def cleanup_cache(self):
        current_time = time.time()
        expired_keys = []
        
        for key, (_, timestamp, _) in list(self.cache.items()):
            if current_time - timestamp > self.CACHE_TTL:
                expired_keys.append(key)
        
        for key in expired_keys:
            if key in self.cache:
                del self.cache[key]
                if key in self.cache_order:
                    self.cache_order.remove(key)
        
        while len(self.cache_order) > self.MAX_CACHE_SIZE:
            oldest_key = self.cache_order.pop(0)
            if oldest_key in self.cache:
                del self.cache[oldest_key]

    async def periodic_cleanup(self):
        while True:
            await asyncio.sleep(3600)
            try:
                await self.cleanup_cache()
                
                if self.db_conn:
                    cursor = self.db_conn.cursor()
                    cursor.execute('DELETE FROM url_cache WHERE expires_at < CURRENT_TIMESTAMP')
                    self.db_conn.commit()
                    
                logger.debug("Periodic cleanup completed")
                
            except Exception as e:
                logger.error(f"Cleanup failed: {e}")

    async def periodic_stats_log(self):
        while True:
            await asyncio.sleep(86400)
            try:
                logger.info(f"Method stats: {self.method_stats}")
                logger.info(f"Performance stats: {self.performance_stats}")
                logger.info(f"Cache size: {len(self.cache)}")
                
            except Exception as e:
                logger.error(f"Stats logging failed: {e}")

    async def close(self):
        if self.cleanup_task:
            self.cleanup_task.cancel()
        if self.stats_task:
            self.stats_task.cancel()
        if self.health_check_task:
            self.health_check_task.cancel()
        
        if self.session:
            await self.session.close()
        
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        
        if self.db_conn:
            self.db_conn.close()
        
        await super().close()

bot = EnhancedLinkvertiseBypassBot()

@bot.tree.command(name="bypass", description="Bypass a Linkvertise link")
async def bypass_command(interaction: discord.Interaction, url: str):
    try:
        await interaction.response.defer(thinking=True)
        
        bypass_result = await bot.bypass_linkvertise(url, interaction.user.id, interaction.guild.id if interaction.guild else None)
        
        if bypass_result.success:
            embed = discord.Embed(
                title="Bypass successful",
                description=f"Original URL: {bypass_result.original_url}",
                color=discord.Color.green()
            )
            embed.add_field(name="Bypassed URL", value=bypass_result.url[:4000], inline=False)
            embed.add_field(name="Method", value=bypass_result.method.value, inline=True)
            embed.add_field(name="Time", value=f"{bypass_result.execution_time:.2f}s", inline=True)
            await interaction.followup.send(embed=embed)
        else:
            embed = discord.Embed(
                title="Bypass failed",
                description=bypass_result.message,
                color=discord.Color.red()
            )
            embed.add_field(name="Original URL", value=bypass_result.original_url, inline=False)
            await interaction.followup.send(embed=embed)
            
    except Exception as e:
        logger.error(f"Bypass command failed: {e}")
        await interaction.followup.send(f"An error occurred: {str(e)[:100]}")

@bot.event
async def on_ready():
    logger.info(f"Bot is ready as {bot.user}")
    
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} commands")
        
        log_server = bot.get_guild(LOG_SERVER_ID)
        if log_server:
            bot.log_channel = log_server.get_channel(LOG_CHANNEL_ID)
            bot.auto_bypass_channel = log_server.get_channel(AUTO_BYPASS_CHANNEL_ID)
            bot.ping_role = log_server.get_role(PING_ROLE_ID)
            
    except Exception as e:
        logger.error(f"Ready event failed: {e}")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    
    if bot.auto_bypass_channel and message.channel.id == AUTO_BYPASS_CHANNEL_ID:
        if message.author.id not in bot.auto_bypass_allowed_users:
            await message.delete()
            try:
                await message.author.send("You are not authorized to use the auto-bypass channel.")
            except:
                pass
            return
        
        url_pattern = re.compile(r'https?://\S+')
        urls = url_pattern.findall(message.content)
        
        if urls:
            for url in urls:
                bypass_result = await bot.bypass_linkvertise(url, message.author.id, message.guild.id if message.guild else None)
                
                if bypass_result.success:
                    await message.channel.send(f"Bypassed URL: {bypass_result.url}")
                else:
                    await message.channel.send(f"Failed to bypass: {bypass_result.message}")
    
    await bot.process_commands(message)

async def bypass_linkvertise(self, url: str, user_id: int, guild_id: Optional[int] = None) -> BypassResult:
    start_time = time.time()
    
    try:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        if url_hash in self.cache:
            bypassed_url, timestamp, method = self.cache[url_hash]
            if time.time() - timestamp <= self.CACHE_TTL:
                self.cache_order.remove(url_hash)
                self.cache_order.append(url_hash)
                
                result = BypassResult(
                    success=True,
                    url=bypassed_url,
                    message="Retrieved from cache",
                    method=method,
                    execution_time=time.time() - start_time,
                    original_url=url,
                    user_id=user_id,
                    guild_id=guild_id
                )
                self.method_stats[BypassMethod.CACHED.value] += 1
                return result
        
        if not await self.check_rate_limit(user_id):
            result = BypassResult(
                success=False,
                url=None,
                message="Rate limit exceeded. Please wait.",
                method=BypassMethod.API_BYPASS,
                execution_time=time.time() - start_time,
                original_url=url,
                user_id=user_id,
                guild_id=guild_id
            )
            return result
        
        bypass_result = await self.try_bypass_methods(url)
        
        if bypass_result.success:
            self.cache[url_hash] = (bypass_result.url, time.time(), bypass_result.method)
            self.cache_order.append(url_hash)
            
            if len(self.cache_order) > self.MAX_CACHE_SIZE:
                oldest_key = self.cache_order.pop(0)
                if oldest_key in self.cache:
                    del self.cache[oldest_key]
            
            await self.save_to_cache_database(url_hash, url, bypass_result.url, bypass_result.method)
        
        bypass_result.execution_time = time.time() - start_time
        bypass_result.user_id = user_id
        bypass_result.guild_id = guild_id
        
        self.performance_stats["total_requests"] += 1
        if bypass_result.success:
            self.performance_stats["successful_requests"] += 1
        self.performance_stats["success_rate"] = (
            self.performance_stats["successful_requests"] / max(self.performance_stats["total_requests"], 1)
        )
        
        await self.log_bypass_attempt(bypass_result)
        await self.update_user_stats(user_id, bypass_result.success)
        
        return bypass_result
        
    except Exception as e:
        logger.error(f"Bypass process failed: {e}")
        return BypassResult(
            success=False,
            url=None,
            message=f"Internal error: {str(e)[:100]}",
            method=BypassMethod.API_BYPASS,
            execution_time=time.time() - start_time,
            original_url=url,
            user_id=user_id,
            guild_id=guild_id
        )

async def try_bypass_methods(self, url: str) -> BypassResult:
    methods = [
        (self.bypass_with_dynamic_decode, BypassMethod.DYNAMIC_DECODE),
        (self.bypass_with_api, BypassMethod.API_BYPASS),
        (self.bypass_with_fallback_services, BypassMethod.FALLBACK_SERVICE),
        (self.extract_from_page, BypassMethod.PAGE_EXTRACTION),
    ]
    
    for method_func, method_enum in methods:
        try:
            result = await method_func(url)
            if result.success:
                self.method_stats[method_enum.value] += 1
                return BypassResult(
                    success=True,
                    url=result.url,
                    message="Bypass successful",
                    method=method_enum,
                    execution_time=0.0,
                    original_url=url
                )
        except Exception as e:
            logger.debug(f"Method {method_enum.value} failed: {e}")
            continue
    
    return BypassResult(
        success=False,
        url=None,
        message="All bypass methods failed",
        method=BypassMethod.API_BYPASS,
        execution_time=0.0,
        original_url=url
    )

async def bypass_with_dynamic_decode(self, url: str) -> BypassResult:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        
        async with self.session.get(url, headers=headers, allow_redirects=True, timeout=15) as response:
            if response.status != 200:
                return BypassResult(
                    success=False,
                    url=None,
                    message=f"HTTP {response.status}",
                    method=BypassMethod.DYNAMIC_DECODE,
                    execution_time=0.0,
                    original_url=url
                )
            
            html = await response.text()
            
            patterns = [
                r'window\.location\.href\s*=\s*["\']([^"\']+)["\']',
                r'window\.location\.replace\s*\(\s*["\']([^"\']+)["\']\s*\)',
                r'<meta\s+http-equiv=["\']refresh["\'][^>]*content=["\'][^;]+;\s*url=([^"\']+)["\']',
                r'<a\s+href=["\']([^"\']+)["\'][^>]*id=["\']bypass["\']',
                r'data-redirect=["\']([^"\']+)["\']',
                r'data-url=["\']([^"\']+)["\']',
                r'https?://[^\s"\'<>]+',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    if match and not any(domain in match for domain in self.linkvertise_domains):
                        if match.startswith('//'):
                            match = 'https:' + match
                        elif match.startswith('/'):
                            parsed = urlparse(url)
                            match = f"{parsed.scheme}://{parsed.netloc}{match}"
                        
                        return BypassResult(
                            success=True,
                            url=match,
                            message="Dynamic decode successful",
                            method=BypassMethod.DYNAMIC_DECODE,
                            execution_time=0.0,
                            original_url=url
                        )
            
            return BypassResult(
                success=False,
                url=None,
                message="No redirect found in page",
                method=BypassMethod.DYNAMIC_DECODE,
                execution_time=0.0,
                original_url=url
            )
            
    except Exception as e:
        logger.debug(f"Dynamic decode failed: {e}")
        return BypassResult(
            success=False,
            url=None,
            message=f"Dynamic decode error: {str(e)[:50]}",
            method=BypassMethod.DYNAMIC_DECODE,
            execution_time=0.0,
            original_url=url
        )

async def bypass_with_api(self, url: str) -> BypassResult:
    try:
        api_url = "https://api.bypass.vip/bypass2"
        payload = {"url": url}
        
        async with self.session.post(api_url, json=payload, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("success") and data.get("destination"):
                    return BypassResult(
                        success=True,
                        url=data["destination"],
                        message="API bypass successful",
                        method=BypassMethod.API_BYPASS,
                        execution_time=0.0,
                        original_url=url
                    )
            
            return BypassResult(
                success=False,
                url=None,
                message=f"API returned {response.status}",
                method=BypassMethod.API_BYPASS,
                execution_time=0.0,
                original_url=url
            )
            
    except Exception as e:
        logger.debug(f"API bypass failed: {e}")
        return BypassResult(
            success=False,
            url=None,
            message=f"API error: {str(e)[:50]}",
            method=BypassMethod.API_BYPASS,
            execution_time=0.0,
            original_url=url
        )

async def bypass_with_fallback_services(self, url: str) -> BypassResult:
    for service in self.fallback_services:
        try:
            if service["method"] == "POST":
                async with self.session.post(service["url"], json={"url": url}, timeout=service["timeout"]) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("destination") or data.get("url"):
                            destination = data.get("destination") or data.get("url")
                            return BypassResult(
                                success=True,
                                url=destination,
                                message=f"Service {service['url']} successful",
                                method=BypassMethod.FALLBACK_SERVICE,
                                execution_time=0.0,
                                original_url=url
                            )
            else:
                async with self.session.get(f"{service['url']}?url={url}", timeout=service["timeout"]) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("destination") or data.get("url"):
                            destination = data.get("destination") or data.get("url")
                            return BypassResult(
                                success=True,
                                url=destination,
                                message=f"Service {service['url']} successful",
                                method=BypassMethod.FALLBACK_SERVICE,
                                execution_time=0.0,
                                original_url=url
                            )
        except Exception as e:
            logger.debug(f"Service {service['url']} failed: {e}")
            continue
    
    return BypassResult(
        success=False,
        url=None,
        message="All fallback services failed",
        method=BypassMethod.FALLBACK_SERVICE,
        execution_time=0.0,
        original_url=url
    )

async def extract_from_page(self, url: str) -> BypassResult:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        async with self.session.get(url, headers=headers, allow_redirects=True, timeout=15) as response:
            if response.status != 200:
                return BypassResult(
                    success=False,
                    url=None,
                    message=f"HTTP {response.status}",
                    method=BypassMethod.PAGE_EXTRACTION,
                    execution_time=0.0,
                    original_url=url
                )
            
            html = await response.text()
            
            url_patterns = [
                r'(https?://[^\s"\'<>]+)',
                r'window\.open\(["\']([^"\']+)["\']',
                r'<iframe[^>]*src=["\']([^"\']+)["\']',
                r'<a[^>]*href=["\']([^"\']+)["\'][^>]*download',
                r'<a[^>]*href=["\']([^"\']+)["\'][^>]*target=["\']_blank["\']',
            ]
            
            all_urls = []
            for pattern in url_patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                all_urls.extend(matches)
            
            for found_url in all_urls:
                if found_url and not any(domain in found_url for domain in self.linkvertise_domains):
                    if found_url.startswith('//'):
                        found_url = 'https:' + found_url
                    elif found_url.startswith('/'):
                        parsed = urlparse(url)
                        found_url = f"{parsed.scheme}://{parsed.netloc}{found_url}"
                    
                    return BypassResult(
                        success=True,
                        url=found_url,
                        message="Page extraction successful",
                        method=BypassMethod.PAGE_EXTRACTION,
                        execution_time=0.0,
                        original_url=url
                    )
            
            return BypassResult(
                success=False,
                url=None,
                message="No valid URLs found in page",
                method=BypassMethod.PAGE_EXTRACTION,
                execution_time=0.0,
                original_url=url
            )
            
    except Exception as e:
        logger.debug(f"Page extraction failed: {e}")
        return BypassResult(
            success=False,
            url=None,
            message=f"Extraction error: {str(e)[:50]}",
            method=BypassMethod.PAGE_EXTRACTION,
            execution_time=0.0,
            original_url=url
        )

async def check_rate_limit(self, user_id: int) -> bool:
    current_time = time.time()
    
    if user_id not in self.request_count:
        self.request_count[user_id] = []
    
    user_requests = self.request_count[user_id]
    
    user_requests = [req_time for req_time in user_requests if current_time - req_time <= self.RATE_LIMIT_WINDOW]
    self.request_count[user_id] = user_requests
    
    if len(user_requests) >= self.MAX_REQUESTS_PER_WINDOW:
        return False
    
    user_requests.append(current_time)
    return True

async def log_bypass_attempt(self, result: BypassResult):
    if not self.db_conn:
        return
    
    try:
        cursor = self.db_conn.cursor()
        cursor.execute('''
            INSERT INTO bypass_logs 
            (user_id, username, guild_id, original_url, bypassed_url, method, success, execution_time, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.user_id,
            str(result.user_id),
            result.guild_id,
            result.original_url[:1000],
            result.url[:1000] if result.url else None,
            result.method.value,
            result.success,
            result.execution_time,
            result.message[:500] if not result.success else None
        ))
        
        self.db_conn.commit()
        
    except Exception as e:
        logger.error(f"Failed to log bypass attempt: {e}")

async def update_user_stats(self, user_id: int, success: bool):
    if not self.db_conn:
        return
    
    try:
        cursor = self.db_conn.cursor()
        
        today = datetime.now(UTC).date()
        cursor.execute('''
            SELECT daily_request_count, last_daily_reset 
            FROM user_stats 
            WHERE user_id = ?
        ''', (user_id,))
        
        row = cursor.fetchone()
        
        if row:
            last_reset = datetime.fromisoformat(row['last_daily_reset']).date()
            if last_reset < today:
                daily_count = 1
            else:
                daily_count = row['daily_request_count'] + 1
        else:
            daily_count = 1
        
        if daily_count > self.DAILY_LIMIT:
            return
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_stats 
            (user_id, total_requests, successful_requests, failed_requests, 
             last_request, daily_request_count, last_daily_reset)
            VALUES (?, 
                COALESCE((SELECT total_requests FROM user_stats WHERE user_id = ?), 0) + 1,
                COALESCE((SELECT successful_requests FROM user_stats WHERE user_id = ?), 0) + (1 * ?),
                COALESCE((SELECT failed_requests FROM user_stats WHERE user_id = ?), 0) + (1 * ?),
                CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (user_id, user_id, user_id, int(success), user_id, int(not success), daily_count))
        
        self.db_conn.commit()
        
    except Exception as e:
        logger.error(f"Failed to update user stats: {e}")

EnhancedLinkvertiseBypassBot.bypass_linkvertise = bypass_linkvertise
EnhancedLinkvertiseBypassBot.try_bypass_methods = try_bypass_methods
EnhancedLinkvertiseBypassBot.bypass_with_dynamic_decode = bypass_with_dynamic_decode
EnhancedLinkvertiseBypassBot.bypass_with_api = bypass_with_api
EnhancedLinkvertiseBypassBot.bypass_with_fallback_services = bypass_with_fallback_services
EnhancedLinkvertiseBypassBot.extract_from_page = extract_from_page
EnhancedLinkvertiseBypassBot.check_rate_limit = check_rate_limit
EnhancedLinkvertiseBypassBot.log_bypass_attempt = log_bypass_attempt
EnhancedLinkvertiseBypassBot.update_user_stats = update_user_stats

if __name__ == "__main__":
    bot.run(os.getenv("D_TOKEN", ""))

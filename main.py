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
from datetime import datetime, timedelta
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

# Your specific server ID for logging
LOG_SERVER_ID = 1463101887141249107
LOG_CHANNEL_ID = 1463101887871062153

# Auto-bypass channel constants
YOUR_USER_ID = 783542452756938813
AUTO_BYPASS_CHANNEL_ID = 1464505405815259305
AUTO_BYPASS_SERVER_ID = 1463101887141249107

# Ping role ID
PING_ROLE_ID = 1330639211317035089  # Game ping role ID

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

class EnhancedLinkvertiseBypassBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        
        super().__init__(command_prefix='!', intents=intents)
        
        # SQLite database connection
        self.db_conn: Optional[sqlite3.Connection] = None
        
        # Rate limiting and statistics
        self.request_count: Dict[int, List[float]] = {}
        self.RATE_LIMIT_WINDOW = 60
        self.MAX_REQUESTS_PER_WINDOW = 20
        self.DAILY_LIMIT = 100
        
        # User statistics
        self.user_stats: Dict[int, Dict] = {}
        
        # Cache with LRU support
        self.cache: Dict[str, Tuple[str, float, BypassMethod]] = {}
        self.cache_order: List[str] = []
        self.MAX_CACHE_SIZE = 1000
        self.CACHE_TTL = 7200
        
        # Fallback services with improved list
        self.fallback_services = [
            {"url": "https://api.bypass.vip/bypass2", "method": "POST", "priority": 1, "timeout": 15},
            {"url": "https://bypass.pm/bypass2", "method": "GET", "priority": 2, "timeout": 15},
            {"url": "https://bypass.bot.nu/bypass2", "method": "GET", "priority": 3, "timeout": 15},
            {"url": "https://bypass.city/bypass2", "method": "GET", "priority": 4, "timeout": 15},
            {"url": "https://api.bypassfor.me/bypass", "method": "GET", "priority": 5, "timeout": 15},
        ]
        
        # Linkvertise domains - expanded list
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
        
        # Session management
        self.session: Optional[aiohttp.ClientSession] = None
        self.method_stats = {method.value: 0 for method in BypassMethod}
        self.blocked_urls = set()
        
        # Logging channel
        self.log_channel: Optional[discord.TextChannel] = None
        
        # Auto-bypass system
        self.auto_bypass_channel: Optional[discord.TextChannel] = None
        self.auto_bypass_allowed_users: Set[int] = {YOUR_USER_ID}
        
        # Ping role
        self.ping_role: Optional[discord.Role] = None
        
        # Last 24-hour stats timestamp
        self.last_daily_stats_time: Optional[datetime] = None
        
        # Thread pool for blocking operations
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Performance tracking
        self.performance_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "avg_response_time": 0.0,
            "success_rate": 0.0
        }
        
    async def setup_hook(self):
        """Setup database, session, and background tasks"""
        # Initialize database
        await self.init_database()
        
        # Initialize HTTP session
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        
        # Load authorized users from database
        await self.load_authorized_users()
        
        # Load cache from database
        await self.load_cache_from_database()
        
        # Start background tasks
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
        self.stats_task = asyncio.create_task(self.periodic_stats_log())
        self.health_check_task = asyncio.create_task(self.health_check_services())
    
    async def health_check_services(self):
        """Periodic health check of bypass services"""
        while True:
            await asyncio.sleep(600)  # Check every 10 minutes
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
                    except:
                        continue
                
                logger.info(f"Health check: {len(active_services)}/{len(self.fallback_services)} services active")
                
                # Reorder services based on health
                for i, service in enumerate(self.fallback_services):
                    if service["url"] in active_services:
                        service["priority"] = i + 1
                    else:
                        service["priority"] = len(self.fallback_services) + i + 1
                
                self.fallback_services.sort(key=lambda x: x["priority"])
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
    
    async def load_cache_from_database(self):
        """Load cache from database on startup"""
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
        """Save cache entry to database"""
        if not self.db_conn:
            return
            
        try:
            expires_at = (datetime.now() + timedelta(seconds=self.CACHE_TTL)).strftime('%Y-%m-%d %H:%M:%S')
            
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
        """Load authorized users from database"""
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
        """Initialize SQLite database and tables"""
        try:
            # Create database connection
            self.db_conn = sqlite3.connect('linkvertise_bot.db', check_same_thread=False)
            self.db_conn.row_factory = sqlite3.Row
            
            # Enable optimizations
            self.db_conn.execute('PRAGMA journal_mode = WAL')
            self.db_conn.execute('PRAGMA synchronous = NORMAL')
            self.db_conn.execute('PRAGMA cache_size = -2000')
            self.db_conn.execute('PRAGMA foreign_keys = ON')
            
            cursor = self.db_conn.cursor()
            
            # Create tables
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
            
            # Create indexes
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
            
            # Insert your user ID if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO authorized_users (user_id, added_by, can_auto_bypass, can_use_batch)
                VALUES (?, ?, 1, 1)
            ''', (YOUR_USER_ID, YOUR_USER_ID))
            
            self.db_conn.commit()
            logger.info("SQLite database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            self.db_conn = None
    
    async def log_to_database(self, result: BypassResult, user: discord.User, guild: Optional[discord.Guild] = None):
        """Log bypass attempt to SQLite database"""
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
                guild.name if guild else 'DM',
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
                ''', (
                    guild.id,
                    guild.name,
                    guild.id
                ))
                
                cursor.execute('''
                    UPDATE server_stats 
                    SET unique_users = (
                        SELECT COUNT(DISTINCT user_id) 
                        FROM bypass_logs 
                        WHERE guild_id = ?
                    )
                    WHERE guild_id = ?
                ''', (guild.id, guild.id))
            
            today = datetime.now().date().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO daily_stats 
                (date, total_requests, successful_requests, unique_users, unique_servers)
                VALUES (?, 
                    COALESCE((SELECT total_requests FROM daily_stats WHERE date = ?), 0) + 1,
                    COALESCE((SELECT successful_requests FROM daily_stats WHERE date = ?), 0) + ?,
                    COALESCE((SELECT unique_users FROM daily_stats WHERE date = ?), 0),
                    COALESCE((SELECT unique_servers FROM daily_stats WHERE date = ?), 0))
            ''', (
                today,
                today,
                today,
                1 if result.success else 0,
                today,
                today
            ))
            
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
            
            # Update performance stats
            self.performance_stats["total_requests"] += 1
            if result.success:
                self.performance_stats["successful_requests"] += 1
                self.performance_stats["success_rate"] = (
                    self.performance_stats["successful_requests"] / self.performance_stats["total_requests"]
                ) * 100
            
            alpha = 0.1
            old_avg = self.performance_stats["avg_response_time"]
            self.performance_stats["avg_response_time"] = (
                alpha * result.execution_time + (1 - alpha) * old_avg
            )
            
        except Exception as e:
            logger.error(f"Failed to log to database: {e}")
            if self.db_conn:
                self.db_conn.rollback()
    
    async def send_log_to_discord(self, result: BypassResult, user: discord.User, guild: Optional[discord.Guild] = None):
        """Send log to your private Discord channel"""
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
                title="Bypass Log",
                color=0x00FF00 if result.success else 0xFF0000,
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="User", value=f"{user} ({user.id})", inline=True)
            embed.add_field(name="Server", value=f"{guild.name if guild else 'DM'} ({guild.id if guild else 'N/A'})", inline=True)
            embed.add_field(name="Success", value="YES" if result.success else "NO", inline=True)
            
            embed.add_field(name="Original URL", value=f"```{truncate_url(result.original_url)}```", inline=False)
            
            if result.success:
                embed.add_field(name="Bypassed URL", value=f"```{truncate_url(result.url)}```", inline=False)
            
            embed.add_field(name="Method", value=result.method.value.replace('_', ' ').title(), inline=True)
            embed.add_field(name="Time", value=f"{result.execution_time:.2f}s", inline=True)
            
            if self.performance_stats["total_requests"] > 0:
                embed.add_field(
                    name="Performance",
                    value=f"Success Rate: {self.performance_stats['success_rate']:.1f}%\nAvg Time: {self.performance_stats['avg_response_time']:.2f}s",
                    inline=True
                )
            
            await self.log_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to send log to Discord: {e}")
    
    # Auto-bypass methods
    async def auto_bypass_to_channel(self, urls: List[str], user: discord.User, source_guild: Optional[discord.Guild] = None):
        """Auto-bypass multiple links and send to specified channel"""
        if not self.auto_bypass_channel:
            logger.error("Auto-bypass channel not found")
            return []
        
        if not urls:
            return []
        
        results = []
        successful_urls = []
        
        # Process URLs in batches to avoid rate limiting
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
            
            # Wait for batch to complete
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
            
            # Small delay between batches
            if i + BATCH_SIZE < len(urls):
                await asyncio.sleep(1)
        
        if successful_urls:
            await self.send_auto_bypass_results(successful_urls, user, len(urls))
        
        return successful_urls
    
    async def send_auto_bypass_results(self, successful_urls: List[str], user: discord.User, total_attempted: int):
        """Send auto-bypass results to the specified channel"""
        if not self.auto_bypass_channel:
            return
        
        try:
            # Get the ping role mention
            ping_mention = f"<@&{PING_ROLE_ID}>" if self.ping_role else f"<@&{PING_ROLE_ID}>"
            
            if len(successful_urls) == 1:
                # Send ping message first
                await self.auto_bypass_channel.send(f"{ping_mention} New game file bypassed!")
                
                embed = discord.Embed(
                    title="Game File Bypassed",
                    description="File has been successfully bypassed.",
                    color=0x00FF00,
                    timestamp=datetime.utcnow()
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
                    value=f"✅ 1/{total_attempted} successful",
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
                # Send ping message for multiple files
                await self.auto_bypass_channel.send(f"{ping_mention} {len(successful_urls)} game files bypassed!")
                
                # Group URLs for better display
                for i in range(0, len(successful_urls), 3):
                    batch_urls = successful_urls[i:i + 3]
                    batch_num = i // 3 + 1
                    total_batches = (len(successful_urls) + 2) // 3
                    
                    embed = discord.Embed(
                        title=f"Game Files Bypassed (Batch {batch_num}/{total_batches})",
                        description=f"Files {i+1}-{min(i+3, len(successful_urls))} of {len(successful_urls)}",
                        color=0x00FF00,
                        timestamp=datetime.utcnow()
                    )
                    
                    for j, final_url in enumerate(batch_urls, 1):
                        display_url = final_url
                        if len(display_url) > 150:
                            display_url = display_url[:147] + "..."
                        
                        embed.add_field(
                            name=f"Direct Link #{i + j}",
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
                            value=f"✅ {len(successful_urls)}/{total_attempted} successful",
                            inline=True
                        )
                    
                    view = discord.ui.View(timeout=None)
                    for j, final_url in enumerate(batch_urls, 1):
                        view.add_item(discord.ui.Button(
                            label=f"Open Link #{i + j}",
                            url=final_url,
                            style=discord.ButtonStyle.link
                        ))
                    
                    await self.auto_bypass_channel.send(embed=embed, view=view)
            
            logger.info(f"Auto-bypass results sent to channel with role ping: {len(successful_urls)} URLs")
            
        except Exception as e:
            logger.error(f"Failed to send auto-bypass results: {e}")
    
    async def close(self):
        """Cleanup on shutdown"""
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
        """Periodic cleanup of old data"""
        while True:
            await asyncio.sleep(300)
            try:
                self.cleanup_old_data()
                await self.cleanup_database_cache()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def cleanup_database_cache(self):
        """Clean up expired cache entries from database"""
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
        """Periodic logging of statistics to Discord - Only every 24 hours"""
        await self.wait_until_ready()
        await asyncio.sleep(60)
        
        while True:
            now = datetime.utcnow()
            if self.last_daily_stats_time is None or (now - self.last_daily_stats_time) >= timedelta(hours=24):
                await self.log_24hour_stats()
                self.last_daily_stats_time = now
            
            await asyncio.sleep(3600)
    
    async def log_24hour_stats(self):
        """Log 24-hour statistics to Discord"""
        if not self.db_conn or not self.log_channel:
            return
            
        try:
            cursor = self.db_conn.cursor()
            twenty_four_hours_ago = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT guild_id) as unique_servers
                FROM bypass_logs 
                WHERE timestamp >= ?
            ''', (twenty_four_hours_ago,))
            
            stats = cursor.fetchone()
            
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
                timestamp=datetime.utcnow()
            )
            
            success_rate = 0
            if stats['total_requests'] > 0:
                success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
            
            embed.add_field(name="Total Requests", value=f"{stats['total_requests']:,}", inline=True)
            embed.add_field(name="Successful", value=f"{stats['successful_requests']:,}", inline=True)
            embed.add_field(name="Success Rate", value=f"{success_rate:.1f}%", inline=True)
            
            embed.add_field(name="Unique Users", value=f"{stats['unique_users']:,}", inline=True)
            embed.add_field(name="Unique Servers", value=f"{stats['unique_servers']:,}", inline=True)
            embed.add_field(name="Memory Cache", value=f"{len(self.cache):,} entries", inline=True)
            
            if method_counts:
                methods_text = "\n".join([f"• {row['method']}: {row['count']:,}" for row in method_counts])
                embed.add_field(name="Methods Used", value=methods_text, inline=False)
            
            if top_users:
                top_users_text = "\n".join([f"• {row['username']}: {row['request_count']:,}" for row in top_users])
                embed.add_field(name="Top Users (24 Hours)", value=top_users_text, inline=False)
            
            embed.add_field(
                name="Performance Summary",
                value=f"• Avg Response Time: {self.performance_stats['avg_response_time']:.2f}s\n• Overall Success Rate: {self.performance_stats['success_rate']:.1f}%\n• Active Services: {len([s for s in self.fallback_services if s['priority'] <= 3])}/{len(self.fallback_services)}",
                inline=False
            )
            
            await self.log_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to log 24-hour stats: {e}")
    
    def cleanup_old_data(self):
        """Clean up old cache entries"""
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
        """Update LRU order for cache entry"""
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
        
        # ===== LOAD CHANNELS AND ROLES =====
        # Initialize auto-bypass channel
        try:
            guild = self.get_guild(AUTO_BYPASS_SERVER_ID)
            if guild:
                self.auto_bypass_channel = guild.get_channel(AUTO_BYPASS_CHANNEL_ID)
                if self.auto_bypass_channel:
                    logger.info(f"Auto-bypass channel loaded: #{self.auto_bypass_channel.name}")
                else:
                    logger.warning(f"Auto-bypass channel not found in guild. ID: {AUTO_BYPASS_CHANNEL_ID}")
                    
                    # Try to force fetch the channel
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
        
        # Initialize ping role
        if self.auto_bypass_channel and self.auto_bypass_channel.guild:
            self.ping_role = self.auto_bypass_channel.guild.get_role(PING_ROLE_ID)
            if self.ping_role:
                logger.info(f"Ping role loaded: @{self.ping_role.name}")
            else:
                logger.warning(f"Ping role not found. ID: {PING_ROLE_ID}")
        
        # Initialize log channel
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
        # ===== END OF CHANNEL LOADING =====
        
        await self.send_startup_notification()
    
    async def send_startup_notification(self):
        """Send startup notification to log channel"""
        if self.log_channel:
            embed = discord.Embed(
                title="Bot Started",
                description=f"Bot is now online in {len(self.guilds)} servers",
                color=0x00FF00,
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Bot User", value=f"{self.user} ({self.user.id})", inline=True)
            embed.add_field(name="Database", value="Connected" if self.db_conn else "Disabled", inline=True)
            embed.add_field(name="Auto-bypass Channel", 
                           value=f"✅ Connected: #{self.auto_bypass_channel.name}" if self.auto_bypass_channel else "❌ Not found", 
                           inline=True)
            embed.add_field(name="Ping Role", 
                           value=f"✅ Loaded: @{self.ping_role.name}" if self.ping_role else "❌ Not found", 
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
        """Check if URL is from Linkvertise"""
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False
            
            # Convert to lowercase for comparison
            url_lower = url.lower()
            domain = parsed.netloc.lower()
            
            # Check for ALL Linkvertise domains and subdomains
            for lv_domain in self.linkvertise_domains:
                if domain == lv_domain or domain.endswith('.' + lv_domain):
                    return True
            
            # Check for linkvertise in any part of the URL (more lenient)
            if 'linkvertise' in url_lower:
                return True
            
            # Check for common Linkvertise URL patterns
            linkvertise_patterns = [
                r'linkvertise\.(com|net|io|co|download|pw|xyz)',
                r'up-to-down\.net',
                r'link-to\.net',
                r'linkv\.to',
                r'lv\.to',
                r'/linkvertise\.com/',
                r'linkvertise/[0-9]+',
            ]
            
            for pattern in linkvertise_patterns:
                if re.search(pattern, url_lower, re.IGNORECASE):
                    return True
            
            # If still unsure, check for common Linkvertise path patterns
            common_paths = ['/access/', '/download/', '/view/', '/file/', '/folder/', '/content/', '/media/']
            for path in common_paths:
                if path in parsed.path:
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"URL validation error for {url}: {e}")
            # Be lenient - if validation fails, still try to bypass
            return True
    
    def extract_all_linkvertise_urls(self, text: str) -> List[str]:
        """Extract all Linkvertise URLs from text with various formats"""
        # More comprehensive patterns
        patterns = [
            r'https?://[^\s<>"\']*linkvertise[^\s<>"\']*',  # Standard URLs
            r'\[[^\]]*\]\s*\(\s*(https?://[^\s<>"\']*linkvertise[^\s<>"\']*)\s*\)',  # Markdown
            r'[\[\(]?\s*(https?://[^\s<>"\']*linkvertise[^\s<>"\']*)\s*[\]\)]?',  # Brackets
            r'(?:https?://)?(?:www\.)?linkvertise\.(?:com|net|io|co)/(?:[^\s<>"\']+)',  # Short form
        ]
        
        all_urls = []
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean the URL
                if isinstance(match, tuple):
                    match = match[0]
                url = match.strip('[]()<>"\'')
                if url and self.is_valid_linkvertise_url(url) and url not in all_urls:
                    all_urls.append(url)
    
        return all_urls
    
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
        self.request_count[user_id] = recent_requests[-50:]  # Keep only last 50 requests
        user_data['total_requests'] += 1
        user_data['daily_requests'] += 1
        user_data['last_request'] = now
        
        return True, 0, None
    
    def extract_dynamic_url(self, url: str) -> Optional[str]:
        """Extract URL from dynamic Linkvertise links"""
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            
            # Check for 'r' parameter (base64 encoded URL)
            if 'r' in query_params:
                encoded = query_params['r'][0]
                
                for encoded_str in [encoded, unquote(encoded)]:
                    try:
                        # Add padding if needed
                        padding = 4 - (len(encoded_str) % 4)
                        if padding != 4:
                            encoded_str += '=' * padding
                            
                        decoded = base64.b64decode(encoded_str).decode('utf-8', errors='ignore')
                        
                        if decoded.startswith(('http://', 'https://')):
                            return decoded
                    except:
                        continue
            
            # Check for other common parameters
            for param in ['url', 'link', 'redirect', 'destination', 'target', 'dl']:
                if param in query_params:
                    potential_url = query_params[param][0]
                    if potential_url.startswith(('http://', 'https://')):
                        return unquote(potential_url)
                        
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
                            message="Dynamic link decoded and resolved successfully",
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
                        message="Dynamic link decoded successfully (redirect failed)",
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
            
            # Try multiple patterns to extract link ID
            link_id = None
            id_patterns = [
                r'/(\d+)(?:/|$)',  # /123456/
                r'/access/(\d+)/',  # /access/123456/
                r'/download/(\d+)/',  # /download/123456/
                r'/view/(\d+)/',  # /view/123456/
                r'/file/(\d+)/',  # /file/123456/
                r'/folder/(\d+)/',  # /folder/123456/
                r'/content/(\d+)/',  # /content/123456/
                r'/media/(\d+)/',  # /media/123456/
            ]
            
            for pattern in id_patterns:
                match = re.search(pattern, parsed.path)
                if match:
                    link_id = match.group(1)
                    break
            
            # Also check query parameters
            if not link_id:
                query_params = parse_qs(parsed.query)
                for param in ['id', 'link', 'file', 'download', 'content', 'media']:
                    if param in query_params:
                        value = query_params[param][0]
                        if value.isdigit():
                            link_id = value
                            break
            
            if not link_id:
                # Try to find any numeric ID in the path
                numeric_parts = re.findall(r'\d+', path)
                if numeric_parts:
                    # Take the longest numeric part (most likely to be an ID)
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
            
            # Try multiple API endpoints
            api_endpoints = [
                f"https://publisher.linkvertise.com/api/v1/redirect/link/{link_id}/target",
                f"https://publisher.linkvertise.com/api/v1/redirect/link/static/{link_id}",
                f"https://linkvertise.com/api/v1/redirect/link/{link_id}/target",
                f"https://linkvertise.net/api/v1/redirect/link/{link_id}/target",
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
                            message="API bypass successful - got final URL",
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
            
            # If API fails, try page content extraction
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
        """Extract final URL from page content"""
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
        """Main bypass function that tries ALL methods"""
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
        
        # Check cache first
        cache_key = self.get_cache_key(url)
        if cache_key in self.cache:
            cached_url, _, method = self.cache[cache_key]
            self.update_cache_order(cache_key)
            self.method_stats[BypassMethod.CACHED.value] += 1
            
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
        
        # Try methods in order of speed/success rate
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
                
                if result.success:
                    result.user_id = user_id
                    result.guild_id = guild_id
                    
                    # Save to cache
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
        
        # If all methods failed, return the best result
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
        
        # Complete failure
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

# ============ BYPASS COMMAND ============
@bot.tree.command(name="bypass", description="Bypass a Linkvertise link")
@app_commands.describe(url="The Linkvertise URL to bypass")
@app_commands.checks.cooldown(1, 3.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def bypass_command(interaction: discord.Interaction, url: str):
    await interaction.response.defer(thinking=True)
    
    # BLOCK DM USAGE
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
            value="• https://linkvertise.com/123456\n• https://linkvertise.com/access/123456/...\n• https://linkvertise.com/.../dynamic?r=...\n• https://linkvertise.net/...\n• https://linkv.to/...",
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
    
    if result.success:
        embed = discord.Embed(
            title="✅ Bypass Successful",
            color=0x00FF00,
            timestamp=datetime.utcnow()
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
            value=f"`{result.method.value.replace('_', ' ').title()}`",
            inline=True
        )
        
        embed.add_field(
            name="Time",
            value=f"`{result.execution_time:.2f}s`",
            inline=True
        )
        
        if interaction.user.id not in bot.auto_bypass_allowed_users:
            embed.add_field(
                name="Want More Features?",
                value=f"Get access to **/autobypass** and **/batch** commands!\nUse `/request_access` to learn how.",
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
        
        # Also send to auto-bypass channel if user is authorized
        if interaction.user.id in bot.auto_bypass_allowed_users and bot.auto_bypass_channel:
            try:
                # Send ping message
                ping_mention = f"<@&{PING_ROLE_ID}>" if bot.ping_role else f"<@&{PING_ROLE_ID}>"
                await bot.auto_bypass_channel.send(f"{ping_mention} Game file bypassed via command!")
                
                embed_channel = discord.Embed(
                    title="Game File Bypassed",
                    description="File has been successfully bypassed.",
                    color=0x00FF00,
                    timestamp=datetime.utcnow()
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
                    value=f"Via `/bypass` in {interaction.guild.name}",
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
        embed = discord.Embed(
            title="❌ Bypass Failed",
            description=result.message,
            color=0xFF0000,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(
            name="URL",
            value=f"```{url[:150] if len(url) > 150 else url}```",
            inline=False
        )
        
        embed.add_field(
            name="Method Tried",
            value=f"`{result.method.value.replace('_', ' ').title()}`",
            inline=True
        )
        
        embed.add_field(
            name="Troubleshooting",
            value="• Try a different Linkvertise URL\n• Check if the URL is valid\n• The link might be expired or private",
            inline=False
        )
        
        embed.set_footer(text="Try again with a different link")
        
        await interaction.followup.send(embed=embed)

# ============ PBYPASS COMMAND ============
@bot.tree.command(name="pbypass", description="PRIVATE bypass - Bypass links without sending to channel")
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
            value="• https://linkvertise.com/123456\n• https://linkvertise.com/access/123456/...\n• https://linkvertise.com/.../dynamic?r=...",
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
            title="✅ Private Bypass Successful",
            color=0x00FF00,
            timestamp=datetime.utcnow()
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
            value=f"`{result.method.value.replace('_', ' ').title()}`",
            inline=True
        )
        
        embed.add_field(
            name="Time",
            value=f"`{result.execution_time:.2f}s`",
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
            title="❌ Private Bypass Failed",
            description=result.message,
            color=0xFF0000,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(
            name="URL",
            value=f"```{url[:150] if len(url) > 150 else url}```",
            inline=False
        )
        
        embed.add_field(
            name="Method Tried",
            value=f"`{result.method.value.replace('_', ' ').title()}`",
            inline=True
        )
        
        embed.add_field(
            name="Troubleshooting",
            value="• Try a different Linkvertise URL\n• Check if the URL is valid\n• The link might be expired or private",
            inline=False
        )
        
        embed.set_footer(text="Try again with a different link")
        
        await interaction.followup.send(embed=embed, ephemeral=True)

# ============ COMMAND ERROR HANDLERS ============
@bypass_command.error
async def bypass_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.CommandOnCooldown):
        embed = discord.Embed(
            title="Command on Cooldown",
            description=f"Please wait {error.retry_after:.1f} seconds.",
            color=0xFFA500
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        logger.error(f"Command error: {error}")
        embed = discord.Embed(
            title="Unexpected Error",
            description="An error occurred while processing your request.",
            color=0xFF0000
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

@pbypass_command.error
async def pbypass_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.CommandOnCooldown):
        embed = discord.Embed(
            title="Command on Cooldown",
            description=f"Please wait {error.retry_after:.1f} seconds.",
            color=0xFFA500
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        logger.error(f"Command error: {error}")
        embed = discord.Embed(
            title="Unexpected Error",
            description="An error occurred while processing your request.",
            color=0xFF0000
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ AUTO BYPASS COMMAND ============
@bot.tree.command(name="autobypass", description="Auto-bypass multiple links and send to results channel (Authorized users only)")
@app_commands.describe(
    links="Links separated by commas (e.g., link1, link2, link3, ...)"
)
async def autobypass_command(interaction: discord.Interaction, links: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    
    if interaction.user.id not in bot.auto_bypass_allowed_users:
        embed = discord.Embed(
            title="Access Denied",
            description="You are not authorized to use auto-bypass.\n\n**Only authorized users can use this command.**",
            color=0xFF0000
        )
        
        if interaction.guild and interaction.guild.id == AUTO_BYPASS_SERVER_ID:
            embed.add_field(
                name="Want Access?",
                value=f"Ask <@{YOUR_USER_ID}> to authorize you using `/autobypass_users add @yourname`",
                inline=False
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    url_list = []
    for link in links.split(','):
        link = link.strip()
        if link and bot.is_valid_linkvertise_url(link):
            url_list.append(link)
    
    # Also extract any missed URLs using regex
    extracted_urls = bot.extract_linkvertise_urls(links)
    for url in extracted_urls:
        if url not in url_list and bot.is_valid_linkvertise_url(url):
            url_list.append(url)
    
    if not url_list:
        embed = discord.Embed(
            title="No Valid URLs",
            description="No valid Linkvertise URLs found in your input.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    MAX_AUTO_BYPASS = 100  # Increased limit
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
            title="✅ Auto-Bypass Complete",
            description=f"Successfully bypassed **{len(successful_urls)}/{len(url_list)}** links.",
            color=0x00FF00,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(
            name="Results Sent To",
            value=f"<#{AUTO_BYPASS_CHANNEL_ID}>",
            inline=True
        )
        
        embed.add_field(
            name="Processing Time",
            value=f"Approx. {len(url_list) * 2}s",
            inline=True
        )
        
        if successful_urls:
            examples = []
            for i, url in enumerate(successful_urls[:5], 1):
                filename = url.split('/')[-1][:30]
                examples.append(f"**{i}.** `{filename}...`")
            
            if len(successful_urls) > 5:
                examples.append(f"... and **{len(successful_urls) - 5}** more")
            
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
            title="❌ Auto-Bypass Failed",
            description="No links were successfully bypassed.",
            color=0xFF0000
        )
        embed.add_field(
            name="Possible Issues",
            value="• All URLs might be invalid or expired\n• Rate limiting from Linkvertise\n• Temporary service issues",
            inline=False
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

# ============ BATCH COMMAND ============
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
    
    if interaction.user.id != YOUR_USER_ID and (not user_data or not user_data['can_use_batch']):
        embed = discord.Embed(
            title="Access Denied",
            description="You need authorization to use the batch command.",
            color=0xFFA500
        )
        
        embed.add_field(
            name="How to Get Access",
            value=f"Ask <@{YOUR_USER_ID}> to grant you batch command access.\n\nUse `/request_access` for more info.",
            inline=False
        )
        
        await interaction.followup.send(embed=embed)
        return
    
    url_list = []
    for link in links.split(','):
        link = link.strip()
        if link and bot.is_valid_linkvertise_url(link):
            url_list.append(link)
    
    extracted_urls = bot.extract_linkvertise_urls(links)
    for url in extracted_urls:
        if url not in url_list and bot.is_valid_linkvertise_url(url):
            url_list.append(url)
    
    if not url_list:
        embed = discord.Embed(
            title="No Valid URLs",
            description="No valid Linkvertise URLs found in your input.",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed)
        return
    
    if len(url_list) > 15:  # Increased limit
        url_list = url_list[:15]
        warning = "Limited to first 15 URLs"
    else:
        warning = None
    
    results = []
    successful = 0
    successful_urls = []
    failed_urls = []
    
    # Process in batches
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
        
        # Small delay between batches
        if i + BATCH_SIZE < len(url_list):
            await asyncio.sleep(0.5)
    
    color = 0x00FF00 if successful > 0 else 0xFFA500
    
    embed = discord.Embed(
        title="Batch Bypass Results",
        color=color,
        timestamp=datetime.utcnow()
    )
    
    embed.add_field(
        name="Summary",
        value=f"**{successful}/{len(results)}** successful bypasses",
        inline=False
    )
    
    if successful_urls:
        display_list = []
        for i, (original_url, success, _, direct_url) in enumerate(results):
            if success and i < 5:
                filename = original_url.split('/')[-1][:40]
                display_list.append(f"• `{filename}...`")
        
        display = "\n".join(display_list)
        
        if len(successful_urls) > 5:
            display += f"\n• ... and **{len(successful_urls) - 5}** more"
        
        embed.add_field(
            name="Successful Bypasses",
            value=display,
            inline=False
        )
    
    if failed_urls:
        display_list = []
        for url in failed_urls[:3]:
            filename = url.split('/')[-1][:40]
            display_list.append(f"• `{filename}...`")
        
        display = "\n".join(display_list)
        
        if len(failed_urls) > 3:
            display += f"\n• ... and **{len(failed_urls) - 3}** more"
        
        embed.add_field(
            name="Failed URLs",
            value=display,
            inline=False
        )
    
    if warning:
        embed.set_footer(text=warning)
    
    if successful_urls:
        view = discord.ui.View(timeout=180)
        # Add buttons for up to 5 successful URLs
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

# ============ STATS COMMAND ============
@bot.tree.command(name="stats", description="View your bypass statistics")
async def stats_command(interaction: discord.Interaction):
    user_id = interaction.user.id
    
    if user_id in bot.user_stats:
        stats = bot.user_stats[user_id]
        success_rate = (stats['successful_bypasses'] / max(stats['total_requests'], 1)) * 100
        
        embed = discord.Embed(
            title="📊 Your Bypass Statistics",
            color=0x7289DA,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(
            name="Total Requests",
            value=f"**{stats['total_requests']}**",
            inline=True
        )
        
        embed.add_field(
            name="Successful",
            value=f"**{stats['successful_bypasses']}**",
            inline=True
        )
        
        embed.add_field(
            name="Failed",
            value=f"**{stats['failed_bypasses']}**",
            inline=True
        )
        
        embed.add_field(
            name="Success Rate",
            value=f"**{success_rate:.1f}%**",
            inline=True
        )
        
        daily_used = stats.get('daily_requests', 0)
        daily_remaining = bot.DAILY_LIMIT - daily_used
        reset_time = stats.get('reset_time', time.time())
        
        embed.add_field(
            name="Daily Usage",
            value=f"**{daily_used}/{bot.DAILY_LIMIT}**",
            inline=True
        )
        
        embed.add_field(
            name="Daily Remaining",
            value=f"**{max(0, daily_remaining)}**",
            inline=True
        )
        
        now = time.time()
        user_requests = bot.request_count.get(user_id, [])
        recent_requests = [req for req in user_requests if now - req < bot.RATE_LIMIT_WINDOW]
        remaining = bot.MAX_REQUESTS_PER_WINDOW - len(recent_requests)
        
        embed.add_field(
            name="Current Minute",
            value=f"**{len(recent_requests)}/{bot.MAX_REQUESTS_PER_WINDOW}**",
            inline=True
        )
        
        embed.add_field(
            name="Remaining Requests",
            value=f"**{remaining}**",
            inline=True
        )
        
        last_request = stats.get('last_request', 0)
        if last_request:
            embed.add_field(
                name="Last Request",
                value=f"<t:{int(last_request)}:R>",
                inline=True
            )
        
        embed.set_footer(text=f"Statistics for {interaction.user.name}")
        
    else:
        embed = discord.Embed(
            title="📊 Your Statistics",
            description="You haven't made any bypass requests yet.",
            color=0x7289DA
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ CACHE COMMAND ============
@bot.tree.command(name="cache", description="View cache statistics")
async def cache_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="📦 Cache Statistics",
        color=0x7289DA,
        timestamp=datetime.utcnow()
    )
    
    embed.add_field(
        name="Total Cached URLs",
        value=f"**{len(bot.cache):,}**",
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
        name="< 1 hour",
        value=f"**{recent}**",
        inline=True
    )
    
    embed.add_field(
        name="> 1 hour",
        value=f"**{old}**",
        inline=True
    )
    
    embed.add_field(
        name="Database Cache",
        value=f"**{bot.MAX_CACHE_SIZE:,}** max entries",
        inline=True
    )
    
    embed.add_field(
        name="Cache TTL",
        value=f"**{bot.CACHE_TTL//3600} hours**",
        inline=True
    )
    
    method_stats_text = "\n".join([f"• **{method}**: {count:,}" for method, count in bot.method_stats.items() if count > 0])
    embed.add_field(
        name="Bypass Methods Used",
        value=method_stats_text or "No methods used yet",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ AUTO BYPASS USERS COMMAND ============
@bot.tree.command(name="autobypass_users", description="[ADMIN] Manage auto-bypass authorized users")
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
            title="✅ User Authorized",
            description=f"Added {user.mention} to authorized users.",
            color=0x00FF00
        )
        
        permissions = []
        if can_auto_bypass:
            permissions.append("**Auto-Bypass** (`/autobypass`)")
        if can_use_batch:
            permissions.append("**Batch Command** (`/batch`)")
        
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
            title="✅ User Removed",
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
                user_obj = bot.get_user(row['user_id'])
                username = user_obj.mention if user_obj else f"User {row['user_id']}"
                
                permissions = []
                if row['can_auto_bypass']:
                    permissions.append("Auto-Bypass")
                if row['can_use_batch']:
                    permissions.append("Batch")
                
                perm_text = ", ".join(permissions) if permissions else "None"
                users_list.append(f"• {username} - {perm_text}")
            
            embed = discord.Embed(
                title="👥 Authorized Users List",
                description="\n".join(users_list),
                color=0x7289DA
            )
            embed.set_footer(text=f"Total: {len(rows)} users")
        else:
            embed = discord.Embed(
                title="👥 Authorized Users List",
                description="No authorized users found.",
                color=0xFFA500
            )
        
    else:
        embed = discord.Embed(
            title="❌ Invalid Action",
            description="Use 'add', 'remove', or 'list'.",
            color=0xFF0000
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ REQUEST ACCESS COMMAND ============
@bot.tree.command(name="request_access", description="Request access to premium features")
async def request_access_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🔒 Request Access to Premium Features",
        color=0x7289DA
    )
    
    embed.add_field(
        name="Available Features",
        value="• **Auto-Bypass**: Process unlimited links at once\n• **Batch Command**: Process multiple links with one command",
        inline=False
    )
    
    embed.add_field(
        name="How to Get Access",
        value=f"Ask <@{YOUR_USER_ID}> to authorize you in the server.\n\nThey can use: `/autobypass_users add @yourname`",
        inline=False
    )
    
    embed.add_field(
        name="Current Status",
        value="✅ **You have access**" if interaction.user.id in bot.auto_bypass_allowed_users else "❌ **You don't have access**",
        inline=False
    )
    
    if interaction.guild and interaction.guild.id == AUTO_BYPASS_SERVER_ID:
        if interaction.user.id not in bot.auto_bypass_allowed_users:
            embed.add_field(
                name="Quick Request",
                value=f"Tag <@{YOUR_USER_ID}> in this server to request access!",
                inline=False
            )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ ON MESSAGE EVENT ============
@bot.event
async def on_message(message):
    if message.author == bot.user or message.author.bot:
        return
    
    # Auto-detect Linkvertise URLs in guild messages
    if message.guild:
        urls = bot.extract_linkvertise_urls(message.content)
        
        if urls:
            # Check if user is authorized for auto-bypass
            if message.author.id in bot.auto_bypass_allowed_users:
                # Ask if they want to auto-bypass
                view = discord.ui.View(timeout=60)
                
                button_auto = discord.ui.Button(
                    label="🚀 Auto-Bypass All Links",
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
                    
                    # Process all links
                    successful_urls = await bot.auto_bypass_to_channel(
                        urls=urls,
                        user=message.author,
                        source_guild=message.guild
                    )
                    
                    if successful_urls:
                        embed = discord.Embed(
                            title="✅ Auto-Bypass Complete",
                            description=f"Sent {len(successful_urls)} links to <#{AUTO_BYPASS_CHANNEL_ID}>",
                            color=0x00FF00
                        )
                        await interaction.followup.send(embed=embed, ephemeral=True)
                        
                        # Delete the original message if possible
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
                    # Existing single bypass logic
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
                
                response_text = f"<@{message.author.id}>, detected **{len(urls)}** Linkvertise link(s). Choose an option:"
                
            else:
                # Regular users only get single bypass
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
                
                response_text = f"Detected **{len(urls)}** Linkvertise link(s). Click below to bypass:"
            
            if message.channel.permissions_for(message.guild.me).send_messages:
                response = await message.reply(
                    response_text,
                    view=view,
                    mention_author=False
                )
                
                # Cleanup after timeout
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
    
    token = os.getenv('DISCORD_BOT_TOKEN')
    
    if not token:
        logger.error("No bot token found. Please set DISCORD_BOT_TOKEN in .env file")
        print("Error: No bot token found.")
        print("Create a .env file with: DISCORD_BOT_TOKEN=your_token_here")
        exit(1)
    
    try:
        bot.run(token)
    except KeyboardInterrupt:
        logger.info("Bot shutting down...")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

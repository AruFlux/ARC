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
from urllib.parse import urlparse, parse_qs, unquote
from typing import Optional, Tuple, Dict, List, Any
from datetime import datetime, timedelta
import aiohttp
import logging
from dataclasses import dataclass
from enum import Enum
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('linkvertise_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BypassMethod(Enum):
    DYNAMIC_DECODE = "dynamic_decode"
    API_BYPASS = "api_bypass"
    FALLBACK_SERVICE = "fallback_service"
    CACHED = "cached"

@dataclass
class BypassResult:
    success: bool
    url: Optional[str]
    message: str
    method: BypassMethod
    execution_time: float
    original_url: str

class EnhancedLinkvertiseBypassBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix='!', intents=intents)
        
        self.request_count: Dict[int, List[float]] = {}
        self.RATE_LIMIT_WINDOW = 60
        self.MAX_REQUESTS_PER_WINDOW = 20
        self.DAILY_LIMIT = 100
        
        self.user_stats: Dict[int, Dict] = {}
        self.daily_stats: Dict[int, Dict] = {}
        
        self.cache: Dict[str, Tuple[str, float, BypassMethod]] = {}
        self.CACHE_TTL = 7200
        
        self.fallback_services = [
            {"url": "https://api.bypass.vip/bypass2", "method": "POST", "priority": 1},
            {"url": "https://bypass.pm/bypass2", "method": "GET", "priority": 2},
            {"url": "https://bypass.bot.nu/bypass2", "method": "GET", "priority": 3},
        ]
        
        self.linkvertise_domains = [
            "linkvertise.com",
            "linkvertise.net",
            "linkvertise.io",
            "linkvertise.co",
            "up-to-down.net"
        ]
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_cleanup = time.time()
        self.method_stats = {method.value: 0 for method in BypassMethod}
        self.blocked_urls = set()
        
    async def setup_hook(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
        self.stats_task = asyncio.create_task(self.periodic_stats_log())
        
    async def close(self):
        if self.session:
            await self.session.close()
        if hasattr(self, 'cleanup_task'):
            self.cleanup_task.cancel()
        if hasattr(self, 'stats_task'):
            self.stats_task.cancel()
        await super().close()
        
    async def periodic_cleanup(self):
        while True:
            await asyncio.sleep(300)
            self.cleanup_old_data()
            
    async def periodic_stats_log(self):
        while True:
            await asyncio.sleep(3600)
            self.log_statistics()
            
    def cleanup_old_data(self):
        now = time.time()
        
        for user_id in list(self.request_count.keys()):
            user_requests = [
                req for req in self.request_count[user_id] 
                if now - req < self.RATE_LIMIT_WINDOW
            ]
            if user_requests:
                self.request_count[user_id] = user_requests
            else:
                del self.request_count[user_id]
        
        expired_keys = [
            key for key, (_, timestamp, _) in self.cache.items()
            if now - timestamp > self.CACHE_TTL
        ]
        for key in expired_keys:
            del self.cache[key]
            
        week_ago = now - 604800
        for user_id in list(self.daily_stats.keys()):
            if self.daily_stats[user_id].get('date', 0) < week_ago:
                del self.daily_stats[user_id]
                
        logger.info(f"Cleanup completed: Removed {len(expired_keys)} expired cache entries")
        
    def log_statistics(self):
        stats = {
            "total_users": len(self.user_stats),
            "total_cached": len(self.cache),
            "method_stats": self.method_stats.copy(),
            "blocked_urls": len(self.blocked_urls),
        }
        logger.info(f"Statistics: {json.dumps(stats, indent=2)}")
        
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
    
    def get_cache_key(self, url: str) -> str:
        normalized = url.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def is_valid_linkvertise_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False
                
            domain = parsed.netloc.lower()
            
            for lv_domain in self.linkvertise_domains:
                if domain.endswith(lv_domain):
                    return True
                    
            if 'linkvertise' in domain or '/linkvertise.com/' in url:
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"URL validation error for {url}: {e}")
            return False
    
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
            return False, reset_in, f"Daily limit reached ({self.DAILY_LIMIT} requests)"
        
        user_requests = self.request_count.get(user_id, [])
        recent_requests = [req for req in user_requests if now - req < self.RATE_LIMIT_WINDOW]
        
        if len(recent_requests) >= self.MAX_REQUESTS_PER_WINDOW:
            oldest = min(recent_requests)
            reset_in = int(oldest + self.RATE_LIMIT_WINDOW - now)
            return False, reset_in, f"Rate limit exceeded ({self.MAX_REQUESTS_PER_WINDOW}/min)"
        
        recent_requests.append(now)
        self.request_count[user_id] = recent_requests
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
                            
                        decoded = base64.b64decode(encoded_str).decode('utf-8')
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
                message=f"Dynamic decode error: {str(e)[:50]}",
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
            
            random_num = random.randint(1000000, 9999999)
            
            api_url = f"https://publisher.linkvertise.com/api/v1/redirect/link/static/{path}"
            
            try:
                async with self.session.get(api_url) as response:
                    if response.status != 200:
                        return BypassResult(
                            success=False,
                            url=None,
                            message=f"API error: HTTP {response.status}",
                            method=BypassMethod.API_BYPASS,
                            execution_time=time.time() - start_time,
                            original_url=url
                        )
                    
                    data = await response.json()
                    
            except asyncio.TimeoutError:
                return BypassResult(
                    success=False,
                    url=None,
                    message="API request timeout",
                    method=BypassMethod.API_BYPASS,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            if not data.get("success", False):
                return BypassResult(
                    success=False,
                    url=None,
                    message="Link not found or invalid",
                    method=BypassMethod.API_BYPASS,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            link_id = data.get("data", {}).get("link", {}).get("id")
            if not link_id:
                return BypassResult(
                    success=False,
                    url=None,
                    message="Could not extract link ID",
                    method=BypassMethod.API_BYPASS,
                    execution_time=time.time() - start_time,
                    original_url=url
                )
            
            serial_data = {
                "timestamp": int(time.time() * 1000),
                "random": str(random_num),
                "link_id": link_id
            }
            
            serial_json = json.dumps(serial_data, separators=(',', ':'))
            serial_base64 = base64.b64encode(serial_json.encode()).decode()
            
            target_url = f"https://publisher.linkvertise.com/api/v1/redirect/link/{path}/target?serial={serial_base64}"
            
            async with self.session.get(target_url) as response:
                if response.status != 200:
                    return BypassResult(
                        success=False,
                        url=None,
                        message=f"Target API error: HTTP {response.status}",
                        method=BypassMethod.API_BYPASS,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
                
                target_data = await response.json()
            
            if target_data.get("success", False):
                final_url = target_data.get("data", {}).get("target")
                if final_url:
                    self.cache[cache_key] = (final_url, time.time(), BypassMethod.API_BYPASS)
                    self.method_stats[BypassMethod.API_BYPASS.value] += 1
                    
                    return BypassResult(
                        success=True,
                        url=final_url,
                        message="API bypass successful",
                        method=BypassMethod.API_BYPASS,
                        execution_time=time.time() - start_time,
                        original_url=url
                    )
            
            return BypassResult(
                success=False,
                url=None,
                message="Bypass failed - no target URL found",
                method=BypassMethod.API_BYPASS,
                execution_time=time.time() - start_time,
                original_url=url
            )
            
        except Exception as e:
            logger.error(f"API bypass error: {e}")
            return BypassResult(
                success=False,
                url=None,
                message=f"Unexpected error: {str(e)[:50]}",
                method=BypassMethod.API_BYPASS,
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
                    async with self.session.post(service["url"], json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("success") and data.get("destination"):
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
                    async with self.session.get(service["url"], params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("success") and data.get("destination"):
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
    
    async def bypass_linkvertise(self, url: str, user_id: int) -> BypassResult:
        logger.info(f"Processing bypass request for {url}")
        
        if url in self.blocked_urls:
            return BypassResult(
                success=False,
                url=None,
                message="This URL is blocked",
                method=BypassMethod.API_BYPASS,
                execution_time=0,
                original_url=url
            )
        
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        if 'r' in query_params or 'dynamic' in parsed.path:
            result = await self.bypass_dynamic_link(url)
        else:
            result = await self.bypass_api_link(url)
            
            if not result.success:
                result = await self.bypass_fallback(url)
        
        if user_id in self.user_stats:
            if result.success:
                self.user_stats[user_id]['successful_bypasses'] += 1
            else:
                self.user_stats[user_id]['failed_bypasses'] += 1
        
        return result
    
    def extract_linkvertise_urls(self, text: str) -> List[str]:
        pattern = r'https?://[^\s]*linkvertise[^\s]*'
        return re.findall(pattern, text, re.IGNORECASE)

bot = EnhancedLinkvertiseBypassBot()

@bot.tree.command(name="bypass", description="Bypass a Linkvertise link")
@app_commands.describe(url="The Linkvertise URL to bypass")
@app_commands.checks.cooldown(1, 3.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def bypass_command(interaction: discord.Interaction, url: str):
    await interaction.response.defer(thinking=True)
    
    if not bot.is_valid_linkvertise_url(url):
        embed = discord.Embed(
            title="Invalid URL",
            description="The provided URL is not a valid Linkvertise link.",
            color=0xFF0000
        )
        embed.add_field(
            name="Supported formats",
            value="https://linkvertise.com/123456/page\nhttps://linkvertise.com/.../dynamic?r=...\nhttps://linkvertise.net/...",
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
    
    result = await bot.bypass_linkvertise(url, interaction.user.id)
    
    if result.success:
        embed = discord.Embed(
            title="Bypass Successful",
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
            text=f"Requested by {interaction.user.name}",
            icon_url=interaction.user.display_avatar.url
        )
        
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(
            label="Open Direct Link",
            url=result.url,
            style=discord.ButtonStyle.link
        ))
        
        await interaction.followup.send(embed=embed, view=view)
        
    else:
        embed = discord.Embed(
            title="Bypass Failed",
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
        
        embed.set_footer(text="Try again with a different link")
        
        await interaction.followup.send(embed=embed)

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

@bot.tree.command(name="stats", description="View your bypass statistics")
async def stats_command(interaction: discord.Interaction):
    user_id = interaction.user.id
    
    if user_id in bot.user_stats:
        stats = bot.user_stats[user_id]
        success_rate = (stats['successful_bypasses'] / max(stats['total_requests'], 1)) * 100
        
        embed = discord.Embed(
            title="Your Bypass Statistics",
            color=0x7289DA,
            timestamp=datetime.utcnow()
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
        recent_requests = bot.request_count.get(user_id, [])
        recent_count = len([r for r in recent_requests if now - r < bot.RATE_LIMIT_WINDOW])
        
        embed.add_field(
            name="Current Minute",
            value=f"{recent_count}/{bot.MAX_REQUESTS_PER_WINDOW}",
            inline=True
        )
        
        last_request = stats.get('last_request', 0)
        if last_request:
            last_time = datetime.fromtimestamp(last_request)
            embed.add_field(
                name="Last Request",
                value=f"<t:{int(last_request)}:R>",
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

@bot.tree.command(name="batch", description="Bypass multiple links at once")
@app_commands.describe(
    links="Links separated by commas (e.g., link1, link2, link3)"
)
@app_commands.checks.cooldown(1, 30.0, key=lambda i: (i.guild_id, i.user.id) if i.guild_id else i.user.id)
async def batch_command(interaction: discord.Interaction, links: str):
    await interaction.response.defer(thinking=True)
    
    # Split links by commas and clean up whitespace
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
        await interaction.followup.send(embed=embed)
        return
    
    if len(url_list) > 10:
        url_list = url_list[:10]
        warning = "Limited to first 10 URLs"
    else:
        warning = None
    
    results = []
    successful = 0
    successful_urls = []
    failed_urls = []
    
    for url in url_list:
        result = await bot.bypass_linkvertise(url, interaction.user.id)
        results.append((url, result.success, result.message, result.url))
        if result.success:
            successful += 1
            successful_urls.append(result.url)
        else:
            failed_urls.append(url)
    
    color = 0x00FF00 if successful > 0 else 0xFFA500
    
    embed = discord.Embed(
        title="Batch Bypass Results",
        color=color,
        timestamp=datetime.utcnow()
    )
    
    embed.add_field(
        name="Summary",
        value=f"{successful}/{len(results)} successful bypasses",
        inline=False
    )
    
    if successful_urls:
        display_list = []
        for i, (original_url, success, _, direct_url) in enumerate(results):
            if success and i < 3:
                display_list.append(f"• {original_url[:60]}...")
        display = "\n".join(display_list)
        
        if len(successful_urls) > 3:
            display += f"\n• ... and {len(successful_urls) - 3} more"
        
        embed.add_field(
            name="Successful Bypasses",
            value=display,
            inline=False
        )
    
    if failed_urls:
        display_list = []
        for url in failed_urls[:3]:
            display_list.append(f"• {url[:60]}...")
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
    
    # Create view with buttons for direct links
    if successful_urls:
        view = discord.ui.View(timeout=180)
        for i, direct_url in enumerate(successful_urls[:5]):
            if i < len(url_list):
                button_label = f"Direct Link {i+1}"
            else:
                button_label = f"Link {i+1}"
            
            button = discord.ui.Button(
                label=button_label,
                url=direct_url,
                style=discord.ButtonStyle.link
            )
            view.add_item(button)
        
        await interaction.followup.send(embed=embed, view=view)
    else:
        await interaction.followup.send(embed=embed)

@bot.tree.command(name="cache", description="View cache statistics")
async def cache_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Cache Statistics",
        color=0x7289DA,
        timestamp=datetime.utcnow()
    )
    
    embed.add_field(
        name="Total Cached URLs",
        value=f"{len(bot.cache)}",
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
        value=f"{recent}",
        inline=True
    )
    
    embed.add_field(
        name="> 1 hour",
        value=f"{old}",
        inline=True
    )
    
    embed.add_field(
        name="Bypass Methods",
        value=f"```{json.dumps(bot.method_stats, indent=2)}```",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.event
async def on_message(message):
    if message.author == bot.user or message.author.bot:
        return
    
    urls = bot.extract_linkvertise_urls(message.content)
    
    if urls:
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
        
        if message.channel.permissions_for(message.guild.me).send_messages:
            response = await message.reply(
                f"Detected {len(urls)} Linkvertise link(s). Click below to bypass:",
                view=view,
                mention_author=False
            )
            
            async def cleanup():
                await asyncio.sleep(300)
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

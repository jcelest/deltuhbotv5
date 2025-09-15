#!/usr/bin/env python3
"""
UNIFIED BOT - Connects to single API with both lit/dp and SD functionality
Much simpler than managing two APIs
FIXED: Discord interaction timeout issues in levels command
"""

import uuid
import os
import math
import io
import asyncio
import httpx
import discord
import traceback
from discord.ext import commands
from discord import app_commands
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from collections import defaultdict
from typing import Optional

# ─── INVITE & GUILD CONFIG ───────────────────────────────────────
TEST_GUILD_ID = 1218555075098841088  # ← replace with your test server ID
TEST_GUILD    = discord.Object(id=TEST_GUILD_ID)

# ─── IMAGE & FONT CONFIG ─────────────────────────────────────────
SIDE_PADDING     = 50
COLUMN_SPACING   = 75
HEADER_FONT_SIZE = 30
BODY_FONT_SIZE   = 50
ROW_PADDING      = 10
BOTTOM_PADDING   = 40

# ─── BOT & API CONFIGURATION ────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN') or ''
API_BASE_URL      = 'http://127.0.0.1:8001'  # Single unified API

# ─── BOT SETUP ───────────────────────────────────────────────────
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

# ─── HELPERS ─────────────────────────────────────────────────────
def format_large_number(num):
    if num is None: return 'N/A'
    if num >= 999_950_000: return f'{num/1_000_000_000:.2f}B'
    if num >= 1_000_000: return f'{num/1_000_000:.2f}M'
    if num >= 1_000: return f'{num/1_000:.2f}K'
    return str(num)

# ─── ENHANCED HTTP CLIENT ──────────────────────────────────────────
async def make_api_request(url: str, method: str = "GET", params=None, json_data=None, timeout_seconds: int = 60):
    """Make API request with proper timeout and error handling"""
    try:
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method.upper() == "GET":
                response = await client.get(url, params=params or {})
            elif method.upper() == "POST":
                response = await client.post(url, json=json_data or {})
            elif method.upper() == "PUT":
                response = await client.put(url, params=params or {})
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            response.raise_for_status()
            return response.json()
            
    except httpx.TimeoutException:
        raise Exception(f"Request timed out after {timeout_seconds} seconds")
    except httpx.HTTPStatusError as e:
        raise Exception(f"HTTP {e.response.status_code}: {e.response.text}")
    except Exception as e:
        raise Exception(f"API request failed: {str(e)}")

# ─── PAGINATION & IMAGE GENERATOR ────────────────────────────────
class PaginatorView(discord.ui.View):
    def __init__(self, trades, headers, title, author, show_summaries=False, **kwargs):
        super().__init__(timeout=180)
        
        if show_summaries:
            self.processed_rows = self._process_trades_with_summaries(trades)
        else:
            self.processed_rows = trades
        
        self.headers      = headers
        self.title        = title
        self.author       = author
        self.current_page = 0
        self.per_page     = 15
        self.total_pages  = math.ceil(len(self.processed_rows) / self.per_page)
        
        self.header_top, self.header_bot = kwargs.get('header_gradient', ((120, 40, 0), (255, 140, 0)))
        self.body_top, self.body_bot = kwargs.get('body_gradient', ((0, 0, 0), (50, 50, 50)))
        
        # Fixed font loading with error handling
        try:
            self.hdr_font = ImageFont.truetype('RobotoCondensed-Bold.ttf', HEADER_FONT_SIZE)
            self.body_font = ImageFont.truetype('RobotoCondensed-Regular.ttf', BODY_FONT_SIZE)
        except OSError:
            self.hdr_font = ImageFont.load_default()
            self.body_font = ImageFont.load_default()

    def _process_trades_with_summaries(self, trades):
        if not trades:
            return []

        trades_by_date = defaultdict(list)
        for trade in trades:
            trade_date = datetime.fromisoformat(trade['trade_time']).strftime('%Y-%m-%d')
            trades_by_date[trade_date].append(trade)

        all_rows = []
        for trade_date in sorted(trades_by_date.keys(), reverse=True):
            day_trades = trades_by_date[trade_date]
            
            total_quantity = sum(t['quantity'] for t in day_trades)
            total_value = sum(t['trade_value'] for t in day_trades)

            summary_row = {
                'is_summary': True,
                'Ticker': day_trades[0]['ticker'],
                'Time': f"{trade_date} Totals:",
                'Quantity': total_quantity,
                'Value': total_value
            }
            all_rows.append(summary_row)
            all_rows.extend(day_trades)
            
        return all_rows

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message('These buttons aren\'t for you.', ephemeral=True)
            return False
        return True

    def generate_image_file(self) -> discord.File:
        stripe_c = (25, 25, 25)
        txt_c = (245, 245, 245)
        summary_text_c = (229, 168, 255)

        start = self.current_page * self.per_page
        page_rows = self.processed_rows[start:start + self.per_page]

        def get_text_width(text, is_header):
            font = self.hdr_font if is_header else self.body_font
            return font.getbbox(str(text))[2]

        column_widths = {col: get_text_width(col, True) for col in self.headers}
        for row in page_rows:
            is_summary = row.get('is_summary', False)
            formatted_vals = {}
            if is_summary:
                formatted_vals = {
                    'Ticker': row['Ticker'],
                    'Time': row['Time'],
                    'Quantity': format_large_number(row['Quantity']),
                    'Value': format_large_number(row['Value']),
                    'Price': ''
                }
            else:
                formatted_vals = {
                    'Ticker': row.get('ticker',''),
                    'Quantity': format_large_number(row['quantity']),
                    'Price': f"{row['price']:.2f}",
                    'Value': format_large_number(row['trade_value']),
                    'Time': datetime.fromisoformat(row['trade_time']).strftime('%Y-%m-%d %H:%M:%S')
                }
            
            for col, val in formatted_vals.items():
                if col in column_widths:
                    column_widths[col] = max(column_widths[col], get_text_width(str(val), False))
        
        positions = {}
        x_cursor = SIDE_PADDING
        for col in self.headers:
            width = column_widths[col]
            align_right = col not in ['Ticker', 'Time']
            positions[col] = {'x': x_cursor + (width if align_right else 0), 'align_right': align_right}
            x_cursor += width + COLUMN_SPACING
        
        img_w = int(x_cursor - COLUMN_SPACING + SIDE_PADDING)
        row_h = int(self.body_font.getbbox("A")[3] + ROW_PADDING * 2)
        hdr_h = int(self.hdr_font.getbbox("A")[3] + ROW_PADDING * 2)
        img_h = int(hdr_h + len(page_rows) * row_h + BOTTOM_PADDING)

        img = Image.new('RGB', (img_w, img_h))
        draw = ImageDraw.Draw(img)

        for y in range(hdr_h):
            blend = y / hdr_h
            r, g, b = [int(self.header_top[i]*(1-blend) + self.header_bot[i]*blend) for i in range(3)]
            draw.line([(0, y), (img_w, y)], fill=(r,g,b))
        for y in range(hdr_h, img_h):
            blend = (y-hdr_h) / (img_h - hdr_h)
            r, g, b = [int(self.body_top[i]*(1-blend) + self.body_bot[i]*blend) for i in range(3)]
            draw.line([(0, y), (img_w, y)], fill=(r,g,b))
        
        y_cursor = 0
        for col in self.headers:
            pos = positions[col]
            x0 = pos['x'] - (get_text_width(col, True) if pos['align_right'] else 0)
            draw.text((x0, ROW_PADDING), col, font=self.hdr_font, fill=(255,255,255))
        y_cursor += hdr_h
        
        for i, row in enumerate(page_rows):
            is_summary = row.get('is_summary', False)
            
            if i % 2 == 1:
                draw.rectangle([(0, y_cursor), (img_w, y_cursor + row_h)], fill=stripe_c)
            
            vals = {}
            if is_summary:
                vals = {
                    'Ticker': row['Ticker'],
                    'Time': row['Time'],
                    'Quantity': format_large_number(row['Quantity']),
                    'Value': format_large_number(row['Value']),
                    'Price': ''
                }
            else:
                vals = {
                    'Ticker':   row.get('ticker',''),
                    'Quantity': format_large_number(row['quantity']),
                    'Price':    f"{row['price']:.2f}",
                    'Value':    format_large_number(row['trade_value']),
                    'Time':     datetime.fromisoformat(row['trade_time']).strftime('%Y-%m-%d %H:%M:%S')
                }

            for col in self.headers:
                txt = vals[col]
                pos = positions[col]
                x0 = pos['x'] - (get_text_width(str(txt), False) if pos['align_right'] else 0)
                font = self.body_font
                color = summary_text_c if is_summary else txt_c
                draw.text((x0, y_cursor + ROW_PADDING), str(txt), font=font, fill=color)
            y_cursor += row_h

        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename='trades.png')

    async def _update_message(self, interaction: discord.Interaction):
        self.previous_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page >= self.total_pages - 1)
        
        file = self.generate_image_file()
        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        if bot.user:
            embed.set_author(name='Deltuh DP Bot', icon_url=bot.user.display_avatar.url)
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.total_pages}")
        embed.set_image(url='attachment://trades.png')
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

    @discord.ui.button(label='◀️ Previous', style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        await self._update_message(interaction)

    @discord.ui.button(label='Next ▶️', style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        await self._update_message(interaction)

# ─── COMMAND RUNNER ──────────────────────────────────────────────
async def run_paginated_command(interaction: discord.Interaction, url: str, headers: list, title: str, show_summaries: bool = False, **kwargs):
    # FIXED: Immediate defer to prevent interaction timeout
    await interaction.response.defer(thinking=True)
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await asyncio.wait_for(client.get(url), timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        traceback.print_exc()
        await interaction.followup.send(f'❌ An API or network error occurred. Check the bot console for details.', ephemeral=True)
        return

    if not data:
        await interaction.followup.send('ℹ️ No data found.', ephemeral=True)
        return

    view = PaginatorView(data, headers, title, interaction.user, show_summaries=show_summaries, **kwargs)
    
    embed = discord.Embed(title=title, color=discord.Color(0xFF8C00))
    if bot.user:
        embed.set_author(name='Deltuh DP Bot', icon_url=bot.user.display_avatar.url)
    embed.set_footer(text=f"Page 1/{view.total_pages}")
    embed.set_image(url='attachment://trades.png')
    
    await interaction.followup.send(
        embed=embed,
        file=view.generate_image_file(),
        view=view
    )

# ─── SUPPLY/DEMAND COMMANDS ──────────────────────────────────────
class SupplyDemandCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='sd', description='Supply/Demand Level Analysis')

    @app_commands.command(description='Create a new supply/demand level')
    @app_commands.describe(
        ticker='Stock ticker symbol',
        level_price='Key price level (e.g., 17.46)',
        level_type='supply or demand',
        level_name='Optional name for the level'
    )
    async def create(
        self, 
        interaction: discord.Interaction, 
        ticker: str,
        level_price: float,
        level_type: str,
        level_name: Optional[str] = None
    ):
        if level_type.lower() not in ['supply', 'demand']:
            await interaction.response.send_message('❌ level_type must be "supply" or "demand"', ephemeral=True)
            return
            
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/create"
            payload = {
                'ticker': ticker.upper(),
                'level_price': level_price,
                'level_type': level_type.lower(),
                'level_name': level_name
            }
            
            data = await make_api_request(url, method="POST", json_data=payload, timeout_seconds=30)
            
            embed = discord.Embed(
                title=f"✅ Level Created - {ticker.upper()}",
                description=f"**Price:** ${level_price:.2f}\n**Type:** {level_type.title()}\n**Name:** {level_name or 'N/A'}",
                color=discord.Color.green()
            )
            embed.add_field(name="Level ID", value=f"`{data['level_id']}`", inline=True)
            embed.add_field(name="Next Steps", 
                          value="1. Use `/sd volume_job` to set initial volume\n2. Use `/sd absorption_job` to track absorption\n3. Use `/sd levels` to view all levels", 
                          inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to create level: {str(e)}', ephemeral=True)

    @app_commands.command(description='Start market volume analysis job')
    @app_commands.describe(
        ticker='Stock ticker symbol',
        level_price='Price level to analyze',
        start_date='Start date (YYYY-MM-DD)',
        end_date='End date (YYYY-MM-DD)',
        tolerance='Price tolerance in dollars (default $0.025)',
        level_id='Level ID to update (optional - from /sd levels)'
    )
    async def volume_job(
        self, 
        interaction: discord.Interaction, 
        ticker: str,
        level_price: float,
        start_date: str,
        end_date: str,
        tolerance: float = 0.025,
        level_id: Optional[int] = None
    ):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/market-volume-job/{ticker.upper()}"
            params = {
                'level_price': level_price,
                'start_date': start_date,
                'end_date': end_date,
                'price_tolerance': tolerance,
                'is_absorption': False
            }
            
            # Add level_id if provided
            if level_id is not None:
                params['level_id'] = level_id
            
            data = await make_api_request(url, params=params, timeout_seconds=60)
            
            job_id = data['job_id']
            
            embed = discord.Embed(
                title=f"🚀 Volume Analysis Job Started - {ticker.upper()}",
                description=f"**Level:** ${level_price:.2f}\n**Period:** {start_date} to {end_date}\n**Type:** Original Volume",
                color=discord.Color.blue()
            )
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            
            if level_id:
                embed.add_field(name="Level ID", value=f"`{level_id}`", inline=True)
                embed.add_field(name="Auto-Link", value="✅ Will update level", inline=True)
            else:
                embed.add_field(name="Auto-Link", value="🔍 Will search for matching level", inline=True)
            
            embed.add_field(name="Estimated Time", value=data.get('estimated_time', 'Unknown'), inline=True)
            embed.add_field(name="Status", value="Starting...", inline=True)
            embed.add_field(name="💡 Tip", value="Use `/sd job_status` to check progress!", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to start job: {str(e)}', ephemeral=True)

    @app_commands.command(description='Start absorption analysis job')
    @app_commands.describe(
        ticker='Stock ticker symbol',
        level_price='Price level to analyze',
        start_date='Start date (YYYY-MM-DD)',
        end_date='End date (YYYY-MM-DD)',
        tolerance='Price tolerance in dollars (default $0.025)',
        level_id='Level ID to update (REQUIRED - from /sd levels)'
    )
    async def absorption_job(
        self, 
        interaction: discord.Interaction, 
        ticker: str,
        level_price: float,
        start_date: str,
        end_date: str,
        level_id: int,
        tolerance: float = 0.025
    ):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/market-volume-job/{ticker.upper()}"
            params = {
                'level_price': level_price,
                'start_date': start_date,
                'end_date': end_date,
                'price_tolerance': tolerance,
                'is_absorption': True,
                'level_id': level_id
            }
            
            data = await make_api_request(url, params=params, timeout_seconds=60)
            
            job_id = data['job_id']
            
            embed = discord.Embed(
                title=f"🔥 Absorption Analysis Job Started - {ticker.upper()}",
                description=f"**Level:** ${level_price:.2f}\n**Period:** {start_date} to {end_date}\n**Type:** Absorption Volume",
                color=discord.Color.orange()
            )
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            embed.add_field(name="Level ID", value=f"`{level_id}`", inline=True)
            embed.add_field(name="Analysis Type", value="🔥 Absorption", inline=True)
            embed.add_field(name="Estimated Time", value=data.get('estimated_time', 'Unknown'), inline=True)
            embed.add_field(name="Status", value="Starting...", inline=True)
            embed.add_field(name="⚠️ Note", value="Level must have original volume data first!", inline=False)
            embed.add_field(name="💡 Tip", value="Use `/sd job_status` to check progress!", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to start absorption job: {str(e)}', ephemeral=True)

    @app_commands.command(description='Check status of a background job')
    @app_commands.describe(job_id='Job ID from the volume_job or absorption_job command')
    async def job_status(self, interaction: discord.Interaction, job_id: str):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/jobs/{job_id}/status"
            data = await make_api_request(url, timeout_seconds=30)
            
            status = data.get('status', 'unknown')
            progress = data.get('progress', 0)
            analysis_type = data.get('analysis_type', 'volume')
            
            if status == 'completed':
                result = data.get('result', {})
                market_data = result.get('market_data', {})
                
                # Set color and emoji based on analysis type
                if analysis_type == 'absorption':
                    color = discord.Color.orange()
                    title_emoji = "🔥"
                    title = f"{title_emoji} Absorption Job Completed"
                else:
                    color = discord.Color.green()
                    title_emoji = "✅"
                    title = f"{title_emoji} Volume Job Completed"
                
                embed = discord.Embed(title=title, color=color)
                embed.add_field(name="📈 Volume", value=format_large_number(market_data.get('total_volume', 0)), inline=True)
                embed.add_field(name="💰 Value", value=format_large_number(market_data.get('total_value', 0)), inline=True)
                embed.add_field(name="🔢 Trades", value=str(market_data.get('total_trades', 0)), inline=True)
                embed.add_field(name="🎯 Price Range", value=market_data.get('price_range', 'N/A'), inline=True)
                embed.add_field(name="📡 API Calls", value=str(market_data.get('api_calls_made', 0)), inline=True)
                embed.add_field(name="📊 Analysis Type", value=analysis_type.title(), inline=True)
                
                # Show level linking status
                if market_data.get('level_updated'):
                    embed.add_field(name="🔗 Level Update", value=f"✅ Level {market_data.get('level_id')} updated", inline=True)
                else:
                    embed.add_field(name="🔗 Level Update", value="❌ No matching level found", inline=True)
                
            elif status == 'failed':
                embed = discord.Embed(
                    title=f"❌ {analysis_type.title()} Job Failed",
                    description=data.get('error', 'Unknown error'),
                    color=discord.Color.red()
                )
                
            else:
                # Set color based on analysis type for in-progress jobs
                if analysis_type == 'absorption':
                    color = discord.Color.orange()
                    title = f"🔥 Absorption Job In Progress"
                else:
                    color = discord.Color.blue()
                    title = f"🔄 Volume Job In Progress"
                
                embed = discord.Embed(title=title, color=color)
                embed.add_field(name="Status", value=status.title(), inline=True)
                embed.add_field(name="Progress", value=f"{progress}%", inline=True)
                embed.add_field(name="Analysis Type", value=analysis_type.title(), inline=True)
                
                # Progress bar
                filled = int(progress / 5)  # 20 segments
                empty = 20 - filled
                progress_bar = "█" * filled + "░" * empty
                embed.add_field(name="Progress Bar", value=f"`{progress_bar}` {progress}%", inline=False)
            
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to check job status: {str(e)}', ephemeral=True)

    @app_commands.command(description='List all levels for a ticker')
    @app_commands.describe(ticker='Stock ticker symbol')
    async def levels(self, interaction: discord.Interaction, ticker: str):
        # FIXED: Immediate defer to prevent interaction timeout
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/{ticker.upper()}"
            # FIXED: Reduced timeout to prevent hanging
            data = await make_api_request(url, timeout_seconds=15)
            
            if not data:
                await interaction.followup.send(f'ℹ️ No levels found for {ticker.upper()}', ephemeral=True)
                return
            
            embed = discord.Embed(
                title=f"📊 S/D Levels - {ticker.upper()}",
                color=discord.Color.gold()
            )
            
            for level_summary in data[:10]:  # Show first 10 levels
                level = level_summary['level']
                absorption = level_summary['absorption']
                
                level_name = f"ID:{level['id']} | ${level['level_price']:.2f} ({level['level_type'].title()})"
                if level['level_name']:
                    level_name += f" - {level['level_name']}"
                
                volume_info = f"Original: {format_large_number(absorption['original_volume'])}\n"
                volume_info += f"Absorbed: {format_large_number(absorption['absorbed_volume'])}\n"
                volume_info += f"Absorption: {absorption['absorption_percentage']:.1f}%"
                
                # Add status indicator
                if absorption['original_volume'] > 0 and absorption['absorbed_volume'] > 0:
                    volume_info += f"\n🔥 Active"
                elif absorption['original_volume'] > 0:
                    volume_info += f"\n⚡ Ready for absorption analysis"
                else:
                    volume_info += f"\n⏳ Needs volume data"
                
                embed.add_field(name=level_name, value=volume_info, inline=True)
            
            if len(data) > 10:
                embed.set_footer(text=f"Showing 10 of {len(data)} levels")
            
            # Add helpful instructions
            embed.add_field(
                name="📖 How to Use",
                value="1. `/sd volume_job` - Set original volume\n2. `/sd absorption_job` - Track absorption\n3. Use Level ID numbers in commands",
                inline=False
            )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            # FIXED: Better error handling for timeout issues
            if "timeout" in str(e).lower():
                await interaction.followup.send(f'❌ Request timed out. The API may be slow. Try again in a moment.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to get levels: {str(e)}', ephemeral=True)

    @app_commands.command(description='Link a completed job to a level (for retroactive updates)')
    @app_commands.describe(
        job_id='Job ID from a completed volume job',
        level_id='Level ID to link the job to'
    )
    async def link_job(self, interaction: discord.Interaction, job_id: str, level_id: int):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/jobs/{job_id}/link-to-level"
            payload = {'level_id': level_id}
            
            data = await make_api_request(url, method="POST", json_data=payload, timeout_seconds=30)
            
            volume_data = data.get('volume_data', {})
            analysis_type = data.get('analysis_type', 'volume')
            
            # Set color and emoji based on analysis type
            if analysis_type == 'absorption':
                color = discord.Color.orange()
                title_emoji = "🔥"
            else:
                color = discord.Color.green()
                title_emoji = "🔗"
            
            embed = discord.Embed(
                title=f"{title_emoji} Job Linked Successfully",
                description=f"Job `{job_id}` has been linked to Level `{level_id}` as **{analysis_type}** data",
                color=color
            )
            
            embed.add_field(name="📈 Volume", value=format_large_number(volume_data.get('total_volume', 0)), inline=True)
            embed.add_field(name="💰 Value", value=format_large_number(volume_data.get('total_value', 0)), inline=True)
            embed.add_field(name="🔢 Trades", value=str(volume_data.get('total_trades', 0)), inline=True)
            embed.add_field(name="🎯 Price Range", value=volume_data.get('price_range', 'N/A'), inline=False)
            embed.add_field(name="📊 Analysis Type", value=analysis_type.title(), inline=True)
            
            embed.add_field(name="✅ Next Steps", value="Use `/sd levels` to see updated data", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to link job: {str(e)}', ephemeral=True)

# ─── DARK POOL COMMANDS ──────────────────────────────────────────
class DarkPoolCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='dp', description='Dark Pool & Block trades')

    @app_commands.command(description='Block trades off-exchange')
    @app_commands.guilds(TEST_GUILD)
    async def allblocks(self, interaction: discord.Interaction, ticker: str):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/dp/allblocks/{ticker.upper()}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Block Trades for {ticker.upper()}",
            show_summaries=True
        )

    @app_commands.command(description='Block trades during market hours')
    @app_commands.guilds(TEST_GUILD)
    async def alldp(self, interaction: discord.Interaction, ticker: str):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/dp/alldp/{ticker.upper()}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Dark-Pool Trades for {ticker.upper()}",
            show_summaries=False 
        )

    @app_commands.command(description='Top block trades by value')
    @app_commands.describe(
        days='Days back (1–30)',
        market_hours_only='Only show trades during market hours (9:30 AM - 4:00 PM ET)'
    )
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(
        self, 
        interaction: discord.Interaction, 
        days: app_commands.Range[int,1,30] = 1,
        market_hours_only: bool = False
    ):
        url = f"{API_BASE_URL}/dp/bigprints?days={days}"
        if market_hours_only:
            url += "&market_hours_only=true"
        
        title = f"Biggest DP Prints Last {days} Day{'s' if days>1 else ''}"
        if market_hours_only:
            title += " (Market Hours Only)"
            
        await run_paginated_command(
            interaction, url,
            ['Ticker','Quantity','Price','Value','Time'],
            title,
            show_summaries=False
        )

# ─── LIT COMMANDS ─────────────────────────────────────────────────
class LitCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='lit', description='Lit market trades')

    @app_commands.command(name='all', description='All lit-market trades for a ticker')
    @app_commands.guilds(TEST_GUILD)
    async def all_trades(self, interaction: discord.Interaction, ticker: str):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/lit/all/{ticker.upper()}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Lit Market Trades for {ticker.upper()}",
            show_summaries=True,
            header_gradient=((97, 138, 250), (0, 68, 255))
        )

    @app_commands.command(name='bigprints', description='Top lit trades by value over the last X days')
    @app_commands.describe(
        days='Days back (1–30)',
        under_400m='Filter for trades under $400M',
        market_hours_only='Only show trades during market hours (9:30 AM - 4:00 PM ET)'
    )
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(
        self, 
        interaction: discord.Interaction, 
        days: app_commands.Range[int, 1, 30] = 1, 
        under_400m: bool = False,
        market_hours_only: bool = False
    ):
        url = f"{API_BASE_URL}/lit/bigprints?days={days}"
        if under_400m:
            url += "&under_400m=true"
        if market_hours_only:
            url += "&market_hours_only=true"
            
        title = f"Biggest Lit Prints Last {days} Day{'s' if days > 1 else ''}"
        if under_400m:
            title += " (Under $400M)"
        if market_hours_only:
            title += " (Market Hours Only)"
            
        await run_paginated_command(
            interaction, url, 
            ['Ticker','Quantity','Price','Value','Time'], 
            title,
            show_summaries=False,
            header_gradient=((97, 138, 250), (0, 68, 255))
        )

# ─── BOT LIFECYCLE ───────────────────────────────────────────────
@bot.event
async def on_ready():
    if bot.user:
        print(f'✅ Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')
    
    # Test unified API connection
    try:
        health_data = await make_api_request(f"{API_BASE_URL}/health", timeout_seconds=10)
        print(f"✅ Unified API Health: {health_data}")
    except Exception as e:
        print(f"⚠️  Unified API Health Check Failed: {str(e)}")
    
    try:
        synced = await bot.tree.sync()
        print(f"✅ Commands synced successfully: {len(synced)} commands")
    except Exception as e:
        print(f"🔴 Command sync failed: {e}")

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"Command error: {error}")
    
    if isinstance(error, app_commands.CommandOnCooldown):
        try:
            await interaction.response.send_message(
                f"Command is on cooldown. Try again in {error.retry_after:.2f} seconds.",
                ephemeral=True
            )
        except:
            pass
    elif isinstance(error, app_commands.MissingPermissions):
        try:
            await interaction.response.send_message(
                "❌ Missing permissions to execute this command.",
                ephemeral=True
            )
        except:
            pass
    else:
        try:
            # FIXED: Better handling for interaction timeout errors
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An unexpected error occurred. This may be due to an interaction timeout. Try the command again.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ An unexpected error occurred. This may be due to an interaction timeout. Try the command again.",
                    ephemeral=True
                )
        except Exception as followup_error:
            print(f"Could not send error message: {followup_error}")
        
        # Log the full error for debugging
        print("Full error traceback:")
        traceback.print_exc()

if __name__ == '__main__':
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError('DISCORD_BOT_TOKEN not set in environment')
    
    bot.tree.add_command(DarkPoolCommands())
    bot.tree.add_command(LitCommands())
    bot.tree.add_command(SupplyDemandCommands())
    bot.run(DISCORD_BOT_TOKEN)
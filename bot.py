#!/usr/bin/env python3
"""
UNIFIED BOT - Enhanced with Segmented Timeline Visualization
ENHANCED VERSION - Custom colors for supply/demand, segmented absorption bars, unlimited API support
INCLUDES: Segmented job visualization with proper date ranges and customizable colors
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
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Union, List, Dict

# Matplotlib imports for visualization with proper error handling
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from matplotlib.patches import Rectangle
    from matplotlib.lines import Line2D
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    print("Warning: matplotlib and/or numpy not available. Visualization features will be disabled.")
    MATPLOTLIB_AVAILABLE = False
    # Create dummy modules to prevent runtime errors
    class DummyModule:
        def __getattr__(self, name):
            def dummy_func(*args, **kwargs):
                raise ImportError(f"matplotlib/numpy not available - install with: pip install matplotlib numpy")
            return dummy_func
        
        def __call__(self, *args, **kwargs):
            raise ImportError(f"matplotlib/numpy not available - install with: pip install matplotlib numpy")
    
    plt = DummyModule()
    patches = DummyModule()
    np = DummyModule()
    Rectangle = DummyModule()
    Line2D = DummyModule()

# ─── INVITE & GUILD CONFIG ───────────────────────────────────────
TEST_GUILD_ID = 1218555075098841088  # ← replace with your test server ID
TEST_GUILD    = discord.Object(id=TEST_GUILD_ID)

# ─── IMAGE & FONT CONFIG ─────────────────────────────────────────
SIDE_PADDING      = 50
COLUMN_SPACING    = 75
HEADER_FONT_SIZE = 30
BODY_FONT_SIZE    = 50
ROW_PADDING       = 10
BOTTOM_PADDING    = 40

# ─── BOT & API CONFIGURATION ────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN') or ''
API_BASE_URL      = 'http://127.0.0.1:8001'  # Single unified API

# ─── ENHANCED COLOR CONFIGURATION ───────────────────────────────
# Supply colors (red tones)
SUPPLY_ORIGINAL_COLOR = "#8B0000"    # Dark red for supply original volume
SUPPLY_ABSORBED_COLOR = "#FF4500"    # Orange-red for supply absorbed volume
SUPPLY_SEGMENTS_COLORS = [           # Different shades for supply segments
    "#DC143C",  # Crimson
    "#B22222",  # Fire brick
    "#CD5C5C",  # Indian red
    "#F08080",  # Light coral
    "#FA8072",  # Salmon
    "#E9967A",  # Dark salmon
    "#FF6347",  # Tomato
    "#FF7F50"   # Coral
]

# Demand colors (green/blue tones)
DEMAND_ORIGINAL_COLOR = "#006400"    # Dark green for demand original volume
DEMAND_ABSORBED_COLOR = "#32CD32"    # Lime green for demand absorbed volume
DEMAND_SEGMENTS_COLORS = [           # Different shades for demand segments
    "#228B22",  # Forest green
    "#00FF00",  # Lime
    "#7FFF00",  # Chartreuse
    "#00FF7F",  # Spring green
    "#00CED1",  # Dark turquoise
    "#20B2AA",  # Light sea green
    "#48D1CC",  # Medium turquoise
    "#40E0D0"   # Turquoise
]

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
            elif method.upper() == "DELETE":
                response = await client.delete(url, params=params or {})
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
    def __init__(self, trades, headers, title, author: Union[discord.User, discord.Member], show_summaries=False, **kwargs):
        super().__init__(timeout=180)
        
        if show_summaries:
            self.processed_rows = self._process_trades_with_summaries(trades)
        else:
            self.processed_rows = trades
        
        self.headers        = headers
        self.title          = title
        self.author         = author
        self.current_page = 0
        self.per_page       = 15
        self.total_pages    = math.ceil(len(self.processed_rows) / self.per_page)
        
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
        user = interaction.user
        if user is None:
            try:
                await interaction.response.send_message('User information not available.', ephemeral=True)
            except:
                pass
            return False
        
        if user.id != self.author.id:
            try:
                await interaction.response.send_message('These buttons aren\'t for you.', ephemeral=True)
            except:
                pass
            return False
        return True

    def generate_image_file(self) -> discord.File:
        stripe_c = (25, 25, 25)
        txt_c = (245, 245, 245)
        summary_text_c = (255, 255, 0)

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
        if bot.user is not None:
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

# ─── ENHANCED TIMELINE ABSORPTION VISUALIZER CLASS ────────────────────────────────────
class TimelineAbsorptionView(discord.ui.View):
    def __init__(self, ticker: str, levels_data: dict, author: Union[discord.User, discord.Member]):
        super().__init__(timeout=300)
        self.ticker = ticker.upper()
        self.levels_data = levels_data
        self.author = author
        self.current_view = 'all'  # 'all', 'supply', 'demand'
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        user = interaction.user
        if user is None:
            try:
                await interaction.response.send_message('User information not available.', ephemeral=True)
            except:
                pass
            return False
        
        if user.id != self.author.id:
            try:
                await interaction.response.send_message('These controls are not for you.', ephemeral=True)
            except:
                pass
            return False
        return True
    
    def generate_timeline_visualization(self) -> discord.File:
        """Generate Enhanced Segmented Timeline Absorption View with custom colors"""
        if not MATPLOTLIB_AVAILABLE:
            return self._create_fallback_image()
        
        levels = self.levels_data['levels']
        
        # Filter levels based on view
        if self.current_view == 'supply':
            levels = [l for l in levels if l['level']['level_type'] == 'supply']
        elif self.current_view == 'demand':
            levels = [l for l in levels if l['level']['level_type'] == 'demand']
        
        if not levels:
            return self._create_empty_state_image()
        
        # Set up dark theme
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(18, 12))
        
        # Filter levels with volume data for timeline
        levels_with_data = [l for l in levels if l['volume']['original_volume'] > 0]
        
        if not levels_with_data:
            return self._create_no_data_image()
        
        # Create enhanced segmented timeline view
        self._draw_segmented_timeline_chart(ax, levels_with_data)
        
        plt.tight_layout()
        
        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                    facecolor='#2f3136', edgecolor='none')
        buf.seek(0)
        plt.close()
        
        return discord.File(fp=buf, filename=f'{self.ticker.lower()}_segmented_timeline.png')
    
    def _draw_segmented_timeline_chart(self, ax, levels_with_data):
        """Draw the enhanced segmented volume comparison bar chart"""
        # Sort levels by type (demand first, then supply) and then by price within each type
        levels_with_data.sort(key=lambda x: (x['level']['level_type'] == 'supply', -x['level']['level_price']))
        
        # Prepare data for segmented bar chart
        level_names = []
        original_volumes = []
        level_types = []
        job_segments_by_level = []
        
        # Debug output
        print(f"🔍 Drawing chart for {len(levels_with_data)} levels")
        
        for level_data in levels_with_data:
            level = level_data['level']
            volume = level_data['volume']
            job_segments = level_data.get('job_segments', [])
            
            level_name = level['level_name'] or f"L{level['id']}"
            price = level['level_price']
            level_names.append(f"{level_name}\n${price:.2f}")
            
            original_volumes.append(volume['original_volume'])
            level_types.append(level['level_type'])
            job_segments_by_level.append(job_segments)
            
            # Debug output
            print(f"📊 Level {level['id']}: {level['level_type']} at ${price:.2f}")
            print(f"   Original Volume: {volume['original_volume']:,}")
            print(f"   Absorbed Volume: {volume['absorbed_volume']:,}")
            print(f"   Job Segments: {len(job_segments)}")
            if job_segments:
                for seg in job_segments:
                    print(f"     Segment: {seg['volume']:,} volume from {seg['date_start']} to {seg['date_end']}")
        
        if not level_names:
            ax.text(0.5, 0.5, 'No volume data available\nRun volume jobs first', 
                    ha='center', va='center', fontsize=16, color='white', transform=ax.transAxes)
            ax.axis('off')
            return
        
        # Set up positions for segmented bars
        y_pos = range(len(level_names))
        bar_height = 0.5  # Reduced for better spacing
        segment_bar_height = bar_height * 0.75  # Increased proportion but still smaller than original
        
        # Track max volume for scaling
        max_volume = max(original_volumes) if original_volumes else 1
        print(f"📈 Max volume for scaling: {max_volume:,}")
        
        # Draw original volume bars first (background)
        for i, (orig_vol, level_type) in enumerate(zip(original_volumes, level_types)):
            if orig_vol > 0:
                # Choose color based on level type
                original_color = SUPPLY_ORIGINAL_COLOR if level_type == 'supply' else DEMAND_ORIGINAL_COLOR
                
                # Draw original volume bar
                ax.barh(i, orig_vol, bar_height, 
                       label='Original Volume' if i == 0 else "", 
                       color=original_color, alpha=0.6, zorder=1)
                
                # Add original volume label - positioned to avoid overlap with absorption bars
                label_y_offset = bar_height * 0.4  # Position above the center to avoid absorption bar overlap
                ax.text(orig_vol + max_volume * 0.01, i + label_y_offset, 
                       format_large_number(orig_vol), va='center', ha='left', 
                       fontsize=10, color='white', weight='bold', zorder=5)
        
        # Draw segmented absorption bars
        for i, (job_segments, level_type, level_data) in enumerate(zip(job_segments_by_level, level_types, levels_with_data)):
            absorbed_volume = level_data['volume']['absorbed_volume']
            
            if not job_segments and absorbed_volume > 0:
                # Fallback: show simple absorbed bar if no segments but has absorbed volume
                print(f"⚠️ No segments for level {i}, showing simple absorbed bar: {absorbed_volume:,}")
                absorbed_color = SUPPLY_ABSORBED_COLOR if level_type == 'supply' else DEMAND_ABSORBED_COLOR
                
                # Position absorption bar below center
                absorption_y = i - segment_bar_height/3
                
                ax.barh(absorption_y, absorbed_volume, segment_bar_height/2, 
                       color=absorbed_color, alpha=0.9, zorder=3,
                       edgecolor='white', linewidth=0.5)
                
                # Add absorbed volume label - positioned close to the bar
                volume_label_x = absorbed_volume + max_volume * 0.005
                ax.text(volume_label_x, absorption_y,
                       format_large_number(absorbed_volume), va='center', ha='left', 
                       fontsize=9, color='white', weight='bold', zorder=5)
                       
                # Add absorption percentage - positioned with proper spacing after volume text
                if original_volumes[i] > 0:
                    absorption_pct = (absorbed_volume / original_volumes[i]) * 100
                    # Calculate better spacing based on volume text length and font size
                    volume_text = format_large_number(absorbed_volume)
                    # Estimate text width more accurately (approximately 6 pixels per character for this font size)
                    estimated_text_width = len(volume_text) * 6
                    # Convert pixels to data coordinates (rough approximation)
                    text_width_data = (estimated_text_width / 1000) * max_volume
                    
                    percentage_x = volume_label_x + text_width_data + max_volume * 0.01
                    ax.text(percentage_x, absorption_y,
                           f"({absorption_pct:.1f}%)", va='center', ha='left', 
                           fontsize=8, color='yellow', weight='bold', zorder=5)
                continue
                
            if not job_segments:
                print(f"⚠️ No segments and no absorbed volume for level {i}")
                continue
                
            # Sort segments by date
            sorted_segments = sorted(job_segments, key=lambda x: x['date_start'])
            print(f"📊 Drawing {len(sorted_segments)} segments for level {i}")
            
            # Choose colors based on level type
            if level_type == 'supply':
                segment_colors = SUPPLY_SEGMENTS_COLORS
            else:
                segment_colors = DEMAND_SEGMENTS_COLORS
            
            # Calculate segment positions for proper stacking
            segment_height = segment_bar_height / max(len(sorted_segments), 1)
            total_absorbed = sum(seg['volume'] for seg in sorted_segments)
            
            # Position segments below center to avoid original volume text
            base_y = i - segment_bar_height/2
            
            for seg_idx, segment in enumerate(sorted_segments):
                segment_volume = segment['volume']
                date_start = segment['date_start']
                date_end = segment['date_end']
                
                print(f"   Drawing segment {seg_idx}: {segment_volume:,} volume")
                
                # Use different color for each segment
                color_idx = seg_idx % len(segment_colors)
                segment_color = segment_colors[color_idx]
                
                # Calculate y position for stacked segments
                segment_y = base_y + (seg_idx * segment_height)
                
                # Draw individual segment bar
                ax.barh(segment_y, segment_volume, segment_height,
                       color=segment_color, alpha=0.9, zorder=3,
                       edgecolor='white', linewidth=0.5)
                
                # Add segment volume label
                if segment_volume > max_volume * 0.02:  # Show labels for segments > 2% of max
                    label_x = segment_volume / 2
                    ax.text(label_x, segment_y, format_large_number(segment_volume),
                           va='center', ha='center', fontsize=8, color='white', 
                           weight='bold', zorder=4)
                
                # Add date range label to the right of each segment
                ax.text(segment_volume + max_volume * 0.005, segment_y, 
                       f"{date_start} → {date_end}", va='center', ha='left', 
                       fontsize=7, color='#cccccc', style='italic', zorder=4)
            
            # Add total absorption summary - positioned to avoid overlap
            if total_absorbed > 0 and original_volumes[i] > 0:
                absorption_pct = (total_absorbed / original_volumes[i]) * 100
                max_segment_volume = max(seg['volume'] for seg in sorted_segments) if sorted_segments else 0
                
                # Position total summary at the end of the longest segment
                summary_y = base_y + (len(sorted_segments) - 1) * segment_height / 2  # Middle of segments
                
                ax.text(max_segment_volume + max_volume * 0.01, summary_y, 
                       f"Total: {format_large_number(total_absorbed)} ({absorption_pct:.1f}%)", 
                       va='center', ha='left', fontsize=9, color='yellow', 
                       weight='bold', zorder=5)
        
        # Styling
        ax.set_yticks(y_pos)
        ax.set_yticklabels(level_names)
        ax.set_xlabel('Volume', fontsize=14, color='white', weight='bold')
        ax.set_ylabel('Price Levels', fontsize=14, color='white', weight='bold')
        
        view_suffix = ""
        if self.current_view != 'all':
            view_suffix = f" - {self.current_view.title()} Only"
        
        ax.set_title(f'{self.ticker} Segmented Volume Absorption Timeline{view_suffix}', 
                    fontsize=18, color='white', pad=30, weight='bold')
        
        # Format ticks
        ax.tick_params(axis='both', colors='white', labelsize=11)
        
        # Enhanced grid
        ax.grid(True, alpha=0.3, axis='x', linestyle='--')
        
        # Custom legend for segmented view
        legend_elements = [
            Line2D([0], [0], color=SUPPLY_ORIGINAL_COLOR, lw=8, alpha=0.6, label='Supply Original'),
            Line2D([0], [0], color=DEMAND_ORIGINAL_COLOR, lw=8, alpha=0.6, label='Demand Original'),
            Line2D([0], [0], color=SUPPLY_SEGMENTS_COLORS[0], lw=8, alpha=0.9, label='Supply Absorption'),
            Line2D([0], [0], color=DEMAND_SEGMENTS_COLORS[0], lw=8, alpha=0.9, label='Demand Absorption')
        ]
        ax.legend(handles=legend_elements, loc='upper right', framealpha=0.9, fontsize=10)
        
        # Remove top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('white')
        ax.spines['left'].set_color('white')
        
        # Auto-scale to fit all data with some padding
        all_volumes = original_volumes + [sum(seg['volume'] for seg in job_segs) for job_segs in job_segments_by_level]
        if all_volumes:
            ax.set_xlim(0, max(all_volumes) * 1.4)
        
        # Add informational text
        ax.text(0.02, 0.98, 'Each absorption segment shows date range and volume\nSegments are colored by job execution order', 
               transform=ax.transAxes, va='top', ha='left', fontsize=10, 
               color='#cccccc', style='italic')
    
    def _create_fallback_image(self) -> discord.File:
        """Create fallback when matplotlib not available"""
        img = Image.new('RGB', (800, 600), color=(45, 45, 45))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype('arial.ttf', 24)
        except OSError:
            font = ImageFont.load_default()
        
        draw.text((50, 50), f'{self.ticker} Segmented Timeline Absorption View', font=font, fill=(255, 255, 255))
        draw.text((50, 100), 'Visualization requires matplotlib and numpy', font=font, fill=(255, 100, 100))
        draw.text((50, 150), 'Install with: pip install matplotlib numpy', font=font, fill=(200, 200, 200))
        
        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename=f'{self.ticker.lower()}_timeline_fallback.png')
    
    def _create_empty_state_image(self) -> discord.File:
        """Create image for when no levels match filter"""
        img = Image.new('RGB', (800, 400), color=(45, 45, 45))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype('arial.ttf', 20)
        except OSError:
            font = ImageFont.load_default()
        
        filter_text = f" ({self.current_view} levels)" if self.current_view != 'all' else ""
        draw.text((50, 50), f'{self.ticker} - No Levels Found{filter_text}', font=font, fill=(255, 255, 255))
        draw.text((50, 100), 'Create levels with /sd create', font=font, fill=(200, 200, 200))
        
        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename=f'{self.ticker.lower()}_empty.png')
    
    def _create_no_data_image(self) -> discord.File:
        """Create image for when levels exist but have no volume data"""
        img = Image.new('RGB', (800, 400), color=(45, 45, 45))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype('arial.ttf', 20)
        except OSError:
            font = ImageFont.load_default()
        
        draw.text((50, 50), f'{self.ticker} - Levels Found but No Volume Data', font=font, fill=(255, 255, 255))
        draw.text((50, 100), 'Run /sd enhanced_volume_job and /sd enhanced_absorption_job first', font=font, fill=(200, 200, 200))
        draw.text((50, 150), 'to see segmented timeline absorption visualization', font=font, fill=(200, 200, 200))
        
        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename=f'{self.ticker.lower()}_no_data.png')
    
    @discord.ui.button(label='📊 All Levels', style=discord.ButtonStyle.primary)
    async def all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_view = 'all'
        await self._update_visualization(interaction)
    
    @discord.ui.button(label='🔴 Supply Only', style=discord.ButtonStyle.danger)
    async def supply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_view = 'supply'
        await self._update_visualization(interaction)
    
    @discord.ui.button(label='🟢 Demand Only', style=discord.ButtonStyle.success)
    async def demand_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_view = 'demand'
        await self._update_visualization(interaction)
    
    @discord.ui.button(label='🔄 Refresh', style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        
        try:
            # Fetch fresh data
            url = f"{API_BASE_URL}/levels/{self.ticker}/enhanced-timeline"
            self.levels_data = await make_api_request(url, timeout_seconds=20)
            await self._update_visualization(interaction, deferred=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to refresh data: {str(e)}", ephemeral=True)
    
    async def _update_visualization(self, interaction: discord.Interaction, deferred: bool = False):
        """Update the segmented timeline visualization"""
        file = self.generate_timeline_visualization()
        
        # Create embed with current view info
        view_names = {
            'all': '📊 All Levels',
            'supply': '🔴 Supply Levels Only', 
            'demand': '🟢 Demand Levels Only'
        }
        
        embed = discord.Embed(
            title=f"{self.ticker} Segmented Volume Absorption Timeline",
            description=f"View: {view_names[self.current_view]} | Shows absorption segments with date ranges and custom colors",
            color=discord.Color.blue()
        )
        
        # Enhanced stats
        level_count = self.levels_data['level_count']
        supply_count = self.levels_data['supply_count']
        demand_count = self.levels_data['demand_count']
        
        if self.current_view != 'all':
            current_count = supply_count if self.current_view == 'supply' else demand_count
            embed.add_field(
                name="Levels in View",
                value=f"**Showing:** {current_count} {self.current_view} levels\n**Total:** {level_count} levels ({supply_count} supply, {demand_count} demand)",
                inline=False
            )
        else:
            embed.add_field(
                name="All Levels",
                value=f"**Total:** {level_count} levels\n**Supply:** {supply_count} (red tones) | **Demand:** {demand_count} (green/blue tones)",
                inline=False
            )
        
        embed.add_field(
            name="🎨 Enhanced Features",
            value="• **Custom Colors:** Supply (red tones) vs Demand (green/blue tones)\n• **Segmented Bars:** Each job shown as separate segment\n• **Date Ranges:** Full date span displayed for each segment\n• **Unlimited API:** Maximum data coverage",
            inline=False
        )
        
        embed.set_image(url=f'attachment://{self.ticker.lower()}_segmented_timeline.png')
        embed.set_footer(text="Segmented timeline shows each absorption job as colored segments with date ranges")
        
        if deferred:
            try:
                original_message = await interaction.original_response()
                await original_message.edit(embed=embed, attachments=[file], view=self)
            except discord.NotFound:
                await interaction.followup.send(embed=embed, file=file, view=self)
        else:
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

# ─── COMMAND RUNNER ──────────────────────────────────────────────
async def run_paginated_command(interaction: discord.Interaction, url: str, headers: list, title: str, show_summaries: bool = False, **kwargs):
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

    user = interaction.user
    if user is None:
        await interaction.followup.send('❌ Unable to identify user.', ephemeral=True)
        return

    view = PaginatorView(data, headers, title, user, show_summaries=show_summaries, **kwargs)
    
    embed = discord.Embed(title=title, color=discord.Color(0xFF8C00))
    if bot.user is not None:
        embed.set_author(name='Deltuh DP Bot', icon_url=bot.user.display_avatar.url)
    embed.set_footer(text=f"Page 1/{view.total_pages}")
    embed.set_image(url='attachment://trades.png')
    
    await interaction.followup.send(
        embed=embed,
        file=view.generate_image_file(),
        view=view
    )

# ─── ENHANCED SUPPLY/DEMAND COMMANDS ──────────────────────────────
class SupplyDemandCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='sd', description='Enhanced Supply/Demand Level Analysis with Unlimited API & Segmented Timeline')

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
            
            # Color the embed based on level type
            color = discord.Color.red() if level_type.lower() == 'supply' else discord.Color.green()
            
            embed = discord.Embed(
                title=f"✅ {level_type.title()} Level Created - {ticker.upper()}",
                description=f"**Price:** ${level_price:.2f}\n**Type:** {level_type.title()}\n**Name:** {level_name or 'N/A'}",
                color=color
            )
            embed.add_field(name="Level ID", value=f"`{data['level_id']}`", inline=True)
            embed.add_field(name="Enhanced Features", 
                            value="✅ Unlimited API support\n✅ Segmented absorption tracking\n✅ Custom color visualization", 
                            inline=True)
            embed.add_field(name="Next Steps", 
                            value="1. Use `/sd enhanced_volume_job` to set initial volume\n2. Use `/sd enhanced_absorption_job` to track absorption\n3. Use `/sd timeline` to see segmented timeline", 
                            inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to create level: {str(e)}', ephemeral=True)

    @app_commands.command(description='🚀 Enhanced market volume analysis with unlimited API calls')
    @app_commands.describe(
        ticker='Stock ticker symbol',
        level_price='Price level to analyze',
        start_date='Start date (YYYY-MM-DD)',
        end_date='End date (YYYY-MM-DD)',
        tolerance='Price tolerance in dollars (default $0.025)',
        level_id='Level ID to update (optional - from /sd levels)'
    )
    async def enhanced_volume_job(
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
            url = f"{API_BASE_URL}/market-volume-job-enhanced/{ticker.upper()}"
            params = {
                'level_price': level_price,
                'start_date': start_date,
                'end_date': end_date,
                'price_tolerance': tolerance,
                'is_absorption': False
            }
            
            if level_id is not None:
                params['level_id'] = level_id
            
            data = await make_api_request(url, params=params, timeout_seconds=60)
            
            job_id = data['job_id']
            
            embed = discord.Embed(
                title=f"🚀 Enhanced Volume Analysis Started - {ticker.upper()}",
                description=f"**Level:** ${level_price:.2f}\n**Period:** {start_date} to {end_date}\n**Type:** Original Volume (Enhanced)",
                color=discord.Color.blue()
            )
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            
            if level_id:
                embed.add_field(name="Level ID", value=f"`{level_id}`", inline=True)
                embed.add_field(name="Auto-Link", value="✅ Will update level", inline=True)
            else:
                embed.add_field(name="Auto-Link", value="🔍 Will search for matching level", inline=True)
            
            embed.add_field(name="🚀 Enhanced Features", 
                            value="• **Unlimited API Calls** - Maximum data coverage\n• **Fast Processing** - Optimized algorithm\n• **Automatic Segmentation** - Ready for timeline view", 
                            inline=False)
            embed.add_field(name="Estimated Time", value=data.get('estimated_time', 'Unknown'), inline=True)
            embed.add_field(name="Status", value="Starting Enhanced Processing...", inline=True)
            embed.add_field(name="💡 Tip", value="Use `/sd job_status` to check progress!", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to start enhanced volume job: {str(e)}', ephemeral=True)

    @app_commands.command(description='🔥 Enhanced absorption analysis with unlimited API & segmented tracking')
    @app_commands.describe(
        ticker='Stock ticker symbol',
        level_price='Price level to analyze',
        start_date='Start date (YYYY-MM-DD)',
        end_date='End date (YYYY-MM-DD) - This will be the absorption completion date shown',
        level_id='Level ID to update (REQUIRED - from /sd levels)',
        tolerance='Price tolerance in dollars (default $0.025)'
    )
    async def enhanced_absorption_job(
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
            url = f"{API_BASE_URL}/market-volume-job-enhanced/{ticker.upper()}"
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
                title=f"🔥 Enhanced Absorption Analysis Started - {ticker.upper()}",
                description=f"**Level:** ${level_price:.2f}\n**Period:** {start_date} to {end_date}\n**Type:** Absorption Volume (Enhanced)",
                color=discord.Color.orange()
            )
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            embed.add_field(name="Level ID", value=f"`{level_id}`", inline=True)
            embed.add_field(name="Absorption End Date", value=f"**{end_date}** (will be displayed in timeline)", inline=True)
            embed.add_field(name="🔥 Enhanced Features", 
                            value="• **Unlimited API Calls** - Complete data coverage\n• **Segmented Tracking** - Creates timeline segments\n• **Custom Colors** - Supply/demand color coding\n• **Correct Date Display** - Shows end date as completion", 
                            inline=False)
            embed.add_field(name="Estimated Time", value=data.get('estimated_time', 'Unknown'), inline=True)
            embed.add_field(name="Status", value="Starting Enhanced Absorption Processing...", inline=True)
            embed.add_field(name="⚠️ Note", value="Level must have original volume data first!", inline=False)
            embed.add_field(name="💡 Timeline View", value="Use `/sd timeline` after completion to see segmented visualization!", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to start enhanced absorption job: {str(e)}', ephemeral=True)

    @app_commands.command(description='Check status of enhanced background job')
    @app_commands.describe(job_id='Job ID from the enhanced volume or absorption job command')
    async def job_status(self, interaction: discord.Interaction, job_id: str):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/jobs/{job_id}/status"
            data = await make_api_request(url, timeout_seconds=30)
            
            status = data.get('status', 'unknown')
            progress = data.get('progress', 0)
            analysis_type = data.get('analysis_type', 'volume')
            enhancement = data.get('enhancement', 'standard')
            api_calls_used = data.get('api_calls_used', 0)
            
            if status == 'completed':
                result = data.get('result', {})
                market_data = result.get('market_data', {})
                
                # Set color and emoji based on analysis type
                if analysis_type == 'absorption':
                    color = discord.Color.orange()
                    title_emoji = "🔥"
                    title = f"{title_emoji} Enhanced Absorption Job Completed"
                else:
                    color = discord.Color.green()
                    title_emoji = "✅"
                    title = f"{title_emoji} Enhanced Volume Job Completed"
                
                embed = discord.Embed(title=title, color=color)
                embed.add_field(name="📈 Volume", value=format_large_number(market_data.get('total_volume', 0)), inline=True)
                embed.add_field(name="💰 Value", value=format_large_number(market_data.get('total_value', 0)), inline=True)
                embed.add_field(name="🔢 Trades", value=str(market_data.get('total_trades', 0)), inline=True)
                embed.add_field(name="🎯 Price Range", value=market_data.get('price_range', 'N/A'), inline=True)
                embed.add_field(name="🚀 API Calls", value=f"**{api_calls_used}** (unlimited)", inline=True)
                embed.add_field(name="📊 Analysis Type", value=f"{analysis_type.title()} ({enhancement})", inline=True)
                
                # Show level linking status
                if market_data.get('level_updated'):
                    embed.add_field(name="🔗 Level Update", value=f"✅ Level {market_data.get('level_id')} updated", inline=True)
                    if market_data.get('segment_created'):
                        embed.add_field(name="📊 Segmentation", value="✅ Timeline segment created", inline=True)
                else:
                    embed.add_field(name="🔗 Level Update", value="❌ No matching level found", inline=True)
                
                # Show absorption end date if applicable
                if analysis_type == 'absorption' and market_data.get('absorption_end_date'):
                    embed.add_field(name="📅 Absorption End Date", 
                                   value=f"**{market_data['absorption_end_date']}** (displayed in timeline)", 
                                   inline=False)
                
            elif status == 'failed':
                embed = discord.Embed(
                    title=f"❌ Enhanced {analysis_type.title()} Job Failed",
                    description=data.get('error', 'Unknown error'),
                    color=discord.Color.red()
                )
                
            else:
                # Set color based on analysis type for in-progress jobs
                if analysis_type == 'absorption':
                    color = discord.Color.orange()
                    title = f"🔥 Enhanced Absorption Job In Progress"
                else:
                    color = discord.Color.blue()
                    title = f"🔄 Enhanced Volume Job In Progress"
                
                embed = discord.Embed(title=title, color=color)
                embed.add_field(name="Status", value=status.replace('_', ' ').title(), inline=True)
                embed.add_field(name="Progress", value=f"{progress}%", inline=True)
                embed.add_field(name="Enhancement", value=enhancement, inline=True)
                
                if api_calls_used > 0:
                    embed.add_field(name="🚀 API Calls Used", value=f"{api_calls_used} (unlimited)", inline=True)
                
                # Progress bar
                filled = int(progress / 5)  # 20 segments
                empty = 20 - filled
                progress_bar = "█" * filled + "░" * empty
                embed.add_field(name="Progress Bar", value=f"`{progress_bar}` {progress}%", inline=False)
            
            embed.add_field(name="Job ID", value=f"`{job_id}`", inline=False)
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to check enhanced job status: {str(e)}', ephemeral=True)

    @app_commands.command(description='List all levels for a ticker with enhanced display')
    @app_commands.describe(ticker='Stock ticker symbol')
    async def levels(self, interaction: discord.Interaction, ticker: str):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/{ticker.upper()}/enhanced-timeline"
            data = await make_api_request(url, timeout_seconds=20)
            
            if not data['levels']:
                await interaction.followup.send(f'ℹ️ No levels found for {ticker.upper()}', ephemeral=True)
                return
            
            embed = discord.Embed(
                title=f"📊 Enhanced S/D Levels - {ticker.upper()}",
                description=f"**Total:** {data['level_count']} levels | **Supply:** {data['supply_count']} | **Demand:** {data['demand_count']}",
                color=discord.Color.gold()
            )
            
            # Show individual levels with enhanced info
            for level_data in data['levels'][:6]:  # Show fewer for better formatting
                level = level_data['level']
                volume = level_data['volume']
                dates = level_data['dates']
                job_segments = level_data.get('job_segments', [])
                
                # Set field color indicator based on type
                type_indicator = "🔴" if level['level_type'] == 'supply' else "🟢"
                level_name = f"{type_indicator} ID:{level['id']} | ${level['level_price']:.2f} ({level['level_type'].title()})"
                if level['level_name']:
                    level_name += f" - {level['level_name']}"
                
                # Enhanced volume info with segment count
                volume_info = f"**Original:** {format_large_number(volume['original_volume'])}\n"
                volume_info += f"**Absorbed:** {format_large_number(volume['absorbed_volume'])}\n"
                volume_info += f"**Absorption:** {volume['absorption_percentage']:.1f}%\n"
                volume_info += f"**Segments:** {len(job_segments)}"
                
                # Show last absorption date (corrected to end date)
                if dates['last_absorption_date']:
                    abs_date = datetime.fromisoformat(dates['last_absorption_date']).strftime('%Y-%m-%d')
                    volume_info += f"\n**Last Absorption:** {abs_date}"
                
                volume_info += f"\n**Status:** {level['status']}"
                
                embed.add_field(name=level_name, value=volume_info, inline=True)
            
            if len(data['levels']) > 6:
                embed.set_footer(text=f"Showing 6 of {len(data['levels'])} levels. Use /sd timeline for complete segmented visualization.")
            
            # Enhanced commands section
            embed.add_field(
                name="🚀 Enhanced Commands",
                value="**Timeline:** `/sd timeline` - Segmented absorption view with custom colors\n**Enhanced Jobs:** `/sd enhanced_volume_job` → `/sd enhanced_absorption_job`\n**Manage:** `/sd delete_level` | `/sd deactivate`",
                inline=False
            )
            
            # Features highlight
            embed.add_field(
                name="✨ Enhanced Features",
                value="🚀 **Unlimited API Calls** - Maximum data coverage\n🎨 **Custom Colors** - Supply (red) vs Demand (green)\n📊 **Segmented Timeline** - Each job as separate segment\n📅 **Correct Dates** - End dates properly displayed",
                inline=False
            )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            if "timeout" in str(e).lower():
                await interaction.followup.send(f'❌ Request timed out. The API may be processing large datasets. Try again in a moment.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to get enhanced levels: {str(e)}', ephemeral=True)

    @app_commands.command(description='🎨 Enhanced segmented timeline with custom colors for supply/demand')
    @app_commands.describe(ticker='Stock ticker symbol')
    async def timeline(self, interaction: discord.Interaction, ticker: str):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/{ticker.upper()}/enhanced-timeline"
            data = await make_api_request(url, timeout_seconds=25)
            
            if not data['levels']:
                embed = discord.Embed(
                    title=f"📊 {ticker.upper()} - No Levels Found",
                    description="Create your first level with `/sd create` command",
                    color=discord.Color.orange()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            
            user = interaction.user
            if user is None:
                await interaction.followup.send('❌ Unable to identify user for interactive visualization.', ephemeral=True)
                return
            
            # Create the enhanced timeline view
            view = TimelineAbsorptionView(ticker.upper(), data, user)
            
            # Generate initial segmented chart
            file = view.generate_timeline_visualization()
            
            embed = discord.Embed(
                title=f"{ticker.upper()} Enhanced Segmented Volume Absorption Timeline",
                description="Interactive segmented timeline showing absorption jobs with custom colors and date ranges",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Enhanced Overview",
                value=f"**Total Levels:** {data['level_count']}\n**Supply:** {data['supply_count']} (🔴 red tones) | **Demand:** {data['demand_count']} (🟢 green tones)",
                inline=False
            )
            
            embed.add_field(
                name="🎨 Enhanced Features",
                value="• **Custom Colors:** Supply levels in red tones, demand in green/blue\n• **Segmented Bars:** Each absorption job shown as separate colored segment\n• **Date Ranges:** Full date span (start to end) displayed for each segment\n• **Unlimited API:** Complete data coverage with enhanced processing",
                inline=False
            )
            
            embed.set_image(url=f'attachment://{ticker.lower()}_segmented_timeline.png')
            embed.set_footer(text="Enhanced segmented timeline - each job creates a colored segment with full date range | Use buttons to filter")
            
            await interaction.followup.send(embed=embed, file=file, view=view)
            
        except Exception as e:
            if "timeout" in str(e).lower():
                await interaction.followup.send('❌ Enhanced visualization timed out. Large datasets may require more time. Try again or check API status.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to generate enhanced timeline: {str(e)}', ephemeral=True)

    @app_commands.command(description='Delete a supply/demand level and all its enhanced data')
    @app_commands.describe(level_id='Level ID to delete (from /sd levels)')
    async def delete_level(self, interaction: discord.Interaction, level_id: int):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/{level_id}"
            data = await make_api_request(url, method="DELETE", timeout_seconds=30)
            
            deleted_level = data['deleted_level']
            
            # Color based on level type
            color = discord.Color.red() if deleted_level['level_type'] == 'supply' else discord.Color.green()
            
            embed = discord.Embed(
                title="🗑️ Enhanced Level Deleted Successfully",
                description=f"**Level ID:** {level_id}\n**Ticker:** {deleted_level['ticker']}\n**Price:** ${deleted_level['level_price']:.2f}\n**Type:** {deleted_level['level_type'].title()}",
                color=color
            )
            
            if deleted_level['level_name']:
                embed.add_field(name="Level Name", value=deleted_level['level_name'], inline=True)
            
            embed.add_field(name="🗑️ Deleted Enhanced Data", 
                           value=f"• Tracking records: {deleted_level['deleted_tracking_records']}\n• Absorption segments: {deleted_level['deleted_segments']}", 
                           inline=True)
            embed.add_field(name="⚠️ Warning", value="This action cannot be undone! All enhanced volume tracking and segmentation data permanently removed.", inline=False)
            embed.add_field(name="💡 Alternative", value="Consider using `/sd deactivate` next time for soft deletion that preserves all enhanced data", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            if "404" in str(e):
                await interaction.followup.send(f'❌ Level {level_id} not found.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to delete enhanced level: {str(e)}', ephemeral=True)

    @app_commands.command(description='Deactivate a level (preserves all enhanced data)')
    @app_commands.describe(level_id='Level ID to deactivate (from /sd levels)')
    async def deactivate(self, interaction: discord.Interaction, level_id: int):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/levels/{level_id}/deactivate"
            data = await make_api_request(url, method="PUT", timeout_seconds=30)
            
            embed = discord.Embed(
                title="📴 Enhanced Level Deactivated",
                description=f"**Level ID:** {level_id}\n**Ticker:** {data['ticker']}\n**Price:** ${data['level_price']:.2f}",
                color=discord.Color.orange()
            )
            embed.add_field(name="Status", value="Hidden from level lists but all enhanced data preserved", inline=False)
            embed.add_field(name="💾 Enhanced Data Preservation", 
                           value="• All volume tracking data remains\n• All absorption segments preserved\n• Timeline visualization data intact", 
                           inline=True)
            embed.add_field(name="👀 Visibility", value="Won't appear in `/sd levels` or `/sd timeline` anymore", inline=True)
            embed.add_field(name="💡 Note", value="Use `/sd delete_level` if you need permanent removal of all enhanced data instead", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            if "404" in str(e):
                await interaction.followup.send(f'❌ Level {level_id} not found.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to deactivate enhanced level: {str(e)}', ephemeral=True)

    @app_commands.command(description='Delete an enhanced background job')
    @app_commands.describe(job_id='Job ID to delete (from /sd job_status)')
    async def delete_job(self, interaction: discord.Interaction, job_id: str):
        await interaction.response.defer(thinking=True)
        
        try:
            url = f"{API_BASE_URL}/jobs/{job_id}"
            data = await make_api_request(url, method="DELETE", timeout_seconds=30)
            
            deleted_job = data['deleted_job']
            
            embed = discord.Embed(
                title="🗑️ Enhanced Job Deleted Successfully",
                description=f"**Job ID:** `{job_id}`\n**Ticker:** {deleted_job.get('ticker', 'Unknown')}\n**Previous Status:** {deleted_job.get('status_before_deletion', 'Unknown')}",
                color=discord.Color.red()
            )
            embed.add_field(name="🔄 Impact", value="Enhanced job removed from tracking system", inline=True)
            embed.add_field(name="💾 Level Data", value="Associated level data and segments remain unless level is also deleted", inline=True)
            embed.add_field(name="⚠️ Note", value="This only removes enhanced job tracking - level volume data and segments are preserved", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            if "404" in str(e):
                await interaction.followup.send(f'❌ Enhanced job `{job_id}` not found.', ephemeral=True)
            else:
                await interaction.followup.send(f'❌ Failed to delete enhanced job: {str(e)}', ephemeral=True)

    @app_commands.command(description='Link a completed enhanced job to a level')
    @app_commands.describe(
        job_id='Job ID from a completed enhanced volume job',
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
                title=f"{title_emoji} Enhanced Job Linked Successfully",
                description=f"Enhanced job `{job_id}` has been linked to Level `{level_id}` as **{analysis_type}** data",
                color=color
            )
            
            embed.add_field(name="📈 Volume", value=format_large_number(volume_data.get('total_volume', 0)), inline=True)
            embed.add_field(name="💰 Value", value=format_large_number(volume_data.get('total_value', 0)), inline=True)
            embed.add_field(name="🔢 Trades", value=str(volume_data.get('total_trades', 0)), inline=True)
            embed.add_field(name="🎯 Price Range", value=volume_data.get('price_range', 'N/A'), inline=True)
            embed.add_field(name="🚀 API Calls", value=f"{volume_data.get('api_calls_made', 0)} (unlimited)", inline=True)
            embed.add_field(name="📊 Analysis Type", value=f"{analysis_type.title()} (Enhanced)", inline=True)
            
            if analysis_type == 'absorption':
                embed.add_field(name="📊 Segmentation", value="✅ Timeline segment created for enhanced visualization", inline=False)
            
            embed.add_field(name="✅ Next Steps", value="Use `/sd levels` or `/sd timeline` to see updated enhanced data with custom colors", inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to link enhanced job: {str(e)}', ephemeral=True)

    @app_commands.command(description='Show comprehensive help for enhanced SD level management')
    async def help(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        
        embed = discord.Embed(
            title="📚 Enhanced Supply/Demand Level Management Guide",
            description="Complete workflow for managing S/D levels with unlimited API, segmented timeline visualization, and custom colors",
            color=discord.Color.blue()
        )
        
        # Enhanced workflow
        embed.add_field(
            name="🚀 Enhanced Workflow",
            value="1. `/sd create` - Create a new level\n2. `/sd enhanced_volume_job` - Set original volume (unlimited API)\n3. `/sd enhanced_absorption_job` - Track absorption (with segments)\n4. `/sd timeline` - View segmented timeline with custom colors",
            inline=False
        )
        
        # Enhanced visualization
        embed.add_field(
            name="🎨 Enhanced Visualization",
            value="• `/sd timeline` - Segmented timeline with custom colors\n• **Supply levels:** Red color scheme\n• **Demand levels:** Green/blue color scheme\n• **Segmented bars:** Each job creates separate segment\n• **Date ranges:** Full start-to-end dates shown",
            inline=True
        )
        
        # Enhanced jobs
        embed.add_field(
            name="🚀 Enhanced Job Commands",
            value="• `/sd enhanced_volume_job` - Unlimited API volume analysis\n• `/sd enhanced_absorption_job` - Segmented absorption tracking\n• `/sd job_status` - Real-time progress with API call counts\n• **Unlimited API calls** for maximum data coverage",
            inline=True
        )
        
        # Management commands
        embed.add_field(
            name="🔧 Management Commands",
            value="• `/sd levels` - Enhanced level display with segments\n• `/sd link_job` - Link completed jobs to levels\n• `/sd delete_level` - Permanent deletion\n• `/sd deactivate` - Soft delete (preserve enhanced data)\n• `/sd delete_job` - Remove job tracking",
            inline=False
        )
        
        # Enhanced features
        embed.add_field(
            name="✨ Enhanced Features",
            value="🚀 **Unlimited API Calls** - Complete data coverage\n🎨 **Custom Colors** - Supply (red) vs Demand (green/blue)\n📊 **Segmented Timeline** - Each absorption job as separate segment\n📅 **Correct Dates** - End dates properly displayed as completion dates\n⚡ **Fast Processing** - Optimized algorithms",
            inline=False
        )
        
        # Examples
        embed.add_field(
            name="📝 Enhanced Example Commands",
            value="`/sd create ticker:TSLA level_price:356.43 level_type:supply level_name:Key_Resistance`\n`/sd enhanced_volume_job ticker:TSLA level_price:356.43 start_date:2024-01-01 end_date:2024-01-31`\n`/sd enhanced_absorption_job ticker:TSLA level_price:356.43 start_date:2025-05-26 end_date:2025-09-11 level_id:1`\n`/sd timeline ticker:TSLA` - See segmented visualization",
            inline=False
        )
        
        # Color scheme info
        embed.add_field(
            name="🎨 Color Schemes",
            value="**Supply Levels (Red Tones):**\n• Original: Dark Red (#8B0000)\n• Absorption Segments: Crimson, Fire Brick, Indian Red, etc.\n\n**Demand Levels (Green/Blue Tones):**\n• Original: Dark Green (#006400)\n• Absorption Segments: Forest Green, Lime, Turquoise, etc.",
            inline=False
        )
        
        await interaction.followup.send(embed=embed)

# ─── DARK POOL COMMANDS (UNCHANGED) ──────────────────────────────
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

# ─── LIT COMMANDS (UNCHANGED) ─────────────────────────────────────
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
    if bot.user is not None:
        print(f'✅ Enhanced Bot logged in as {bot.user} (ID: {bot.user.id})')
    else:
        print('✅ Enhanced Bot logged in but user info not available')
    print('------')
    
    # Test enhanced unified API connection
    try:
        health_data = await make_api_request(f"{API_BASE_URL}/health", timeout_seconds=10)
        print(f"✅ Enhanced Unified API Health: {health_data}")
        
        # Test enhanced features
        if health_data.get('enhanced'):
            print("🚀 Enhanced features detected:")
            for feature in health_data.get('features', []):
                print(f"   • {feature}")
        
    except Exception as e:
        print(f"⚠️  Enhanced Unified API Health Check Failed: {str(e)}")
    
    try:
        synced = await bot.tree.sync()
        print(f"✅ Enhanced commands synced successfully: {len(synced)} commands")
        print("🎨 Custom color schemes loaded for supply/demand visualization")
        print("📊 Segmented timeline visualization ready")
        print("🚀 Unlimited API call support enabled")
    except Exception as e:
        print(f"🔴 Enhanced command sync failed: {e}")

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"Enhanced command error: {error}")
    
    if isinstance(error, app_commands.CommandOnCooldown):
        try:
            await interaction.response.send_message(
                f"Enhanced command is on cooldown. Try again in {error.retry_after:.2f} seconds.",
                ephemeral=True
            )
        except:
            pass
    elif isinstance(error, app_commands.MissingPermissions):
        try:
            await interaction.response.send_message(
                "❌ Missing permissions to execute this enhanced command.",
                ephemeral=True
            )
        except:
            pass
    else:
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An unexpected error occurred with the enhanced command. This may be due to processing large datasets with unlimited API calls. Try the command again.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ An unexpected error occurred with the enhanced command. This may be due to processing large datasets with unlimited API calls. Try the command again.",
                    ephemeral=True
                )
        except Exception as followup_error:
            print(f"Could not send enhanced error message: {followup_error}")
        
        # Log the full error for debugging
        print("Full enhanced error traceback:")
        traceback.print_exc()

if __name__ == '__main__':
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError('DISCORD_BOT_TOKEN not set in environment')
    
    print("🚀 Starting Enhanced Unified Bot with:")
    print("   • Unlimited API call support")
    print("   • Custom supply/demand color schemes")
    print("   • Segmented timeline visualization")
    print("   • Correct absorption date display")
    print("   • Enhanced job tracking with segments")
    print("------")
    
    bot.tree.add_command(DarkPoolCommands())
    bot.tree.add_command(LitCommands())
    bot.tree.add_command(SupplyDemandCommands())
    bot.run(DISCORD_BOT_TOKEN)
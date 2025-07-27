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

# ─── INVITE & GUILD CONFIG ───────────────────────────────────────
TEST_GUILD_ID = 123456789012345678  # ← replace with your test server ID
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
API_BASE_URL      = 'http://127.0.0.1:8001'

# ─── BOT SETUP ───────────────────────────────────────────────────
intents = discord.Intents.default()
bot     = commands.Bot(command_prefix='!', intents=intents)

# ─── HELPERS ─────────────────────────────────────────────────────
def format_large_number(num):
    if num is None: return 'N/A'
    if num >= 1_000_000_000: return f'{num/1_000_000_000:.2f}B'
    if num >= 1_000_000: return f'{num/1_000_000:.2f}M'
    if num >= 1_000: return f'{num/1_000:.2f}K'
    return str(num)

# ─── PAGINATION & IMAGE GENERATOR ─────────────────────────────────
class PaginatorView(discord.ui.View):
    def __init__(self, trades, headers, title, author, **kwargs):
        super().__init__(timeout=180)
        self.trades       = trades
        self.headers      = headers
        self.title        = title
        self.author       = author
        self.current_page = 0
        self.per_page     = 15
        self.total_pages  = math.ceil(len(trades) / self.per_page)
        
        self.header_top, self.header_bot = kwargs.get('header_gradient', ((120, 40, 0), (255, 140, 0)))
        self.body_top, self.body_bot = kwargs.get('body_gradient', ((0, 0, 0), (50, 50, 50)))
        
        self.hdr_font = ImageFont.truetype('RobotoCondensed-Bold.ttf', HEADER_FONT_SIZE)
        self.body_font = ImageFont.truetype('RobotoCondensed-Regular.ttf', BODY_FONT_SIZE)
        
        self._update_buttons()

    def _update_buttons(self):
        self.previous_button.disabled = (self.current_page == 0)
        self.next_button.disabled     = (self.current_page >= self.total_pages - 1)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message('These buttons aren’t for you.', ephemeral=True)
            return False
        return True

    def generate_image_file(self) -> discord.File:
        stripe_c = (25, 25, 25)
        txt_c = (245, 245, 245)

        start = self.current_page * self.per_page
        page_trades = self.trades[start:start + self.per_page]

        def get_text_width(text, is_header):
            font = self.hdr_font if is_header else self.body_font
            return font.getbbox(text)[2]

        column_widths = {col: get_text_width(col, True) for col in self.headers}
        for t in page_trades:
            formatted_vals = {
                'Ticker': t.get('ticker',''),
                'Quantity': format_large_number(t['quantity']),
                'Price': f"{t['price']:.2f}",
                'Value': format_large_number(t['trade_value']),
                'Time': datetime.fromisoformat(t['trade_time']).strftime('%Y-%m-%d %H:%M:%S')
            }
            for col, val in formatted_vals.items():
                if col in column_widths:
                    column_widths[col] = max(column_widths[col], get_text_width(val, False))
        
        positions = {}
        x_cursor = SIDE_PADDING
        for col in self.headers:
            width = column_widths[col]
            align_right = col != 'Ticker'
            positions[col] = {'x': x_cursor + (width if align_right else 0), 'align_right': align_right}
            x_cursor += width + COLUMN_SPACING
        
        img_w = int(x_cursor - COLUMN_SPACING + SIDE_PADDING)
        row_h = int(self.body_font.getbbox("A")[3] + ROW_PADDING * 2)
        hdr_h = int(self.hdr_font.getbbox("A")[3] + ROW_PADDING * 2)
        img_h = int(hdr_h + len(page_trades) * row_h + BOTTOM_PADDING)

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
        
        for i, t in enumerate(page_trades):
            if i % 2 == 1:
                draw.rectangle([(0, y_cursor), (img_w, y_cursor + row_h)], fill=stripe_c)
            
            vals = {
                'Ticker':   t.get('ticker',''),
                'Quantity': format_large_number(t['quantity']),
                'Price':    f"{t['price']:.2f}",
                'Value':    format_large_number(t['trade_value']),
                'Time':     datetime.fromisoformat(t['trade_time']).strftime('%Y-%m-%d %H:%M:%S')
            }
            for col in self.headers:
                txt = vals[col]
                pos = positions[col]
                x0 = pos['x'] - (get_text_width(txt, False) if pos['align_right'] else 0)
                draw.text((x0, y_cursor + ROW_PADDING), txt, font=self.body_font, fill=txt_c)
            y_cursor += row_h

        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename='trades.png')

    async def _update_message(self, interaction: discord.Interaction):
        self._update_buttons()
        file = self.generate_image_file()
        
        # ✅ FIX: Recreate the embed with the updated footer text
        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        if bot.user:
            embed.set_author(name='Deltuh DP Bot', icon_url=bot.user.display_avatar.url)
        # ✅ FIX: Update the page number in the footer
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

# --- COMMAND RUNNER ---------------------------------------------
async def run_paginated_command(interaction: discord.Interaction, url: str, headers: list, title: str, **kwargs):
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

    view = PaginatorView(data, headers, title, interaction.user, **kwargs)
    
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

# --- SLASH COMMANDS ----------------------------------------------
class DarkPoolCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='dp', description='Dark Pool & Block trades')

    @app_commands.command(description='Block trades off-exchange')
    @app_commands.guilds(TEST_GUILD)
    async def allblocks(self, interaction: discord.Interaction, ticker: str):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/dp/allblocks/{ticker.upper()}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Block Trades for {ticker.upper()}"
        )

    @app_commands.command(description='Block trades during market hours')
    @app_commands.guilds(TEST_GUILD)
    async def alldp(self, interaction: discord.Interaction, ticker: str):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/dp/alldp/{ticker.upper()}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Dark-Pool Trades for {ticker.upper()}"
        )

    @app_commands.command(description='Top 100 block trades by value')
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(self, interaction: discord.Interaction, days: app_commands.Range[int,1,30] = 1):
        await run_paginated_command(
            interaction, f"{API_BASE_URL}/dp/bigprints?days={days}",
            ['Ticker','Quantity','Price','Value','Time'],
            f"Biggest Prints Last {days} Day{'s' if days>1 else ''}"
        )

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
            header_gradient=((97, 138, 250), (0, 68, 255))
        )

    @app_commands.command(name='bigprints', description='Top lit trades by value over the last X days')
    @app_commands.describe(
        days='Days back (1–30)',
        under_400m='Filter for trades under $400M' # ✅ MODIFIED
    )
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(self, interaction: discord.Interaction, days: app_commands.Range[int, 1, 30] = 1, under_400m: bool = False): # ✅ MODIFIED
        url = f"{API_BASE_URL}/lit/bigprints?days={days}"
        if under_400m:
            url += "&under_400m=true" # ✅ MODIFIED
            
        title   = f"Biggest Lit Prints Last {days} Day{'s' if days > 1 else ''}"
        if under_400m:
            title += " (Under $400M)" # ✅ MODIFIED

        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(
            interaction, url, headers, title,
            header_gradient=((97, 138, 250), (0, 68, 255))
        )

# --- BOT LIFECYCLE ───────────────────────────────────────────────
@bot.event
async def on_ready():
    if bot.user:
        print(f'✅ Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')
    try:
        await bot.tree.sync()
        print("✅ Commands synced successfully.")
    except Exception as e:
        print(f"🔴 Command sync failed: {e}")

if __name__ == '__main__':
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError('DISCORD_BOT_TOKEN not set in environment')
    
    bot.tree.add_command(DarkPoolCommands())
    bot.tree.add_command(LitCommands())
    bot.run(DISCORD_BOT_TOKEN)

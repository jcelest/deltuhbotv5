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
SIDE_PADDING     = 50      # margin on left/right
COLUMN_SPACING   = 75      # space between columns
HEADER_FONT_SIZE = 30      # header font size
BODY_FONT_SIZE   = 50      # body font size
ROW_PADDING      = 10      # vertical padding in rows
BOTTOM_PADDING   = 40      # padding under last row

# ─── BOT & API CONFIGURATION ────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN') or ''
API_BASE_URL      = 'http://127.0.0.1:8001'

# ─── BOT SETUP ───────────────────────────────────────────────────
intents = discord.Intents.default()
bot     = commands.Bot(command_prefix='!', intents=intents)

# ─── HELPERS ─────────────────────────────────────────────────────
def format_large_number(num):
    if num is None:
        return 'N/A'
    if num >= 1_000_000_000:
        return f'{num/1_000_000_000:.2f}B'
    if num >= 1_000_000:
        return f'{num/1_000_000:.2f}M'
    if num >= 1_000:
        return f'{num/1_000:.2f}K'
    return str(num)

# ─── PAGINATION & IMAGE GENERATOR ─────────────────────────────────
class PaginatorView(discord.ui.View):
    def __init__(
        self,
        trades,
        headers,
        title,
        author,
        *,
        header_gradient=((120, 40, 0), (255, 140, 0)),
        body_gradient=((0, 0, 0), (50, 50, 50))
    ):
        super().__init__(timeout=180)
        self.trades       = trades
        self.headers      = headers
        self.title        = title
        self.author       = author
        self.current_page = 0
        self.per_page     = 15
        self.total_pages  = math.ceil(len(trades) / self.per_page)
        self._update_buttons()

        self.header_top, self.header_bot = header_gradient
        self.body_top,   self.body_bot   = body_gradient

    def _update_buttons(self):
        self.previous_button.disabled = (self.current_page == 0)
        self.next_button.disabled     = (self.current_page >= self.total_pages - 1)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                'These buttons aren’t for you.', ephemeral=True
            )
            return False
        return True

    def generate_image_file(self) -> discord.File:
        # This function remains unchanged
        hdr_font  = ImageFont.truetype('RobotoCondensed-Bold.ttf', HEADER_FONT_SIZE)
        body_font = ImageFont.truetype('RobotoCondensed-Regular.ttf', BODY_FONT_SIZE)
        stripe_c  = (25, 25, 25)
        txt_c     = (245, 245, 245)

        start       = self.current_page * self.per_page
        page_trades = self.trades[start:start + self.per_page]

        tmp_img  = Image.new('RGB', (1,1))
        tmp_draw = ImageDraw.Draw(tmp_img)
        column_widths = {}
        for col in self.headers:
            texts = [col]
            for t in page_trades:
                if col == 'Quantity':
                    texts.append(format_large_number(t['quantity']))
                elif col == 'Price':
                    texts.append(f"{t['price']:.2f}")
                elif col == 'Value':
                    texts.append(format_large_number(t['trade_value']))
                elif col == 'Time':
                    texts.append(datetime.fromisoformat(t['trade_time'])
                                       .strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    texts.append(t.get('ticker',''))
            max_w = 0
            for txt in texts:
                font = hdr_font if txt == col else body_font
                w = tmp_draw.textbbox((0,0), txt, font=font)[2]
                max_w = max(max_w, w)
            column_widths[col] = max_w

        positions = {}
        x_cursor  = SIDE_PADDING
        for col in self.headers:
            w     = column_widths[col]
            align = 'left' if col=='Ticker' else 'right'
            positions[col] = {
                'x': x_cursor + (w if align=='right' else 0),
                'align': align
            }
            x_cursor += w + COLUMN_SPACING
        img_w = x_cursor - COLUMN_SPACING + SIDE_PADDING

        row_h = BODY_FONT_SIZE + ROW_PADDING*2
        hdr_h = HEADER_FONT_SIZE + ROW_PADDING*2
        img_h = hdr_h + len(page_trades)*row_h + BOTTOM_PADDING

        img  = Image.new('RGB', (img_w, img_h))
        draw = ImageDraw.Draw(img)

        top_h, bot_h = self.header_top, self.header_bot
        for y in range(hdr_h):
            blend = y / hdr_h
            r = int(top_h[0]*(1-blend) + bot_h[0]*blend)
            g = int(top_h[1]*(1-blend) + bot_h[1]*blend)
            b = int(top_h[2]*(1-blend) + bot_h[2]*blend)
            draw.line([(0,y),(img_w,y)], fill=(r,g,b))

        body_top, body_bot = self.body_top, self.body_bot
        for y in range(hdr_h, img_h):
            blend = (y - hdr_h) / (img_h - hdr_h)
            r = int(body_top[0]*(1-blend) + body_bot[0]*blend)
            g = int(body_top[1]*(1-blend) + body_bot[1]*blend)
            b = int(body_top[2]*(1-blend) + body_bot[2]*blend)
            draw.line([(0,y),(img_w,y)], fill=(r,g,b))

        for col in self.headers:
            pos = positions[col]
            w   = draw.textbbox((0,0), col, font=hdr_font)[2]
            x0  = pos['x'] - (w if pos['align']=='right' else 0)
            draw.text((x0, ROW_PADDING), col, font=hdr_font, fill=(255,255,255))

        y0 = hdr_h
        for i, t in enumerate(page_trades):
            if i % 2 == 1:
                draw.rectangle([(0,y0),(img_w,y0+row_h)], fill=stripe_c)
            vals = {
                'Ticker':   t.get('ticker',''),
                'Quantity': format_large_number(t['quantity']),
                'Price':    f"{t['price']:.2f}",
                'Value':    format_large_number(t['trade_value']),
                'Time':     datetime.fromisoformat(t['trade_time'])
                                        .strftime('%Y-%m-%d %H:%M:%S')
            }
            for col in self.headers:
                txt = vals[col]
                w   = draw.textbbox((0,0), txt, font=body_font)[2]
                pos = positions[col]
                x0  = pos['x'] - (w if pos['align']=='right' else 0)
                draw.text((x0, y0+ROW_PADDING), txt, font=body_font, fill=txt_c)
            y0 += row_h

        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename='trades.png')

    async def _update_message(self, interaction: discord.Interaction):
        self._update_buttons()
        file = self.generate_image_file()
        user = interaction.client.user
        if not user:
            raise RuntimeError("Bot client has no user")
        icon = user.display_avatar.url
        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        embed.set_author(name='Deltuh DP Bot', icon_url=icon)
        embed.set_footer(text=f"Page {self.current_page+1}/{self.total_pages}")
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

# ─── COMMAND RUNNER & DEBUGGER ────────────────────────────────────
async def run_paginated_command(
    interaction: discord.Interaction,
    url: str,
    headers: list,
    title: str,
    *,
    header_gradient=None,
    body_gradient=None
):
    await interaction.response.defer(thinking=True)
    try:
        async with httpx.AsyncClient() as client:
            resp = await asyncio.wait_for(client.get(url), timeout=30)
        resp.raise_for_status()
        data = resp.json()

    except Exception as e:
        print("--- AN ERROR OCCURRED ---")
        
        # ✅ FINAL FIX: Use isinstance for a definitive type check
        full_command_name = "N/A"
        if interaction.command:
            command_name = interaction.command.name
            if isinstance(interaction.command, app_commands.Command) and interaction.command.parent:
                full_command_name = f"/{interaction.command.parent.name} {command_name}"
            else:
                full_command_name = f"/{command_name}"
        print(f"Command: {full_command_name}")
        
        print(f"URL Requested: {url}")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e!r}")
        print("--- TRACEBACK ---")
        traceback.print_exc()
        print("---------------------")
        await interaction.followup.send(
            '❌ An error occurred while contacting the API. Check the bot console for details.',
            ephemeral=True
        )
        return

    if not data:
        await interaction.followup.send('ℹ️ No data found.', ephemeral=True)
        return

    view = PaginatorView(
        data, headers, title, interaction.user,
        header_gradient=header_gradient or ((255, 140, 0), (120, 40, 0)),
        body_gradient=body_gradient or ((0,0,0),(50,50,50))
    )
    user = bot.user
    icon = user.display_avatar.url if user else None
    embed = discord.Embed(title=title, color=discord.Color(0xFF8C00))
    embed.set_author(name='Deltuh DP Bot', icon_url=icon)
    embed.set_footer(text=f"Page 1/{view.total_pages}")
    embed.set_image(url='attachment://trades.png')
    await interaction.followup.send(
        embed=embed,
        file=view.generate_image_file(),
        view=view
    )

# ─── SLASH COMMAND GROUPS ────────────────────────────────────────
class DarkPoolCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='dp', description='Dark Pool & Block trades')

    @app_commands.command(description='Block trades off-exchange before/after hours')
    @app_commands.describe(ticker='Ticker symbol')
    @app_commands.guilds(TEST_GUILD)
    async def allblocks(self, interaction: discord.Interaction, ticker: str):
        url     = f"{API_BASE_URL}/dp/allblocks/{ticker.upper()}"
        title   = f"Block Trades for {ticker.upper()}"
        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(interaction, url, headers, title)

    @app_commands.command(description='Block trades during market hours')
    @app_commands.describe(ticker='Ticker symbol')
    @app_commands.guilds(TEST_GUILD)
    async def alldp(self, interaction: discord.Interaction, ticker: str):
        url     = f"{API_BASE_URL}/dp/alldp/{ticker.upper()}"
        title   = f"Dark-Pool Trades for {ticker.upper()}"
        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(interaction, url, headers, title)

    @app_commands.command(description='Top 100 block trades by value over the last X days')
    @app_commands.describe(days='Days back (1–30)')
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(self, interaction: discord.Interaction, days: app_commands.Range[int,1,30] = 1):
        url     = f"{API_BASE_URL}/dp/bigprints?days={days}"
        title   = f"Biggest Prints Last {days} Day{'s' if days>1 else ''}"
        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(interaction, url, headers, title)

class LitCommands(app_commands.Group):
    def __init__(self):
        super().__init__(name='lit', description='Lit market trades')

    @app_commands.command(name='all', description='All lit-market trades for a ticker')
    @app_commands.describe(ticker='Ticker symbol')
    @app_commands.guilds(TEST_GUILD)
    async def all_trades(self, interaction: discord.Interaction, ticker: str):
        url     = f"{API_BASE_URL}/lit/all/{ticker.upper()}"
        title   = f"Lit Market Trades for {ticker.upper()}"
        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(
            interaction, url, headers, title,
            header_gradient=((0, 25, 255), (0, 10, 105))
        )

    @app_commands.command(name='bigprints', description='Top 100 lit trades by value over the last X days')
    @app_commands.describe(days='Days back (1–30)')
    @app_commands.guilds(TEST_GUILD)
    async def bigprints(self, interaction: discord.Interaction, days: app_commands.Range[int, 1, 30] = 1):
        url     = f"{API_BASE_URL}/lit/bigprints?days={days}"
        title   = f"Biggest Lit Prints Last {days} Day{'s' if days > 1 else ''}"
        headers = ['Ticker','Quantity','Price','Value','Time']
        await run_paginated_command(
            interaction, url, headers, title,
            header_gradient=((0, 25, 255), (0, 10, 105))
        )

# ─── BOT LIFECYCLE ───────────────────────────────────────────────
bot.tree.add_command(DarkPoolCommands())
bot.tree.add_command(LitCommands())

@bot.event
async def on_ready():
    if bot.user:
        print(f'✅ Logged in as {bot.user} (ID: {bot.user.id})')
    else:
        print("✅ Logged in, but bot.user is not yet available.")
    print('------')
    
    try:
        bot.tree.copy_global_to(guild=TEST_GUILD)
        await bot.tree.sync(guild=TEST_GUILD)
        print(f"✅ Commands synced to guild {TEST_GUILD_ID}")
    except discord.errors.Forbidden:
        await bot.tree.sync()
        print("⚠️ Could not sync to test guild, synced globally instead.")

if __name__ == '__main__':
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError('DISCORD_BOT_TOKEN not set in environment')
    bot.run(DISCORD_BOT_TOKEN)
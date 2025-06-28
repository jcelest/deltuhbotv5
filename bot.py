import os
import math
import io
import asyncio
import httpx
import discord
from discord.ext import commands
from discord import app_commands
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime

# --- CONFIG ---
SIDE_PADDING     = 50      # margin on left and right
COLUMN_SPACING   = 75      # increased space between columns
HEADER_FONT_SIZE = 30      # header font size
BODY_FONT_SIZE   = 50      # slightly reduced body font size for better fit
ROW_PADDING      = 10      # vertical padding within each row
BOTTOM_PADDING   = 40      # padding below the last row

# --- BOT & API CONFIGURATION ---
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
API_BASE_URL      = "http://127.0.0.1:8001"

# --- BOT SETUP ---
intents = discord.Intents.default()
bot     = commands.Bot(command_prefix="!", intents=intents)

# --- HELPER FUNCTIONS ---
def format_large_number(num):
    if num is None:
        return "N/A"
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num/1_000:.2f}K"
    return str(num)

# --- PAGINATION & IMAGE GENERATION ---
class PaginatorView(discord.ui.View):
    def __init__(self, trades, headers, title, author):
        super().__init__(timeout=180)
        self.trades       = trades
        self.headers      = headers
        self.title        = title
        self.author       = author
        self.current_page = 0
        self.per_page     = 15
        self.total_pages  = math.ceil(len(trades) / self.per_page)
        self._update_buttons()

    def _update_buttons(self):
        self.previous_button.disabled = (self.current_page == 0)
        self.next_button.disabled     = (self.current_page >= self.total_pages - 1)

    async def interaction_check(self, interaction):
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "These buttons aren’t for you.", ephemeral=True
            )
            return False
        return True

    def generate_image_file(self):
        # fonts, colors
        hdr_font  = ImageFont.truetype("RobotoCondensed-Bold.ttf", HEADER_FONT_SIZE)
        body_font = ImageFont.truetype("RobotoCondensed-Regular.ttf", BODY_FONT_SIZE)
        stripe_c  = (25, 25, 25)
        txt_c     = (245, 245, 245)

        # current page slice
        start       = self.current_page * self.per_page
        page_trades = self.trades[start:start + self.per_page]

        # temporary draw for measurement
        tmp_img  = Image.new("RGB", (1, 1))
        tmp_draw = ImageDraw.Draw(tmp_img)

        # calculate column widths
        column_widths = {}
        for col in self.headers:
            sample_texts = [col]
            for t in page_trades:
                if col == "Quantity":
                    sample_texts.append(format_large_number(t["quantity"]))
                elif col == "Price":
                    sample_texts.append(f"{t['price']:.2f}")
                elif col == "Value":
                    sample_texts.append(format_large_number(t["trade_value"]))
                elif col == "Time":
                    sample_texts.append(
                        datetime.fromisoformat(t["trade_time"])\
                            .strftime("%Y-%m-%d %H:%M:%S")
                    )
                else:
                    sample_texts.append(t.get("ticker", ""))
            max_w = 0
            for txt in sample_texts:
                font = hdr_font if txt == col else body_font
                bbox = tmp_draw.textbbox((0,0), txt, font=font)
                w = bbox[2] - bbox[0]
                max_w = max(max_w, w)
            column_widths[col] = max_w

        # set positions
        positions = {}
        x_cursor  = SIDE_PADDING
        for col in self.headers:
            w = column_widths[col]
            align = 'left' if col == 'Ticker' else 'right'
            x = x_cursor + (w if align == 'right' else 0)
            positions[col] = {'x': x, 'align': align}
            x_cursor += w + COLUMN_SPACING
        img_width = x_cursor - COLUMN_SPACING + SIDE_PADDING

        # calculate heights
        row_h = BODY_FONT_SIZE + ROW_PADDING * 2
        hdr_h = HEADER_FONT_SIZE + ROW_PADDING * 2
        img_h = hdr_h + len(page_trades) * row_h + BOTTOM_PADDING

        # create image
        img  = Image.new("RGB", (img_width, img_h))
        draw = ImageDraw.Draw(img)

        # header gradient
        top_c, bot_c = (120, 40, 0), (255, 140, 0)
        for y in range(hdr_h):
            blend = y / hdr_h
            r = int(top_c[0] * (1 - blend) + bot_c[0] * blend)
            g = int(top_c[1] * (1 - blend) + bot_c[1] * blend)
            b = int(top_c[2] * (1 - blend) + bot_c[2] * blend)
            draw.line([(0, y), (img_width, y)], fill=(r, g, b))

        # body gradient
        for y in range(hdr_h, img_h):
            blend = (y - hdr_h) / (img_h - hdr_h)
            gray  = int(50 * blend)
            draw.line([(0, y), (img_width, y)], fill=(gray, gray, gray))

        # draw headers
        for col in self.headers:
            pos = positions[col]
            bbox = draw.textbbox((0,0), col, font=hdr_font)
            w = bbox[2] - bbox[0]
            x = pos['x'] - (w if pos['align'] == 'right' else 0)
            y = ROW_PADDING
            draw.text((x, y), col, font=hdr_font, fill=(255, 255, 255))

        # draw rows
        y0 = hdr_h
        for i, t in enumerate(page_trades):
            if i % 2 == 1:
                draw.rectangle([(0, y0), (img_width, y0 + row_h)], fill=stripe_c)
            vals = {
                'Ticker':   t.get('ticker', ''),
                'Quantity': format_large_number(t['quantity']),
                'Price':    f"{t['price']:.2f}",
                'Value':    format_large_number(t['trade_value']),
                'Time':     datetime.fromisoformat(t['trade_time'])
                               .strftime("%Y-%m-%d %H:%M:%S")
            }
            for col in self.headers:
                txt  = vals[col]
                font = hdr_font if txt == col else body_font
                bbox = draw.textbbox((0,0), txt, font=font)
                w     = bbox[2] - bbox[0]
                pos   = positions[col]
                x     = pos['x'] - (w if pos['align'] == 'right' else 0)
                y     = y0 + ROW_PADDING
                draw.text((x, y), txt, font=font, fill=txt_c)
            y0 += row_h

        buf = io.BytesIO()
        img.save(buf, 'PNG')
        buf.seek(0)
        return discord.File(fp=buf, filename='trades.png')

    @discord.ui.button(label='Previous', style=discord.ButtonStyle.secondary, emoji='◀️')
    async def previous_button(self, interaction, button):
        self.current_page -= 1
        self._update_buttons()
        file = self.generate_image_file()
        user = interaction.client.user
        assert user is not None
        icon_url = user.display_avatar.url
        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        embed.set_author(name='Deltuh DP Bot', icon_url=icon_url)
        embed.set_image(url='attachment://trades.png')
        embed.set_footer(text=f"Page {self.current_page+1}/{self.total_pages}")
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

    @discord.ui.button(label='Next', style=discord.ButtonStyle.secondary, emoji='▶️')
    async def next_button(self, interaction, button):
        self.current_page += 1
        self._update_buttons()
        file = self.generate_image_file()
        user = interaction.client.user
        assert user is not None
        icon_url = user.display_avatar.url
        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        embed.set_author(name='Deltuh DP Bot', icon_url=icon_url)
        embed.set_image(url='attachment://trades.png')
        embed.set_footer(text=f"Page {self.current_page+1}/{self.total_pages}")
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

# --- SLASH COMMANDS ---
class DarkPoolCommands(app_commands.Group):
    async def run_paginated_command(self, interaction, endpoint_url, headers, title):
        await interaction.response.defer(thinking=True)
        try:
            async with httpx.AsyncClient() as client:
                resp = await asyncio.wait_for(client.get(endpoint_url), timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            await interaction.followup.send(f"❌ Error fetching: {e}", ephemeral=True)
            return
        if not data:
            await interaction.followup.send("ℹ️ No data found.")
            return
        view = PaginatorView(data, headers, title, interaction.user)
        user = bot.user
        assert user is not None
        icon_url = user.display_avatar.url
        embed = discord.Embed(title=title, color=discord.Color(0xFF8C00))
        embed.set_author(name='Deltuh DP Bot', icon_url=icon_url)
        embed.set_image(url='attachment://trades.png')
        embed.set_footer(text=f"Page 1/{view.total_pages}")
        await interaction.followup.send(embed=embed, file=view.generate_image_file(), view=view)

    @app_commands.command(name='allblocks', description='Shows off-exchange block trades.')
    async def allblocks(self, interaction, ticker: str):
        headers = ['Ticker','Quantity','Price','Value','Time']
        url     = f"{API_BASE_URL}/dp/allblocks/{ticker.upper()}"
        title   = f"Block Trades for **{ticker.upper()}**"
        await self.run_paginated_command(interaction, url, headers, title)

    @app_commands.command(name='alldp', description='Shows dark-pool trades.')
    async def alldp(self, interaction, ticker: str):
        headers = ['Ticker','Quantity','Price','Value','Time']
        url     = f"{API_BASE_URL}/dp/alldp/{ticker.upper()}"
        title   = f"Dark-Pool Trades for **{ticker.upper()}**"
        await self.run_paginated_command(interaction, url, headers, title)

    @app_commands.command(name='bigprints', description="Shows today's biggest prints.")
    async def bigprints(self, interaction):
        headers = ['Ticker','Quantity','Price','Value','Time']
        url     = f"{API_BASE_URL}/dp/bigprints"
        title   = "Today's Biggest Prints"
        await self.run_paginated_command(interaction, url, headers, title)

@bot.event
async def on_ready():
    bot.tree.add_command(DarkPoolCommands(name='dp'))
    await bot.tree.sync()
    print(f"Logged in as {bot.user}")

if __name__ == '__main__':
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN not set")
    bot.run(DISCORD_BOT_TOKEN)

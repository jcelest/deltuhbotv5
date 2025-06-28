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

# --- BOT & API CONFIGURATION ---
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
API_BASE_URL     = "http://127.0.0.1:8001"

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

# --- PAGINATION VIEW CLASS with Gradient Header ---
class PaginatorView(discord.ui.View):
    def __init__(self, trades, headers, positions, title, author):
        super().__init__(timeout=180)
        self.trades       = trades
        self.headers      = headers
        self.positions    = positions
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
        start       = self.current_page * self.per_page
        end         = start + self.per_page
        page_trades = self.trades[start:end]

        width, row_h, hdr_h, pad = 800, 40, 55, 40
        height = hdr_h + row_h * len(page_trades) + pad
        img    = Image.new("RGB", (width, height))
        draw   = ImageDraw.Draw(img)

        # gradient header
        top_c, bot_c = (120, 40, 0), (255, 140, 0)
        for y in range(hdr_h):
            blend = y / hdr_h
            r = int(top_c[0] * (1 - blend) + bot_c[0] * blend)
            g = int(top_c[1] * (1 - blend) + bot_c[1] * blend)
            b = int(top_c[2] * (1 - blend) + bot_c[2] * blend)
            draw.line([(0, y), (width, y)], fill=(r, g, b))

        hdr_font  = ImageFont.truetype("RobotoCondensed-Bold.ttf", 30)
        body_font = ImageFont.truetype("RobotoCondensed-Regular.ttf", 28)
        stripe_c  = (25, 25, 25)
        txt_c     = (245, 245, 245)

        # headers
        for h, pos in self.positions.items():
            align, x = pos["align"], pos["x"]
            w = draw.textlength(h, font=hdr_font)
            draw.text((x - w if align == "right" else x, 15),
                      h, font=hdr_font, fill=(255, 255, 255))

        # rows
        y0 = hdr_h
        for i, trade in enumerate(page_trades):
            if i % 2 == 1:
                draw.rectangle([(0, y0), (width, y0 + row_h)], fill=stripe_c)
            vals = {
                "Ticker":   trade["ticker"],
                "Quantity": format_large_number(trade["quantity"]),
                "Price":    f"{trade['price']:.2f}",
                "Value":    format_large_number(trade["trade_value"]),
                "Time":     datetime.fromisoformat(trade["trade_time"])\
                                  .strftime("%Y-%m-%d %H:%M:%S")
            }
            for col, pos in self.positions.items():
                text = vals[col]
                w    = draw.textlength(text, font=body_font)
                x    = pos["x"] - w if pos["align"] == "right" else pos["x"]
                y    = y0 + (row_h - body_font.size) / 2 - 2
                draw.text((x, y), text, font=body_font, fill=txt_c)
            y0 += row_h

        buf = io.BytesIO()
        img.save(buf, "PNG")
        buf.seek(0)
        return discord.File(fp=buf, filename="trades.png")

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="◀️")
    async def previous_button(self, interaction, button):
        self.current_page -= 1
        self._update_buttons()
        file = self.generate_image_file()

        # assert bot.user is logged in
        assert interaction.client.user is not None
        icon_url = interaction.client.user.display_avatar.url

        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        embed.set_author(name="Deltuh DP Bot", icon_url=icon_url)
        embed.set_image(url="attachment://trades.png")
        embed.set_footer(text=f"Page {self.current_page+1}/{self.total_pages}")

        await interaction.response.edit_message(
            embed=embed,
            attachments=[file],
            view=self
        )

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="▶️")
    async def next_button(self, interaction, button):
        self.current_page += 1
        self._update_buttons()
        file = self.generate_image_file()

        assert interaction.client.user is not None
        icon_url = interaction.client.user.display_avatar.url

        embed = discord.Embed(title=self.title, color=discord.Color(0xFF8C00))
        embed.set_author(name="Deltuh DP Bot", icon_url=icon_url)
        embed.set_image(url="attachment://trades.png")
        embed.set_footer(text=f"Page {self.current_page+1}/{self.total_pages}")

        await interaction.response.edit_message(
            embed=embed,
            attachments=[file],
            view=self
        )

# --- SLASH COMMAND GROUP ---
class DarkPoolCommands(app_commands.Group):
    async def run_paginated_command(self, interaction, endpoint_url, headers, positions, title):
        await interaction.response.defer(thinking=True)

        try:
            async with httpx.AsyncClient() as client:
                resp = await asyncio.wait_for(
                    client.get(endpoint_url),
                    timeout=30.0
                )

            resp.raise_for_status()
            data = resp.json()

        except asyncio.TimeoutError:
            await interaction.followup.send(
                "⚠️ This request took too long and timed out—please try again later.",
                ephemeral=True
            )
            return
        except httpx.HTTPError as e:
            await interaction.followup.send(
                f"❌ Error fetching data: {e}",
                ephemeral=True
            )
            return

        if not data:
            await interaction.followup.send("ℹ️ No data found for this command.")
            return

        view = PaginatorView(data, headers, positions, title, interaction.user)

        # initial embed
        assert bot.user is not None
        icon_url = bot.user.display_avatar.url

        embed = discord.Embed(title=title, color=discord.Color(0xFF8C00))
        embed.set_author(name="Deltuh DP Bot", icon_url=icon_url)
        embed.set_image(url="attachment://trades.png")
        embed.set_footer(text=f"Page 1/{view.total_pages}")

        await interaction.followup.send(
            embed=embed,
            file=view.generate_image_file(),
            view=view
        )

    @app_commands.command(name="allblocks", description="Shows off‐exchange block trades for a stock.")
    @app_commands.describe(ticker="Stock ticker (e.g. SPY)")
    async def allblocks(self, interaction, ticker: str):
        ticker_up = ticker.upper()
        headers   = ["Ticker","Quantity","Price","Value","Time"]
        positions = {
            "Ticker":   {"x":100, "align":"right"},
            "Quantity": {"x":220, "align":"right"},
            "Price":    {"x":340, "align":"right"},
            "Value":    {"x":460, "align":"right"},
            "Time":     {"x":730, "align":"right"}
        }
        url   = f"{API_BASE_URL}/dp/allblocks/{ticker_up}"
        title = f"Block Trades for **{ticker_up}**"
        await self.run_paginated_command(interaction, url, headers, positions, title)

    @app_commands.command(name="alldp", description="Shows dark‐pool trades during market hours for a stock.")
    @app_commands.describe(ticker="Stock ticker (e.g. AAPL)")
    async def alldp(self, interaction, ticker: str):
        ticker_up = ticker.upper()
        headers   = ["Ticker","Quantity","Price","Value","Time"]
        positions = {
            "Ticker":   {"x":100, "align":"right"},
            "Quantity": {"x":220, "align":"right"},
            "Price":    {"x":340, "align":"right"},
            "Value":    {"x":460, "align":"right"},
            "Time":     {"x":730, "align":"right"}
        }
        url   = f"{API_BASE_URL}/dp/alldp/{ticker_up}"
        title = f"Dark-Pool Trades for **{ticker_up}**"
        await self.run_paginated_command(interaction, url, headers, positions, title)

    @app_commands.command(name="bigprints", description="Shows largest prints today.")
    async def bigprints(self, interaction):
        headers   = ["Ticker","Quantity","Price","Value","Time"]
        positions = {
            "Ticker":   {"x":100, "align":"right"},
            "Quantity": {"x":220, "align":"right"},
            "Price":    {"x":340, "align":"right"},
            "Value":    {"x":460, "align":"right"},
            "Time":     {"x":730, "align":"right"}
        }
        url   = f"{API_BASE_URL}/dp/bigprints"
        title = "Today's Biggest Prints"
        await self.run_paginated_command(interaction, url, headers, positions, title)

@bot.event
async def on_ready():
    bot.tree.add_command(DarkPoolCommands(name="dp"))
    await bot.tree.sync()
    print(f"Bot logged in as {bot.user}. Commands synced.")

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set")
    bot.run(DISCORD_BOT_TOKEN)

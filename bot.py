import discord
from discord.ext import commands
from discord import app_commands
import os
import httpx
from PIL import Image, ImageDraw, ImageFont
import io
from datetime import datetime
import math

# --- BOT & API CONFIGURATION ---
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
API_BASE_URL = "http://127.0.0.1:8001"

# --- BOT SETUP ---
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- HELPER FUNCTIONS ---
def format_large_number(num):
    if num is None:
        return "N/A"
    if num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num / 1_000:.2f}K"
    return str(num)

# --- PAGINATION VIEW CLASS with Gradient Header ---
class PaginatorView(discord.ui.View):
    def __init__(self, trades: list, headers: list, positions: dict, title: str, author: discord.User | discord.Member):
        super().__init__(timeout=180)
        self.author = author
        self.trades = trades
        self.headers = headers
        self.positions = positions
        self.current_page = 0
        self.trades_per_page = 15
        self.total_pages = math.ceil(len(self.trades) / self.trades_per_page)
        self.update_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You can't use these buttons.", ephemeral=True)
            return False
        return True

    def generate_image_file(self):
        start_index = self.current_page * self.trades_per_page
        end_index = start_index + self.trades_per_page
        page_trades = self.trades[start_index:end_index]

        width, row_height, header_height, bottom_padding = 800, 40, 55, 40
        height = header_height + (len(page_trades) * row_height) + bottom_padding

        image = Image.new("RGB", (width, height))
        draw = ImageDraw.Draw(image)

        # Gradient header background
        top_color = (120, 40, 0)
        bottom_color = (255, 140, 0)
        for y in range(header_height):
            blend = y / header_height
            r = int(top_color[0] * (1 - blend) + bottom_color[0] * blend)
            g = int(top_color[1] * (1 - blend) + bottom_color[1] * blend)
            b = int(top_color[2] * (1 - blend) + bottom_color[2] * blend)
            draw.line([(0, y), (width, y)], fill=(r, g, b))

        stripe_color = (25, 25, 25)
        font_color = (245, 245, 245)
        header_color = (255, 255, 255)

        header_font = ImageFont.truetype("RobotoCondensed-Bold.ttf", 30)
        body_font = ImageFont.truetype("RobotoCondensed-Regular.ttf", 28)

        for header, pos_info in self.positions.items():
            text_width = draw.textlength(header, font=header_font) if pos_info['align'] == 'right' else 0
            draw.text((pos_info['x'] - text_width if pos_info['align'] == 'right' else pos_info['x'], 15), header, font=header_font, fill=header_color)

        y_pos = header_height
        for i, trade in enumerate(page_trades):
            if i % 2 == 1:
                draw.rectangle([(0, y_pos), (width, y_pos + row_height)], fill=stripe_color)

            values = {
                'Ticker': trade.get("ticker"),
                'Quantity': format_large_number(trade.get("quantity")),
                'Price': f"{trade.get('price'):.2f}",
                'Value': format_large_number(trade.get("trade_value")),
                'Time': datetime.fromisoformat(trade.get("trade_time")).strftime("%Y-%m-%d %H:%M:%S")
            }

            for col_name, pos_info in self.positions.items():
                text = str(values.get(col_name, ''))
                text_y = y_pos + (row_height - body_font.size) / 2 - 2
                text_width = draw.textlength(text, font=body_font)
                x_pos = pos_info['x'] - text_width if pos_info.get('align') == 'right' else pos_info['x']
                draw.text((x_pos, text_y), text, font=body_font, fill=font_color)

            y_pos += row_height

        buffer = io.BytesIO()
        image.save(buffer, "PNG")
        buffer.seek(0)
        return discord.File(fp=buffer, filename="trades.png")

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="◀️")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(attachments=[self.generate_image_file()], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="▶️")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(attachments=[self.generate_image_file()], view=self)


# --- SLASH COMMAND GROUP CLASS ---
class DarkPoolCommands(app_commands.Group):
    async def run_paginated_command(self, interaction: discord.Interaction, endpoint_url: str, headers: list, positions: dict, title: str):
        await interaction.response.defer(thinking=True)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(endpoint_url)
            trades = response.json()
            if not trades:
                await interaction.followup.send(f"No data found for this command.")
                return

            view = PaginatorView(trades, headers, positions, title, interaction.user)
            await interaction.followup.send(content=title, file=view.generate_image_file(), view=view)
        except Exception as e:
            await interaction.followup.send(f"An unexpected error occurred: {e}")

    @app_commands.command(name="allblocks", description="Shows block trades for a stock.")
    @app_commands.describe(ticker="The stock ticker to look up (e.g., SPY)")
    async def allblocks(self, interaction: discord.Interaction, ticker: str):
        headers = ["Ticker", "Quantity", "Price", "Value", "Time"]
        positions = {
            "Ticker":   {"x": 100, "align": "right"},
            "Quantity": {"x": 220, "align": "right"},
            "Price":    {"x": 340, "align": "right"},
            "Value":    {"x": 460, "align": "right"},
            "Time":     {"x": 730, "align": "right"}
        }
        await self.run_paginated_command(interaction, f"{API_BASE_URL}/dp/allblocks/{ticker}", headers, positions, f"Block Trades for **{ticker.upper()}**:")

    @app_commands.command(name="alldp", description="Shows dark pool trades for a stock.")
    @app_commands.describe(ticker="The stock ticker to look up (e.g., AAPL)")
    async def alldp(self, interaction: discord.Interaction, ticker: str):
        headers = ["Ticker", "Quantity", "Price", "Value", "Time"]
        positions = {
            "Ticker":   {"x": 100, "align": "right"},
            "Quantity": {"x": 220, "align": "right"},
            "Price":    {"x": 340, "align": "right"},
            "Value":    {"x": 460, "align": "right"},
            "Time":     {"x": 730, "align": "right"}
        }
        await self.run_paginated_command(interaction, f"{API_BASE_URL}/dp/alldp/{ticker}", headers, positions, f"Dark Pool Trades for **{ticker.upper()}**:")

    @app_commands.command(name="bigprints", description="Shows the largest trades across the market today.")
    async def bigprints(self, interaction: discord.Interaction):
        headers = ["Ticker", "Quantity", "Price", "Value", "Time"]
        positions = {
            "Ticker":   {"x": 100, "align": "right"},
            "Quantity": {"x": 220, "align": "right"},
            "Price":    {"x": 340, "align": "right"},
            "Value":    {"x": 460, "align": "right"},
            "Time":     {"x": 730, "align": "right"}
        }
        await self.run_paginated_command(interaction, f"{API_BASE_URL}/dp/bigprints", headers, positions, "Today's Biggest Prints:")


@bot.event
async def on_ready():
    bot.tree.add_command(DarkPoolCommands(name="dp"))
    await bot.tree.sync()
    print(f"Bot has logged in as {bot.user} and slash commands synced.")

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        raise ValueError("Discord Bot Token not found.")
    bot.run(DISCORD_BOT_TOKEN)

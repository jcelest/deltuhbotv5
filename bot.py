import discord
from discord.ext import commands
import os
import httpx
from PIL import Image, ImageDraw, ImageFont
import io
from datetime import datetime

# --- BOT & API CONFIGURATION ---
DISCORD_BOT_TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
API_BASE_URL = "http://127.0.0.1:8001" # IMPORTANT: Using our new port 8001

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# --- HELPER FUNCTIONS ---

def format_large_number(num):
    """Formats a large number into a string with K, M, B suffixes."""
    if num is None: return "N/A"
    if num >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    if num >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    if num >= 1_000: return f"{num / 1_000:.2f}K"
    return str(num)

def generate_trade_image(trades: list, headers: list, positions: list):
    """Generates an image of a trade table from a list of trades."""
    width, height = 700, 450
    bg_color = (18, 18, 18)
    font_color = (220, 221, 222)
    header_color = (255, 255, 255)
    font_path = "consola.ttf"
    font_size = 15
    font = ImageFont.truetype(font_path, font_size)

    image = Image.new('RGB', (width, height), color=bg_color)
    draw = ImageDraw.Draw(image)

    # Draw Headers
    for i, header in enumerate(headers):
        draw.text((positions[i], 10), header, font=font, fill=header_color)

    # Draw Trades
    y_pos = 40
    for trade in trades[:15]:
        # Dynamically build the row based on headers
        row_values = []
        if 'Ticker' in headers: row_values.append(trade['ticker'])
        if 'Quantity' in headers: row_values.append(format_large_number(trade['quantity']))
        if 'Price' in headers: row_values.append(f"{trade['price']:.2f}")
        if 'Value' in headers: row_values.append(format_large_number(trade['trade_value']))
        if 'Time' in headers:
            dt_object = datetime.fromisoformat(trade['trade_time'])
            row_values.append(dt_object.strftime('%Y-%m-%d %H:%M:%S'))

        for i, value in enumerate(row_values):
            draw.text((positions[i], y_pos), value, font=font, fill=font_color)
        y_pos += 20

    buffer = io.BytesIO()
    image.save(buffer, 'PNG')
    buffer.seek(0)
    return buffer


# --- BOT EVENTS ---
@bot.event
async def on_ready():
    print(f'Bot has logged in as {bot.user}')
    print('Ready to receive commands!')


# --- BOT COMMANDS ---
@bot.command(name='allblocks')
async def allblocks(ctx, ticker: str):
    """Fetches and displays block trades for a given stock ticker."""
    await ctx.send(f"Searching for block trades for **{ticker.upper()}**...")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/dp/allblocks/{ticker}")
            response.raise_for_status()
        trades = response.json()
        if not trades:
            await ctx.send(f"No recent block trades found for **{ticker.upper()}**.")
            return

        headers = ["Quantity", "Price", "Value", "Time"]
        positions = [20, 150, 280, 420]
        image_buffer = generate_trade_image(trades, headers, positions)
        await ctx.send(file=discord.File(fp=image_buffer, filename=f'{ticker.upper()}_blocks.png'))
    except Exception as e:
        await ctx.send(f"An unexpected error occurred: {e}")

@bot.command(name='alldp')
async def alldp(ctx, ticker: str):
    """Fetches and displays dark pool trades for a given stock ticker."""
    await ctx.send(f"Searching for dark pool trades for **{ticker.upper()}**...")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/dp/alldp/{ticker}")
            response.raise_for_status()
        trades = response.json()
        if not trades:
            await ctx.send(f"No recent dark pool trades found for **{ticker.upper()}**.")
            return

        headers = ["Quantity", "Price", "Value", "Time"]
        positions = [20, 150, 280, 420]
        image_buffer = generate_trade_image(trades, headers, positions)
        await ctx.send(file=discord.File(fp=image_buffer, filename=f'{ticker.upper()}_darkpool.png'))
    except Exception as e:
        await ctx.send(f"An unexpected error occurred: {e}")

@bot.command(name='bigprints')
async def bigprints(ctx):
    """Fetches and displays the largest trades across the market today."""
    await ctx.send(f"Searching for today's biggest prints...")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/dp/bigprints")
            response.raise_for_status()
        trades = response.json()
        if not trades:
            await ctx.send(f"No big prints found for today yet.")
            return

        # Note the different headers and positions for this command
        headers = ["Ticker", "Quantity", "Price", "Value", "Time"]
        positions = [20, 120, 250, 380, 500]
        image_buffer = generate_trade_image(trades, headers, positions)
        await ctx.send(file=discord.File(fp=image_buffer, filename='big_prints.png'))
    except Exception as e:
        await ctx.send(f"An unexpected error occurred: {e}")


# --- RUN THE BOT ---
if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        raise ValueError("Discord Bot Token not found.")
    
    print("Starting Discord bot...")
    bot.run(DISCORD_BOT_TOKEN)
import asyncio
import aiohttp
from urllib.parse import quote

HEADERS_JSON = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

async def test_understat():
    league = "Bundesliga"
    season = 2024
    url = f"https://understat.com/getLeagueData/{quote(league)}/{season}"
    referer = f"https://understat.com/league/{league}/{season}"
    headers = HEADERS_JSON.copy()
    headers["Referer"] = referer
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            print("Status:", resp.status)
            if resp.status == 200:
                data = await resp.json()
                print("Keys:", data.keys())
            else:
                text = await resp.text()
                print("Text:", text[:100])

asyncio.run(test_understat())

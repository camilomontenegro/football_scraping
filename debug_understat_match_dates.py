import asyncio
import aiohttp
import json

async def main():
    url = 'https://understat.com/getLeagueData/La_Liga/2025'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://understat.com/league/La_Liga/2025',
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            print('status', resp.status)
            text = await resp.text()
    data = json.loads(text)
    ids = {int(m['id']): m for m in data['dates']}
    for mid in range(29490, 29538):
        if mid in ids:
            m = ids[mid]
            print(mid, m['datetime'], m['h']['title'], 'vs', m['a']['title'])
        else:
            print(mid, 'not found')

asyncio.run(main())
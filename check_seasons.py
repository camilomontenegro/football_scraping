import requests
url = 'https://api.sofascore.com/api/v1/unique-tournament/8/seasons'
resp = requests.get(url)
print(f"Status: {resp.status_code}")
data = resp.json()
print(f"Keys: {data.keys()}")
for s in data.get('seasons', []):
    print(f"{s['id']}: {s['name']}")
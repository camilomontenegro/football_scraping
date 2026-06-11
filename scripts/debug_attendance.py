"""Debug: inspecciona el HTML de spieltag (jornada) para ver donde esta attendance."""
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

url = "https://www.transfermarkt.es/laliga/spieltag/wettbewerb/ES1/saison_id/2024/spieltag/1"
print(f"Fetching: {url}\n")

r = requests.get(url, headers=HEADERS, timeout=20)
soup = BeautifulSoup(r.text, "html.parser")

# Show first 3 match rows
match_count = 0
for tr in soup.find_all("tr"):
    links = tr.find_all("a", class_="vereinprofil_tooltip")
    if not links:
        links = [a for a in tr.find_all("a", href=True)
                 if "/verein/" in a.get("href", "") and a.get_text(strip=True)]
    if len(links) >= 2:
        match_count += 1
        if match_count <= 3:
            print(f"=== MATCH ROW #{match_count} ===")
            print(f"\nALL TDs:")
            for i, td in enumerate(tr.find_all("td")):
                cls = td.get("class", [])
                txt = td.get_text(strip=True)
                print(f"  td[{i}] class={cls} text={txt!r}")
            print()

print(f"\nTotal match rows found: {match_count}")

if match_count == 0:
    print("\nNo match rows found. Checking page content...")
    print(f"Page title: {soup.title.get_text() if soup.title else 'N/A'}")
    print(f"Total TRs: {len(soup.find_all('tr'))}")
    # Show all tables
    for i, table in enumerate(soup.find_all("table")):
        rows = table.find_all("tr")
        print(f"  table[{i}] class={table.get('class',[])} rows={len(rows)}")

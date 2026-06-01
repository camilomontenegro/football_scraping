from pathlib import Path
root = Path('data/clean')
print(f"{'liga':25s}{'fuente':15s}{'archivos':50s}{'size'}")
print('-' * 110)
rows = []
for comp_dir in sorted(root.iterdir()):
    if not comp_dir.is_dir():
        continue
    season_dir = comp_dir / '2025_2026'
    if not season_dir.exists():
        continue
    for src_dir in sorted(season_dir.iterdir()):
        if not src_dir.is_dir():
            continue
        files = sorted(p.name for p in src_dir.iterdir() if p.is_file())
        sz = sum(p.stat().st_size for p in src_dir.iterdir() if p.is_file())
        names = ", ".join(files)
        print(f"{comp_dir.name:25s}{src_dir.name:15s}{names:50s}{sz:,}")

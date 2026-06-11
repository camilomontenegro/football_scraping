import pandas as pd

p = r"c:\Users\ivanm\Desktop\fact_events_202606100248.csv"
df = pd.read_csv(p, low_memory=False)
print("Filas:", len(df))
print("data_source:", df["data_source"].value_counts().to_dict())
print()

ang = df["angle"]
print("angle no nulo:", int(ang.notna().sum()))
print("angle nulo:", int(ang.isna().sum()))
print()

by_type = df.groupby("event_type").agg(
    total=("event_id", "count"),
    with_angle=("angle", lambda s: int(s.notna().sum())),
).sort_values("total", ascending=False)
by_type["pct_angle"] = (by_type["with_angle"] / by_type["total"] * 100).round(1)
print("Por event_type (top 20):")
print(by_type.head(20).to_string())
print()

has_angle_in_q = df["qualifiers"].astype(str).str.contains("Angle", na=False)
print("Filas con Angle en qualifiers JSON:", int(has_angle_in_q.sum()))
print("Angle en JSON pero columna angle vacia:", int((has_angle_in_q & df["angle"].isna()).sum()))
print("Columna angle llena pero sin Angle en JSON:", int(((~has_angle_in_q) & df["angle"].notna()).sum()))
print()

# WhoScored angle is pass direction in radians-ish - check value range
filled = df[df["angle"].notna()]
if len(filled):
    print("angle min/max/mean:", filled["angle"].min(), filled["angle"].max(), round(filled["angle"].mean(), 2))
    print("Tipos con angle relleno:")
    print(filled["event_type"].value_counts().head(10).to_string())
print()

no_ang_types = df[df["angle"].isna()]["event_type"].value_counts().head(12)
print("Tipos SIN angle (top):")
print(no_ang_types.to_string())
print()

# Shots / goals
for et in ["Shot", "Goal", "MissedShots", "SavedShot", "AttemptSaved", "Post"]:
    sub = df[df["event_type"] == et]
    if len(sub):
        print(f"{et}: n={len(sub)} angle={sub['angle'].notna().sum()}")

shot_kw = df[df["event_type"].str.contains("Shot|Goal|Attempt|Save", case=False, na=False)]
print(f"\nEventos shot/save/goal-like: {len(shot_kw)}, con angle: {shot_kw['angle'].notna().sum()}")

# Check if user expects shot angle vs pass angle
shots = df[df["event_type"].isin(["Shot", "Goal"]) | df["event_type"].str.contains("Shot", na=False)]
if len(shots):
    sample = shots.head(5)[["event_type", "angle", "goal_mouth_y", "goal_mouth_z", "qualifiers"]]
    print("\nMuestra tiros:")
    for _, r in sample.iterrows():
        print(r["event_type"], "angle=", r["angle"], "gm_y=", r["goal_mouth_y"], "qual=", str(r["qualifiers"])[:100])

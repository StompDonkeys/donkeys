#!/usr/bin/env python3
import json
import math
from pathlib import Path
from datetime import datetime, date, timedelta

import pandas as pd


# ---------------------------
# Config
# ---------------------------
TRACKED_PLAYERS = {"Armygeddon", "Jobby", "Miza", "Bucis", "Youare22"}
ALIASES = {
    "Misa": "Miza",
}
HANDICAP_MIN_ROUNDS = 5
HANDICAP_WINDOW = 20
HANDICAP_BEST = 8
HANDICAP_POINTS_PER_STROKE = 10.0
HANDICAP_CAP = 6.0  # +/- cap
BEST10_COUNT = 10


# ---------------------------
# Helpers
# ---------------------------
def parse_udisc_dt(s: str) -> datetime:
    # Example: "2025-12-22 0349" (no colon in time)
    s = str(s).strip()
    return datetime.strptime(s, "%Y-%m-%d %H%M")


def first_sunday(year: int, month: int) -> date:
    d = date(year, month, 1)
    # weekday(): Mon=0 ... Sun=6
    days_until_sun = (6 - d.weekday()) % 7
    return d + timedelta(days=days_until_sun)


def season_for(d: date) -> dict:
    """
    Season rules:
    - Summer starts first Sunday in October, ends first Saturday in April.
    - Winter starts first Sunday in April, ends first Saturday in October.

    We assign by the round date.
    """
    apr_start = first_sunday(d.year, 4)
    oct_start = first_sunday(d.year, 10)

    if d >= oct_start:
        # Summer spans across year boundary
        return {
            "season_type": "Summer",
            "season_label": f"Summer {d.year}-{str(d.year + 1)[-2:]}",
            "season_start": oct_start.isoformat(),
        }
    if d >= apr_start:
        return {
            "season_type": "Winter",
            "season_label": f"Winter {d.year}",
            "season_start": apr_start.isoformat(),
        }
    # Jan–Mar belongs to Summer that started in Oct of previous year
    prev_oct_start = first_sunday(d.year - 1, 10)
    return {
        "season_type": "Summer",
        "season_label": f"Summer {d.year - 1}-{str(d.year)[-2:]}",
        "season_start": prev_oct_start.isoformat(),
    }


def round_half(x: float) -> float:
    return round(x * 2.0) / 2.0


def safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


# ---------------------------
# Main
# ---------------------------
def main():
    repo_root = Path(__file__).resolve().parents[1]
    csv_path = repo_root / "data-source" / "UDisc Scorecards.csv"
    out_dir = repo_root / "_data"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        raise SystemExit(f"CSV not found at: {csv_path}")

    df = pd.read_csv(csv_path)

    # Canonicalise player names (alias + strip)
    df["PlayerName"] = df["PlayerName"].astype(str).str.strip()
    df["PlayerName"] = df["PlayerName"].apply(lambda n: ALIASES.get(n, n))

    # Parse datetimes
    df["StartDT"] = df["StartDate"].apply(parse_udisc_dt)
    df["EndDT"] = df["EndDate"].apply(parse_udisc_dt)

    # Enforce "full 18-hole rounds" = Holes 1-18 present AND no extra holes 19+
    hole_1_18 = [f"Hole{i}" for i in range(1, 19)]
    hole_19_plus = [f"Hole{i}" for i in range(19, 28)]

    has_all_18 = ~df[hole_1_18].isna().any(axis=1)
    has_extra = df[hole_19_plus].notna().any(axis=1)
    full_18_only = has_all_18 & (~has_extra)

    # Filter to tracked players + full 18 only
    df = df[df["PlayerName"].isin(TRACKED_PLAYERS) & full_18_only].copy()

    # Rating handling
    df["RoundRatingNum"] = pd.to_numeric(df["RoundRating"], errors="coerce")
    df["IsRated"] = df["RoundRatingNum"].notna()

    # Season tagging
    df["RoundDate"] = df["StartDT"].dt.date
    season_meta = df["RoundDate"].apply(season_for)
    df["SeasonType"] = season_meta.apply(lambda x: x["season_type"])
    df["SeasonLabel"] = season_meta.apply(lambda x: x["season_label"])

    # Layout-aware key
    df["CourseKey"] = df["CourseName"].astype(str).str.strip() + " — " + df["LayoutName"].astype(str).str.strip()

    # Create a stable round id
    df["RoundId"] = (
        df["PlayerName"].astype(str)
        + "|"
        + df["CourseKey"].astype(str)
        + "|"
        + df["StartDate"].astype(str)
        + "|"
        + df["Total"].astype(str)
    ).apply(lambda s: str(abs(hash(s))))

    # Build rounds.json (keep it tidy and web-friendly)
    rounds = []
    for _, r in df.sort_values("StartDT").iterrows():
        rounds.append(
            {
                "round_id": r["RoundId"],
                "player": r["PlayerName"],
                "course": str(r["CourseName"]).strip(),
                "layout": str(r["LayoutName"]).strip(),
                "course_key": r["CourseKey"],
                "start": r["StartDT"].isoformat(),
                "end": r["EndDT"].isoformat(),
                "total": safe_float(r["Total"]),
                "plus_minus": str(r["+/−"]) if "+/−" in df.columns else str(r.get("+/-", "")),
                "rating": safe_float(r["RoundRatingNum"]),
                "is_rated": bool(r["IsRated"]),
                "season_type": r["SeasonType"],
                "season_label": r["SeasonLabel"],
            }
        )

    # --- Season ladders: Sum of best 10 ratings per season
    ladders = []
    rated_df = df[df["IsRated"]].copy()
    for (season_label, player), g in rated_df.groupby(["SeasonLabel", "PlayerName"]):
        ratings = sorted(g["RoundRatingNum"].dropna().astype(float).tolist(), reverse=True)
        top10 = ratings[:BEST10_COUNT]
        ladders.append(
            {
                "season_label": season_label,
                "player": player,
                "season_total_best10": float(sum(top10)) if top10 else None,
                "rated_rounds_in_season": int(len(ratings)),
                "counted_rounds": int(len(top10)),
                "best_round_rating": float(max(ratings)) if ratings else None,
            }
        )

    # --- Handicaps: best 8 of last 20 rated rounds (rounded to 0.5, capped +/-6)
    handicaps = []
    player_form = {}

    for player, g in rated_df.groupby("PlayerName"):
        g2 = g.sort_values("StartDT", ascending=False)
        last20 = g2.head(HANDICAP_WINDOW)
        ratings = last20["RoundRatingNum"].dropna().astype(float).tolist()

        if len(ratings) < HANDICAP_MIN_ROUNDS:
            player_form[player] = None
            continue

        best8 = sorted(ratings, reverse=True)[:HANDICAP_BEST]
        form_index = sum(best8) / len(best8)
        player_form[player] = form_index

    eligible_forms = [v for v in player_form.values() if v is not None]
    reference = sum(eligible_forms) / len(eligible_forms) if eligible_forms else None

    for player in sorted(TRACKED_PLAYERS):
        form_index = player_form.get(player)
        if reference is None or form_index is None:
            handicaps.append(
                {
                    "player": player,
                    "handicap": None,
                    "form_index": form_index,
                    "reference_rating": reference,
                    "method": f"best {HANDICAP_BEST} of last {HANDICAP_WINDOW} rated rounds; {HANDICAP_POINTS_PER_STROKE:g} pts = 1 stroke",
                    "status": "No handicap yet (insufficient rated rounds)",
                }
            )
            continue

        raw = (reference - form_index) / HANDICAP_POINTS_PER_STROKE
        h = round_half(raw)
        h = max(-HANDICAP_CAP, min(HANDICAP_CAP, h))

        handicaps.append(
            {
                "player": player,
                "handicap": float(h),
                "form_index": float(form_index),
                "reference_rating": float(reference),
                "method": f"best {HANDICAP_BEST} of last {HANDICAP_WINDOW} rated rounds; {HANDICAP_POINTS_PER_STROKE:g} pts = 1 stroke",
                "status": "OK",
            }
        )

    # --- Players summary (simple starter pack)
    players = []
    for player, g in df.groupby("PlayerName"):
        rated = g[g["IsRated"]]
        players.append(
            {
                "player": player,
                "rounds": int(len(g)),
                "rated_rounds": int(len(rated)),
                "best_rating": float(rated["RoundRatingNum"].max()) if len(rated) else None,
                "avg_rating": float(rated["RoundRatingNum"].mean()) if len(rated) else None,
            }
        )

    # Write outputs
    (out_dir / "rounds.json").write_text(json.dumps(rounds, indent=2), encoding="utf-8")
    (out_dir / "ladders.json").write_text(json.dumps(ladders, indent=2), encoding="utf-8")
    (out_dir / "handicaps.json").write_text(json.dumps(handicaps, indent=2), encoding="utf-8")
    (out_dir / "players.json").write_text(json.dumps(players, indent=2), encoding="utf-8")

    print("Wrote:")
    print(" - _data/rounds.json")
    print(" - _data/ladders.json")
    print(" - _data/handicaps.json")
    print(" - _data/players.json")


if __name__ == "__main__":
    main()

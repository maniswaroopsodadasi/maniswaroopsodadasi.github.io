#!/usr/bin/env python3
"""
Test YouTube uploads for Day 1-6 articles.

Usage:
    python test_youtube_upload.py           # uploads Day 1-6
    python test_youtube_upload.py 3         # uploads only Day 3
    python test_youtube_upload.py 1 2 3     # uploads Days 1, 2, 3
"""
import os
import sys
import json
from pathlib import Path

# ── Load YouTube credentials from local token file if not already in env ──────
_tokens_file = Path("youtube_tokens_local.json")
if not (os.getenv("YOUTUBE_CLIENT_SECRET_B64") and os.getenv("YOUTUBE_REFRESH_TOKEN")):
    if _tokens_file.exists():
        _t = json.loads(_tokens_file.read_text())
        os.environ["YOUTUBE_CLIENT_SECRET_B64"] = _t["client_secret_b64"]
        os.environ["YOUTUBE_REFRESH_TOKEN"]     = _t["refresh_token"]
        print("✅ Loaded YouTube credentials from youtube_tokens_local.json")
    else:
        print("❌  YouTube credentials not found.")
        print("   Run  python youtube_oauth_setup.py  first.")
        sys.exit(1)

# Use local-only mode so GitHub/LinkedIn creds are not required
os.environ["FABRIC_LOCAL_ONLY"] = "1"
# Enable AI narration scripts (uses GEMINI_API_KEY / ANTHROPIC_API_KEY if set)
os.environ.setdefault("FABRIC_USE_AI", "true")

# ── Load HeyGen key (primary) ─────────────────────────────────────────────────
_heygen_file = Path("heygen_key.txt")
if not os.getenv("HEYGEN_API_KEY") and _heygen_file.exists():
    os.environ["HEYGEN_API_KEY"] = _heygen_file.read_text().strip()
    print("✅ Loaded HeyGen key from heygen_key.txt")

# ── Load D-ID key (fallback) ──────────────────────────────────────────────────
_did_file = Path("did_key.txt")
if not os.getenv("DID_API_KEY") and _did_file.exists():
    os.environ["DID_API_KEY"] = _did_file.read_text().strip()
    print("✅ Loaded D-ID key from did_key.txt")

# ── Import after env is configured ────────────────────────────────────────────
from full_automation_system import (
    FullAutomationSystem,
    slugify_fabric_article,
    _resolve_website_url,
    _resolve_github_repo,
)


def main():
    # Days to upload — default 1-6, or pass day numbers as CLI args
    if len(sys.argv) > 1:
        try:
            days = [int(x) for x in sys.argv[1:]]
        except ValueError:
            print("Usage: python test_youtube_upload.py [day1] [day2] ...")
            sys.exit(1)
    else:
        days = list(range(1, 7))

    heygen_key = os.getenv("HEYGEN_API_KEY", "").strip()
    did_key    = os.getenv("DID_API_KEY", "").strip()
    if heygen_key:
        print("🎭  Presenter: HeyGen ON  — 12-slide 1080p + animated talking-head avatar")
    elif did_key:
        print("🎭  Presenter: D-ID ON   — 12-slide 1080p + D-ID avatar (HeyGen fallback)")
    else:
        print("🎭  Presenter: OFF       — static image + TTS")
        print("    To enable: echo 'your_key' > heygen_key.txt")

    print(f"\n🎬  YouTube upload test — Days: {days}")
    print("=" * 55)

    system = FullAutomationSystem()

    if not system.youtube_api.enabled:
        print("❌  YouTube API not enabled — check credentials")
        sys.exit(1)

    # Build a lookup of already-published article URLs
    pub_map = {a["day"]: a for a in system.published_articles}
    website_url = _resolve_website_url(_resolve_github_repo())

    results = {}

    for day in days:
        print(f"\n📺  Day {day} — ", end="", flush=True)

        if day < 1 or day > len(system.content_generator.content_bank):
            print(f"no content for day {day}, skipping")
            continue

        day_content = system.content_generator.content_bank[day - 1]
        title = day_content.get("title", f"Day {day}")
        print(title)

        # Resolve article URL (use published URL if available, else build it)
        if day in pub_map:
            article_url = pub_map[day]["url"]
        else:
            slug = slugify_fabric_article(day, title)
            article_url = f"{website_url}/articles/fabric-100-days/{slug}.html"

        # Generate image for video thumbnail (also used in the video frame)
        print("    🖼  Generating branded image…", flush=True)
        diagram = day_content.get("diagram")
        if not diagram:
            diagram = system._generate_diagram_data(
                day, title, day_content.get("category", "Data Engineering")
            )
        concepts = [
            h.replace("_", " ")
            for h in (day_content.get("hashtags") or [])
            if h not in ("MicrosoftFabric", "100DaysChallenge", "Azure", "Analytics")
        ]
        post_image = system.linkedin_api.generate_post_image(
            day, title,
            day_content.get("category", "Data Engineering"),
            concepts,
            diagram=diagram,
        )

        # Generate video + upload
        print("    🎙  Generating narration + video + uploading…", flush=True)
        video_id = system.generate_and_upload_youtube(
            day, day_content, article_url, post_image
        )

        results[day] = video_id

        if video_id:
            print(f"    ✅  https://youtu.be/{video_id}")
            # Update published_articles.json with the video ID
            updated = False
            for a in system.published_articles:
                if a.get("day") == day:
                    a["youtube_video_id"] = video_id
                    updated = True
                    break
            if not updated:
                system.published_articles.append({
                    "day":            day,
                    "title":          title,
                    "url":            article_url,
                    "youtube_video_id": video_id,
                })
            # Save locally (not pushed to GitHub here — next CI run will sync)
            with open("published_articles.json", "w", encoding="utf-8") as fh:
                json.dump(system.published_articles, fh, indent=2)
        else:
            print(f"    ❌  Upload failed (check logs above)")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("Summary:")
    for day, vid in results.items():
        if vid:
            print(f"  Day {day:3d} ✅  https://youtu.be/{vid}")
        else:
            print(f"  Day {day:3d} ❌  failed")

    print()
    ok_count = sum(1 for v in results.values() if v)
    print(f"{ok_count}/{len(results)} uploaded successfully.")
    if ok_count < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()

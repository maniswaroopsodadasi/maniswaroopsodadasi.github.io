#!/usr/bin/env python3
"""
Microsoft Fabric 100 Days - FULL AUTOMATION SYSTEM
=================================================

Complete automation with LinkedIn API integration:
1. Generates articles daily at 9 AM IST
2. Updates website automatically
3. Posts to LinkedIn automatically with article links
4. Runs continuously for 100 days

Setup:
- Set GITHUB_TOKEN environment variable
- Set LINKEDIN_ACCESS_TOKEN environment variable  
- Set LINKEDIN_PERSON_ID environment variable
- Run: python full_automation_system.py
"""

import argparse
import html
import os
import json
import re
import requests
import schedule
import sys
import time
import datetime
import pytz
from typing import Dict, List
import base64
from pathlib import Path
from urllib.parse import quote
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fabric_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DEFAULT_GITHUB_REPO = "maniswaroopsodadasi/maniswaroopsodadasi.github.io"

# Marker pair in articles/index.html — replaced each publish by update_articles_hub_page()
FABRIC_HUB_BEGIN = "<!-- FABRIC_HUB_AUTO_BEGIN -->"
FABRIC_HUB_END = "<!-- FABRIC_HUB_AUTO_END -->"

# Marker pair in index.html (portfolio) — replaced each publish by update_portfolio_page()
FABRIC_PORTFOLIO_BEGIN = "<!-- FABRIC_PORTFOLIO_AUTO_BEGIN -->"
FABRIC_PORTFOLIO_END = "<!-- FABRIC_PORTFOLIO_AUTO_END -->"


def slugify_fabric_article(day: int, title: str) -> str:
    """URL-safe filename stem (no .html) for fabric-100-days articles."""
    t = title.lower()
    t = re.sub(r"[^a-z0-9\s-]", "", t)
    t = re.sub(r"\s+", "-", t.strip())
    t = t.replace("&", "and")
    t = re.sub(r"-+", "-", t).strip("-")
    return f"day-{day}-{t}"


def normalize_fabric_article_urls(text: str, article_url: str) -> str:
    """Point any in-repo fabric-100-days links in predefined copy to the canonical article URL."""
    if not text or not article_url:
        return text
    return re.sub(
        r"https?://[^\s)\]]+/articles/fabric-100-days/[^\s)\]]+",
        article_url,
        text,
        flags=re.IGNORECASE,
    )


def _resolve_github_repo() -> str:
    """Use GITHUB_REPOSITORY in Actions; override locally with GITHUB_REPO."""
    return (
        os.getenv("GITHUB_REPOSITORY")
        or os.getenv("GITHUB_REPO")
        or DEFAULT_GITHUB_REPO
    )


def _resolve_website_url(repo: str) -> str:
    """Public site URL for article links. Override with WEBSITE_URL when needed."""
    explicit = os.getenv("WEBSITE_URL")
    if explicit:
        return explicit.rstrip("/")
    try:
        owner, name = repo.split("/", 1)
    except ValueError:
        return "https://maniswaroopsodadasi.github.io"
    if name == f"{owner}.github.io":
        return f"https://{owner}.github.io"
    return f"https://{owner}.github.io/{name}"


class LinkedInAPI:
    """LinkedIn API integration for automated posting"""

    # Share on LinkedIn (UGC): author must be the Person URN for the *same* member as the access token.
    # Docs: https://learn.microsoft.com/en-us/linkedin/consumer/integrations/self-serve/share-on-linkedin
    USERINFO_URL = "https://api.linkedin.com/v2/userinfo"

    def __init__(self, access_token: str, person_id: str):
        self.access_token = access_token
        self.person_id = (person_id or "").strip()
        self.base_url = "https://api.linkedin.com/v2"

        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
            # Newer versions may validate /author differently; override with LINKEDIN_API_VERSION if needed
            "Linkedin-Version": os.getenv("LINKEDIN_API_VERSION", "202304"),
        }

    def _author_urn_for_ugc(self) -> str:
        """
        Resolve author so it matches the authenticated member (403 if token user ≠ author).

        Microsoft "Share on LinkedIn" uses Person URN: urn:li:person:{id}

        Priority:
        1. LINKEDIN_AUTHOR_URN — full URN
        2. GET /v2/userinfo `sub` — **use before /v2/me**: OIDC `sub` (e.g. botDMcOL-E) is what UGC expects for
           OpenID tokens. `/v2/me` id can be a different numeric id and will 403 on /author.
        3. GET /v2/me?projection=(id) — fallback if no OpenID
        4. LINKEDIN_PERSON_ID — manual
        """
        explicit = (os.getenv("LINKEDIN_AUTHOR_URN") or "").strip()
        if explicit:
            # urn:li:member: is rejected by both REST Posts and ugcPosts — auto-convert
            if explicit.startswith("urn:li:member:"):
                converted = "urn:li:person:" + explicit[len("urn:li:member:"):]
                logger.info("LINKEDIN_AUTHOR_URN: converted %s → %s", explicit, converted)
                return converted
            return explicit

        # 2) /v2/me numeric id — reliable person URN for both REST Posts and ugcPosts
        try:
            r = requests.get(
                f"{self.base_url}/me",
                headers=self.headers,
                params={"projection": "(id)"},
                timeout=10,
            )
            if r.status_code == 200:
                mid = r.json().get("id")
                if mid:
                    urn = f"urn:li:person:{mid}"
                    logger.info("Author URN from /v2/me id → %s", urn)
                    return urn
            logger.info(
                "/v2/me not available (%s). Ensure r_liteprofile scope or set LINKEDIN_PERSON_ID.",
                r.status_code,
            )
        except Exception as e:
            logger.debug("/v2/me failed: %s", e)

        # 3) OpenID userinfo sub — only use if sub looks like a numeric person id
        try:
            oid_headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            }
            r = requests.get(self.USERINFO_URL, headers=oid_headers, timeout=10)
            if r.status_code != 200:
                r = requests.get(
                    self.USERINFO_URL,
                    headers={**self.headers, "Accept": "application/json"},
                    timeout=10,
                )
            if r.status_code == 200:
                sub = r.json().get("sub")
                if sub:
                    urn = f"urn:li:person:{sub}"
                    logger.info("Author URN from userinfo sub → %s", urn)
                    return urn
            if r.status_code == 403:
                logger.info(
                    "userinfo returned 403. Re-authorize for openid+profile or set LINKEDIN_PERSON_ID. %s",
                    r.text[:300],
                )
            else:
                logger.debug("userinfo HTTP %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.debug("userinfo failed: %s", e)

        pid = self.person_id
        if pid.startswith("urn:li:member:"):
            # member URN type is rejected by REST Posts API — convert to person URN
            numeric_id = pid[len("urn:li:member:"):]
            urn = f"urn:li:person:{numeric_id}"
            logger.info("Converted urn:li:member: → %s", urn)
            return urn
        if pid.startswith(("urn:li:person:", "urn:li:organization:")):
            return pid
        if pid:
            urn = f"urn:li:person:{pid}"
            logger.info("Author URN from LINKEDIN_PERSON_ID → %s", urn)
            return urn

        raise ValueError(
            "Could not resolve LinkedIn author URN. Fix one of: "
            "(1) OAuth scopes: add r_liteprofile so GET /v2/me works, OR openid+profile for userinfo; "
            "(2) set LINKEDIN_PERSON_ID to your LinkedIn member profile id (same user as this token); "
            "(3) set LINKEDIN_AUTHOR_URN=urn:li:person:YOUR_ID"
        )

    def _post_via_rest_posts(self, author_urn: str, content: str, image_urn: str = "") -> Dict:
        """
        POST https://api.linkedin.com/rest/posts — current API (replaces ugcPosts for many apps).
        Docs: https://learn.microsoft.com/en-us/linkedin/marketing/community-management/shares/posts-api
        """
        rest_version = os.getenv("LINKEDIN_REST_VERSION", "202602")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
            "Linkedin-Version": rest_version,
        }
        payload = {
            "author": author_urn,
            "commentary": content,
            "visibility": "PUBLIC",
            "distribution": {
                "feedDistribution": "MAIN_FEED",
                "targetEntities": [],
                "thirdPartyDistributionChannels": [],
            },
            "lifecycleState": "PUBLISHED",
            "isReshareDisabledByAuthor": False,
        }
        if image_urn:
            payload["content"] = {"media": {"id": image_urn}}
        r = requests.post(
            "https://api.linkedin.com/rest/posts",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if r.status_code == 201:
            # Prefer x-restli-id header — this is the ugcPost URN needed for future edits
            post_id = r.headers.get("x-restli-id") or ""
            share_id = ""
            try:
                if r.text:
                    share_id = r.json().get("id", "")
                    if not post_id:
                        post_id = share_id
            except Exception:
                pass
            logger.info("LinkedIn REST Posts: ugcPost URN=%s  share URN=%s", post_id, share_id)
            return {
                "success": True,
                "post_id": post_id,
                "share_id": share_id,
                "message": "Posted successfully (REST Posts API)",
            }
        return {
            "success": False,
            "error": f"{r.status_code}: {r.text}",
            "status": r.status_code,
        }

    def _post_via_ugc_posts(self, author_urn: str, content: str, image_urn: str = "") -> Dict:
        """Legacy POST /v2/ugcPosts (Share on LinkedIn consumer doc)."""
        share_content: Dict = {
            "shareCommentary": {"text": content},
            "shareMediaCategory": "IMAGE" if image_urn else "NONE",
        }
        if image_urn:
            share_content["media"] = [{"status": "READY", "media": image_urn}]
        payload = {
            "author": author_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": share_content,
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            },
        }
        response = requests.post(
            f"{self.base_url}/ugcPosts",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        if response.status_code == 201:
            post_data = response.json()
            post_id = post_data.get("id", "")
            return {
                "success": True,
                "post_id": post_id,
                "message": "Posted successfully (legacy UGC)",
            }
        return {
            "success": False,
            "error": f"{response.status_code}: {response.text}",
        }

    # ------------------------------------------------------------------ #
    #  Branded image generation + LinkedIn image upload                   #
    # ------------------------------------------------------------------ #

    def generate_post_image(self, day: int, title: str, category: str, concepts: list[str] = None, diagram: dict = None) -> bytes | None:
        """
        Generate a beautiful 1200×627 branded infographic PNG for LinkedIn.
        Layout: left panel (author + day + title) | right panel (concept pills grid).
        Returns raw PNG bytes or None if Pillow is unavailable.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageFilter
        except ImportError:
            logger.warning("Pillow not installed — skipping image. pip install Pillow")
            return None

        import io, math

        W, H = 1200, 627
        SPLIT = 520          # left panel width
        PAD   = 48

        # ── Colour palette ────────────────────────────────────────────────
        BG_LEFT   = (8,   52,  58)   # deep teal (left panel)
        BG_RIGHT  = (12,  68,  75)   # slightly lighter (right panel)
        ACCENT    = (56, 212, 196)   # vivid mint
        ACCENT2   = (34, 170, 155)   # darker mint
        GOLD      = (255, 200,  80)  # warm highlight for day number
        WHITE     = (255, 255, 255)
        OFFWHITE  = (210, 238, 235)
        MUTED     = (120, 175, 170)
        DARK      = (6,   42,  48)
        PILL_COLS = [
            # (fill, outline, text)
            ((20, 110, 108), (56, 212, 196), WHITE),     # teal filled
            ((0,   0,   0,   0), (56, 212, 196), OFFWHITE),  # outlined
            ((255,200,80, 220), (255,200,80), DARK),     # gold filled
            ((30, 140, 130), (34, 170, 155), WHITE),     # mid-teal filled
            ((0,   0,   0,   0), (120,175,170), MUTED),  # subtle outlined
        ]

        img  = Image.new("RGB", (W, H))
        draw = ImageDraw.Draw(img)

        # ── Backgrounds ───────────────────────────────────────────────────
        # Left panel gradient (top-to-bottom dark → slightly lighter)
        for y in range(H):
            t = y / H
            r = int(BG_LEFT[0] + t * 6)
            g = int(BG_LEFT[1] + t * 10)
            b = int(BG_LEFT[2] + t * 10)
            draw.line([(0, y), (SPLIT, y)], fill=(r, g, b))

        # Right panel
        for y in range(H):
            t = y / H
            r = int(BG_RIGHT[0] + t * 4)
            g = int(BG_RIGHT[1] + t * 6)
            b = int(BG_RIGHT[2] + t * 6)
            draw.line([(SPLIT, y), (W, y)], fill=(r, g, b))

        # Subtle dot grid on right panel
        for gx in range(SPLIT + 20, W, 36):
            for gy in range(20, H, 36):
                draw.ellipse([gx-1, gy-1, gx+1, gy+1], fill=(30, 95, 100))

        # Glowing orb behind pills (soft circle, right panel centre)
        orb_x, orb_y, orb_r = (SPLIT + W) // 2, H // 2, 220
        for step in range(30, 0, -1):
            alpha = int(18 * (step / 30))
            rr = orb_r * step // 30
            draw.ellipse(
                [orb_x - rr, orb_y - rr, orb_x + rr, orb_y + rr],
                fill=(56, 212, 196) if step > 15 else (34, 170, 155),
            )
        # Re-draw right bg over orb to make it just a glow hint
        for y in range(H):
            t = y / H
            dist_x = abs((SPLIT + W) // 2 - W // 2)
            r2 = int(BG_RIGHT[0] + t * 4)
            g2 = int(BG_RIGHT[1] + t * 6)
            b2 = int(BG_RIGHT[2] + t * 6)
            for x in range(SPLIT, W):
                dx = x - orb_x
                dy = y - orb_y
                d  = math.sqrt(dx*dx + dy*dy)
                glow = max(0.0, 1.0 - d / orb_r) * 0.18
                draw.point((x, y), fill=(
                    min(255, int(r2 + glow * 56)),
                    min(255, int(g2 + glow * 212)),
                    min(255, int(b2 + glow * 196)),
                ))

        # Vertical separator with glow
        draw.rectangle([SPLIT - 2, 0, SPLIT, H], fill=ACCENT2)
        draw.rectangle([SPLIT,     0, SPLIT + 2, H], fill=(30, 90, 95))

        # ── Font loader ───────────────────────────────────────────────────
        def load_font(size, bold=False):
            paths_bold = [
                "/System/Library/Fonts/Helvetica.ttc",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
            ]
            paths_reg = [
                "/System/Library/Fonts/Helvetica.ttc",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
            ]
            for path in (paths_bold if bold else paths_reg):
                try:
                    return ImageFont.truetype(path, size)
                except Exception:
                    pass
            return ImageFont.load_default()

        f_small   = load_font(18)
        f_med     = load_font(22)
        f_large   = load_font(28, bold=True)
        f_day     = load_font(96, bold=True)
        f_title   = load_font(38, bold=True)
        f_pill    = load_font(19)
        f_brand   = load_font(17)

        # ── LEFT PANEL ────────────────────────────────────────────────────

        # Avatar circle + author name (top)
        av_r  = 26
        av_cx, av_cy = PAD + av_r, 40

        # Try to load profile photo; fall back to initials circle
        photo_loaded = False
        photo_candidates = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile_photo.png"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile_photo.jpg"),
            "profile_photo.png",
            "profile_photo.jpg",
        ]
        for photo_path in photo_candidates:
            if os.path.exists(photo_path):
                try:
                    avatar_src = Image.open(photo_path).convert("RGBA")
                    # Crop to square from centre
                    aw, ah = avatar_src.size
                    side    = min(aw, ah)
                    left    = (aw - side) // 2
                    top     = max(0, ah // 6 - side // 8)   # shift slightly up to favour face
                    top     = min(top, ah - side)
                    avatar_src = avatar_src.crop((left, top, left + side, top + side))
                    diam    = av_r * 2
                    avatar_src = avatar_src.resize((diam, diam), Image.LANCZOS)
                    # Circular mask
                    mask    = Image.new("L", (diam, diam), 0)
                    md      = ImageDraw.Draw(mask)
                    md.ellipse([0, 0, diam, diam], fill=255)
                    avatar_rgba = Image.new("RGBA", (diam, diam), (0, 0, 0, 0))
                    avatar_rgba.paste(avatar_src, (0, 0), mask)
                    # Accent ring around avatar
                    ring = Image.new("RGBA", (diam + 4, diam + 4), (0, 0, 0, 0))
                    rd   = ImageDraw.Draw(ring)
                    rd.ellipse([0, 0, diam + 3, diam + 3], outline=ACCENT, width=2)
                    img.paste(ring, (av_cx - av_r - 2, av_cy - av_r - 2), ring)
                    img.paste(avatar_rgba, (av_cx - av_r, av_cy - av_r), avatar_rgba)
                    photo_loaded = True
                    break
                except Exception:
                    pass

        if not photo_loaded:
            draw.ellipse([av_cx-av_r, av_cy-av_r, av_cx+av_r, av_cy+av_r], fill=ACCENT)
            draw.text((av_cx - 10, av_cy - 13), "MS", font=load_font(18, bold=True), fill=DARK)

        draw.text((av_cx + av_r + 14, av_cy - 16), "Mani Swaroop", font=f_med, fill=WHITE)
        draw.text((av_cx + av_r + 14, av_cy + 6),  "Senior Data & AI Engineer", font=f_small, fill=MUTED)

        # Series label
        draw.text((PAD, 90), "100 DAYS OF MICROSOFT FABRIC", font=f_small, fill=ACCENT2)
        draw.line([(PAD, 114), (SPLIT - PAD, 114)], fill=(30, 90, 95), width=1)

        # Day number (large, gold)
        day_str  = f"Day {day}"
        d_bb     = draw.textbbox((0, 0), day_str, font=f_day)
        draw.text((PAD, 120), day_str, font=f_day, fill=GOLD)

        # "/ 100" aligned to baseline of day number
        slash_x = PAD + (d_bb[2] - d_bb[0]) + 14
        draw.text((slash_x, 182), "/ 100", font=f_large, fill=ACCENT)

        # Progress bar under day number
        bar_y   = 240
        bar_w   = SPLIT - PAD * 2
        prog    = min(1.0, day / 100)
        draw.rounded_rectangle([PAD, bar_y, PAD + bar_w, bar_y + 6], radius=3, fill=(25, 80, 85))
        if prog > 0:
            draw.rounded_rectangle([PAD, bar_y, PAD + int(bar_w * prog), bar_y + 6], radius=3, fill=ACCENT)

        # Category pill
        cat_text = f"  {category.upper()}  "
        cat_bb   = draw.textbbox((0, 0), cat_text, font=f_small)
        cat_w    = cat_bb[2] - cat_bb[0] + 8
        cat_h    = cat_bb[3] - cat_bb[1] + 10
        draw.rounded_rectangle([PAD, 260, PAD + cat_w, 260 + cat_h], radius=cat_h // 2, fill=ACCENT)
        draw.text((PAD + 4, 264), cat_text.strip(), font=f_small, fill=DARK)

        # Title (word-wrapped, max 3 lines, left panel width minus padding)
        MAX_TITLE_W = SPLIT - PAD * 2
        words, t_lines, t_line = title.split(), [], []
        for w in words:
            test = " ".join(t_line + [w])
            bb   = draw.textbbox((0, 0), test, font=f_title)
            if bb[2] - bb[0] > MAX_TITLE_W and t_line:
                t_lines.append(" ".join(t_line))
                t_line = [w]
            else:
                t_line.append(w)
        if t_line:
            t_lines.append(" ".join(t_line))

        y_t = 295
        for ln in t_lines[:3]:
            draw.text((PAD, y_t), ln, font=f_title, fill=OFFWHITE)
            y_t += 50

        # ── RIGHT PANEL ───────────────────────────────────────────────────
        R_PAD   = 32
        R_LEFT  = SPLIT + R_PAD
        R_RIGHT = W - R_PAD
        R_TOP   = 90
        R_BOT   = H - 70

        # ── Diagram: Hub & Spoke ──────────────────────────────────────────
        def draw_hub_spoke(d):
            center_label = d.get("center", "Hub")
            nodes        = d.get("nodes", [])
            N = len(nodes)
            if N == 0:
                return
            cx = (R_LEFT + R_RIGHT) // 2
            cy = (R_TOP  + R_BOT)  // 2
            radius = min(185, (min(R_RIGHT - R_LEFT, R_BOT - R_TOP) // 2) - 55)
            fn_center = load_font(17, bold=True)
            fn_node   = load_font(14)

            # Lines first so shapes paint over them
            for i in range(N):
                angle = -math.pi / 2 + i * 2 * math.pi / N
                nx = int(cx + radius * math.cos(angle))
                ny = int(cy + radius * math.sin(angle))
                draw.line([(cx, cy), (nx, ny)], fill=ACCENT2, width=2)

            # Center rounded rect
            cbw, cbh = 126, 52
            draw.rounded_rectangle(
                [cx - cbw//2, cy - cbh//2, cx + cbw//2, cy + cbh//2],
                radius=12, fill=ACCENT, outline=WHITE, width=2
            )
            c_lines = center_label.split("\n")
            line_h  = 20
            y_start = cy - (len(c_lines) * line_h) // 2
            for li, ln in enumerate(c_lines):
                bb = draw.textbbox((0, 0), ln, font=fn_center)
                tw = bb[2] - bb[0]
                draw.text((cx - tw // 2, y_start + li * line_h), ln, font=fn_center, fill=DARK)

            # Peripheral nodes
            nw, nh = 116, 38
            for i, node in enumerate(nodes):
                angle = -math.pi / 2 + i * 2 * math.pi / N
                nx = int(cx + radius * math.cos(angle))
                ny = int(cy + radius * math.sin(angle))
                # Clamp to right-panel bounds
                nx = max(R_LEFT + nw//2 + 4, min(R_RIGHT - nw//2 - 4, nx))
                ny = max(R_TOP  + nh//2 + 4, min(R_BOT  - nh//2 - 4, ny))
                draw.rounded_rectangle(
                    [nx - nw//2, ny - nh//2, nx + nw//2, ny + nh//2],
                    radius=nh // 2, fill=(20, 110, 108), outline=ACCENT, width=2
                )
                n_lines = node.split("\n")
                nl_h    = 16
                ny_start = ny - (len(n_lines) * nl_h) // 2
                for li, ln in enumerate(n_lines):
                    # Truncate if too wide
                    bb = draw.textbbox((0, 0), ln, font=fn_node)
                    tw = bb[2] - bb[0]
                    while tw > nw - 10 and len(ln) > 3:
                        ln = ln[:-1]
                        bb = draw.textbbox((0, 0), ln + "…", font=fn_node)
                        tw = bb[2] - bb[0]
                    draw.text((nx - tw // 2, ny_start + li * nl_h), ln, font=fn_node, fill=WHITE)

        # ── Diagram: Comparison Table ─────────────────────────────────────
        def draw_comparison(d):
            cols = d.get("columns", [])
            rows = d.get("rows",    [])
            if not cols or not rows:
                return
            nc      = len(cols)
            tw      = R_RIGHT - R_LEFT - 10
            col_w   = tw // nc
            hdr_h   = 40
            row_h   = 42
            total_h = hdr_h + len(rows) * row_h
            tx      = R_LEFT + 5
            ty      = max(R_TOP + 8, (R_TOP + R_BOT - total_h) // 2)
            fn_hdr  = load_font(14, bold=True)
            fn_cell = load_font(13)

            hdr_fills = [(50, 80, 85), ACCENT, ACCENT2, (38, 155, 142)]

            # Header
            for ci, col in enumerate(cols):
                x0   = tx + ci * col_w
                fill = hdr_fills[ci % len(hdr_fills)]
                draw.rectangle([x0, ty, x0 + col_w, ty + hdr_h], fill=fill)
                draw.rectangle([x0, ty, x0 + col_w, ty + hdr_h], outline=(30, 90, 95), width=1)
                bb = draw.textbbox((0, 0), col, font=fn_hdr)
                cw2, ch2 = bb[2] - bb[0], bb[3] - bb[1]
                text_col = DARK if fill == ACCENT else WHITE
                draw.text((x0 + (col_w - cw2) // 2, ty + (hdr_h - ch2) // 2), col, font=fn_hdr, fill=text_col)

            # Data rows
            row_bgs = [(15, 75, 80), (10, 58, 64)]
            for ri, row in enumerate(rows):
                ry2  = ty + hdr_h + ri * row_h
                bg   = row_bgs[ri % 2]
                for ci in range(nc):
                    x0   = tx + ci * col_w
                    draw.rectangle([x0, ry2, x0 + col_w, ry2 + row_h], fill=bg)
                    draw.rectangle([x0, ry2, x0 + col_w, ry2 + row_h], outline=(30, 90, 95), width=1)
                    orig = row[ci] if ci < len(row) else ""
                    cell = orig
                    bb   = draw.textbbox((0, 0), cell, font=fn_cell)
                    cw2  = bb[2] - bb[0]
                    while cw2 > col_w - 8 and len(cell) > 3:
                        cell = cell[:-1]
                        bb   = draw.textbbox((0, 0), cell + "…", font=fn_cell)
                        cw2  = bb[2] - bb[0]
                    if cell != orig:
                        cell += "…"
                    bb   = draw.textbbox((0, 0), cell, font=fn_cell)
                    cw2, ch2 = bb[2] - bb[0], bb[3] - bb[1]
                    text_col = OFFWHITE if ci > 0 else ACCENT
                    draw.text((x0 + (col_w - cw2) // 2, ry2 + (row_h - ch2) // 2), cell, font=fn_cell, fill=text_col)

        # ── Diagram: Capacity Tiers ───────────────────────────────────────
        def draw_tiers(d):
            tiers = d.get("tiers", [])
            N = len(tiers)
            if N == 0:
                return
            panel_w  = R_RIGHT - R_LEFT
            bar_w    = min(68, (panel_w - 20) // (N + 1))
            total_bw = bar_w * N
            gap      = max(8, (panel_w - total_bw) // (N + 1))
            baseline = R_BOT - 44
            max_bh   = baseline - R_TOP - 44
            fn_name  = load_font(16, bold=True)
            fn_det   = load_font(12)

            for i, tier in enumerate(tiers):
                frac = 0.22 + 0.78 * i / max(N - 1, 1)
                bh   = int(max_bh * frac)
                bx   = R_LEFT + gap + i * (bar_w + gap)
                by   = baseline - bh

                # Colour interpolation ACCENT2 → ACCENT
                r = int(ACCENT2[0] + (ACCENT[0] - ACCENT2[0]) * i / max(N - 1, 1))
                g = int(ACCENT2[1] + (ACCENT[1] - ACCENT2[1]) * i / max(N - 1, 1))
                b = int(ACCENT2[2] + (ACCENT[2] - ACCENT2[2]) * i / max(N - 1, 1))

                draw.rounded_rectangle([bx, by, bx + bar_w, baseline], radius=4, fill=(r, g, b))
                # Top accent cap
                cap_col = GOLD if i == N - 1 else WHITE
                draw.rectangle([bx, by, bx + bar_w, by + 3], fill=cap_col)

                # Tier name above bar
                name = tier.get("name", "")
                bb   = draw.textbbox((0, 0), name, font=fn_name)
                nw2  = bb[2] - bb[0]
                draw.text((bx + (bar_w - nw2) // 2, by - 26), name, font=fn_name, fill=GOLD)

                # Detail below baseline
                detail = tier.get("detail", "")
                parts  = detail.split(" ")
                for pi, part in enumerate(parts[:2]):
                    bb2 = draw.textbbox((0, 0), part, font=fn_det)
                    dw  = bb2[2] - bb2[0]
                    draw.text((bx + (bar_w - dw) // 2, baseline + 6 + pi * 15), part, font=fn_det, fill=MUTED)

        # ── Diagram: Flow / Pipeline ──────────────────────────────────────
        def draw_flow(d):
            steps = d.get("steps", [])
            N = len(steps)
            if N == 0:
                return
            panel_w  = R_RIGHT - R_LEFT
            arrow_w  = 26
            box_w    = min(118, (panel_w - arrow_w * (N - 1)) // N)
            box_h    = 54
            mid_y    = (R_TOP + R_BOT) // 2
            row_y    = mid_y - box_h // 2
            total_w  = N * box_w + (N - 1) * arrow_w
            start_x  = R_LEFT + (panel_w - total_w) // 2
            fn_lbl   = load_font(14, bold=True) if N <= 5 else load_font(12, bold=True)
            fn_step  = load_font(11)
            box_fills = [(20, 110, 108), (30, 140, 130)]

            for i, step in enumerate(steps):
                bx   = start_x + i * (box_w + arrow_w)
                fill = box_fills[i % 2]
                draw.rounded_rectangle([bx, row_y, bx + box_w, row_y + box_h],
                                        radius=8, fill=fill, outline=ACCENT, width=2)

                # Step number
                draw.text((bx + 6, row_y + 4), str(i + 1), font=fn_step, fill=GOLD)

                # Label — word-wrap within box
                words = step.split()
                lines, cur = [], []
                for w in words:
                    test = " ".join(cur + [w])
                    bb   = draw.textbbox((0, 0), test, font=fn_lbl)
                    if bb[2] - bb[0] > box_w - 10 and cur:
                        lines.append(" ".join(cur)); cur = [w]
                    else:
                        cur.append(w)
                if cur:
                    lines.append(" ".join(cur))
                lines   = lines[:2]
                line_h  = 17
                text_h  = len(lines) * line_h
                ty2     = mid_y - text_h // 2
                for li, ln in enumerate(lines):
                    bb  = draw.textbbox((0, 0), ln, font=fn_lbl)
                    lw  = bb[2] - bb[0]
                    draw.text((bx + (box_w - lw) // 2, ty2 + li * line_h), ln, font=fn_lbl, fill=WHITE)

                # Arrow
                if i < N - 1:
                    ax0 = bx + box_w + 2
                    ax1 = bx + box_w + arrow_w - 2
                    draw.line([(ax0, mid_y), (ax1 - 6, mid_y)], fill=ACCENT, width=2)
                    draw.polygon([(ax1, mid_y), (ax1 - 8, mid_y - 5), (ax1 - 8, mid_y + 5)], fill=ACCENT)

        # ── Dispatch ──────────────────────────────────────────────────────
        def draw_pills():
            if not concepts:
                pass
            defaults = {
                "foundations":      ["Microsoft Fabric", "OneLake", "Data Lakehouse", "Unified Analytics", "Azure"],
                "data engineering": ["Data Pipeline", "ETL/ELT", "Delta Lake", "Apache Spark", "Medallion"],
                "analytics":        ["Power BI", "DAX", "Semantic Model", "Reports", "DirectLake"],
                "governance":       ["Data Governance", "Purview", "Lineage", "Row-Level Security", "Compliance"],
            }
            extra     = defaults.get(category.lower(), defaults["foundations"])
            all_pills = list(dict.fromkeys((concepts or []) + extra))[:12]
            GAP_X, GAP_Y = 12, 14
            PH, P_PAD_X  = 38, 18
            pill_sizes = []
            for pt in all_pills:
                bb = draw.textbbox((0, 0), pt, font=f_pill)
                pill_sizes.append((pt, bb[2] - bb[0] + P_PAD_X * 2, PH))
            rows2: list = []
            cur_row2: list = []
            cur_x2 = R_LEFT
            for pt, pw, ph in pill_sizes:
                if cur_x2 + pw > R_RIGHT and cur_row2:
                    rows2.append(cur_row2); cur_row2 = []; cur_x2 = R_LEFT
                cur_row2.append((pt, pw, ph)); cur_x2 += pw + GAP_X
            if cur_row2:
                rows2.append(cur_row2)
            total_h2 = len(rows2) * (PH + GAP_Y) - GAP_Y
            start_y2 = max(R_TOP, (R_TOP + R_BOT - total_h2) // 2)
            pidx = 0
            for row2 in rows2:
                row_w2 = sum(pw for _, pw, _ in row2) + GAP_X * (len(row2) - 1)
                rx2    = R_LEFT + max(0, (R_RIGHT - R_LEFT - row_w2) // 2)
                for pt, pw, ph in row2:
                    style = PILL_COLS[pidx % len(PILL_COLS)]
                    fill2, outline2, text_col2 = style
                    if fill2 == (0, 0, 0, 0):
                        draw.rounded_rectangle([rx2, start_y2, rx2+pw, start_y2+ph], radius=ph//2, outline=outline2, width=2)
                    else:
                        draw.rounded_rectangle([rx2, start_y2, rx2+pw, start_y2+ph], radius=ph//2, fill=fill2, outline=outline2, width=1)
                    tb = draw.textbbox((0, 0), pt, font=f_pill)
                    draw.text((rx2+(pw-(tb[2]-tb[0]))//2, start_y2+(ph-(tb[3]-tb[1]))//2-1), pt, font=f_pill, fill=text_col2)
                    rx2 += pw + GAP_X; pidx += 1
                start_y2 += PH + GAP_Y

        if diagram and isinstance(diagram, dict):
            dtype = diagram.get("type", "")
            if   dtype == "hub_spoke":  draw_hub_spoke(diagram)
            elif dtype == "comparison": draw_comparison(diagram)
            elif dtype == "tiers":      draw_tiers(diagram)
            elif dtype == "flow":       draw_flow(diagram)
            else:                       draw_pills()
        else:
            draw_pills()

        # ── BOTTOM BAR ────────────────────────────────────────────────────
        bar_top = H - 52
        draw.rectangle([0, bar_top, W, H], fill=DARK)
        draw.line([(0, bar_top), (W, bar_top)], fill=ACCENT, width=2)

        follow = "Follow Mani Swaroop for more about Data & AI Engineering"
        draw.text((PAD, bar_top + 15), follow, font=f_brand, fill=OFFWHITE)

        site  = "maniswaroopsodadasi.github.io"
        sb    = draw.textbbox((0, 0), site, font=f_brand)
        draw.text((W - (sb[2] - sb[0]) - PAD, bar_top + 15), site, font=f_brand, fill=ACCENT)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    def upload_image_to_linkedin(self, author_urn: str, image_bytes: bytes) -> str | None:
        """
        Upload image to LinkedIn Images API (two-step: initializeUpload → PUT binary).
        Returns asset URN (urn:li:image:...) or None on failure.
        """
        rest_version = os.getenv("LINKEDIN_REST_VERSION", "202602")
        base_headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
            "Linkedin-Version": rest_version,
        }
        try:
            r = requests.post(
                "https://api.linkedin.com/rest/images?action=initializeUpload",
                headers=base_headers,
                json={"initializeUploadRequest": {"owner": author_urn}},
                timeout=30,
            )
        except Exception as e:
            logger.warning("Image upload init error: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Image upload init failed %s: %s", r.status_code, r.text[:300])
            return None

        data      = r.json().get("value", {})
        upload_url = data.get("uploadUrl", "")
        asset_urn  = data.get("image", "")
        if not upload_url or not asset_urn:
            logger.warning("Image upload init missing fields: %s", data)
            return None

        try:
            r2 = requests.put(
                upload_url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                data=image_bytes,
                timeout=60,
            )
        except Exception as e:
            logger.warning("Image binary upload error: %s", e)
            return None

        if r2.status_code not in (200, 201):
            logger.warning("Image binary upload failed %s: %s", r2.status_code, r2.text[:200])
            return None

        logger.info("✅ LinkedIn image uploaded: %s", asset_urn)
        return asset_urn

    def post_to_linkedin(self, content: str, image_bytes: bytes | None = None) -> Dict:
        """Post to LinkedIn: try REST Posts API first, then legacy ugcPosts."""

        try:
            try:
                author_urn = self._author_urn_for_ugc()
            except ValueError as ve:
                logger.error("%s", ve)
                return {"success": False, "error": str(ve)}

            logger.info("LinkedIn UGC author URN: %s", author_urn)

            # Upload image once; pass asset URN to whichever post path succeeds
            image_urn = ""
            if image_bytes:
                image_urn = self.upload_image_to_linkedin(author_urn, image_bytes) or ""
                if not image_urn:
                    logger.warning("Image upload failed — posting without image")

            use_rest_first = os.getenv("LINKEDIN_USE_REST_POSTS", "true").lower() in (
                "1",
                "true",
                "yes",
            )
            if use_rest_first:
                rest = self._post_via_rest_posts(author_urn, content, image_urn)
                if rest.get("success"):
                    return rest
                logger.warning(
                    "REST Posts API failed (%s). Trying legacy /v2/ugcPosts…",
                    (rest.get("error") or "")[:400],
                )

            ugc = self._post_via_ugc_posts(author_urn, content, image_urn)
            if ugc.get("success"):
                return ugc

            logger.error("LinkedIn UGC Error: %s", ugc.get("error", ""))
            return {
                "success": False,
                "error": ugc.get("error", "Unknown"),
            }

        except Exception as e:
            logger.error(f"LinkedIn posting error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def test_connection(self) -> bool:
        """Test LinkedIn API connection (token valid for member APIs)."""
        try:
            r_ui = requests.get(
                self.USERINFO_URL,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Accept": "application/json",
                },
                timeout=10,
            )
            if r_ui.status_code == 200:
                return True
            r_me = requests.get(
                f"{self.base_url}/me",
                headers=self.headers,
                timeout=10,
            )
            if r_me.status_code == 200:
                return True
            r_person = requests.get(
                f"{self.base_url}/people/(id:{self.person_id})",
                headers=self.headers,
                params={"projection": "(id)"},
                timeout=10,
            )
            if r_person.status_code == 200:
                return True
            logger.warning(
                "LinkedIn preflight: userinfo -> %s; /me -> %s; people/(id:) -> %s — %s",
                r_ui.status_code,
                r_me.status_code,
                r_person.status_code,
                (r_ui.text or r_me.text)[:300],
            )
            return False
        except Exception as e:
            logger.warning("LinkedIn preflight error: %s", e)
            return False


class YouTubeAPI:
    """YouTube Data API v3 — video upload via OAuth2 refresh-token (no browser in CI)."""

    TOKEN_URL = "https://oauth2.googleapis.com/token"

    def __init__(self):
        self.client_secret_b64 = os.getenv("YOUTUBE_CLIENT_SECRET_B64", "")
        self.refresh_token     = os.getenv("YOUTUBE_REFRESH_TOKEN", "")
        self.enabled = bool(self.client_secret_b64.strip() and self.refresh_token.strip())
        if not self.enabled:
            logger.info(
                "YouTube integration disabled "
                "(set YOUTUBE_CLIENT_SECRET_B64 + YOUTUBE_REFRESH_TOKEN to enable)"
            )

    def _client_info(self) -> Dict:
        """Decode the base64-encoded client_secret JSON."""
        import base64
        # Add padding if stripped
        b64 = self.client_secret_b64.strip()
        b64 += "=" * (-len(b64) % 4)
        raw = json.loads(base64.b64decode(b64))
        return raw.get("installed") or raw.get("web", {})

    def _get_access_token(self) -> str:
        """Exchange stored refresh token for a short-lived access token."""
        ci = self._client_info()
        r = requests.post(
            self.TOKEN_URL,
            data={
                "client_id":     ci["client_id"],
                "client_secret": ci["client_secret"],
                "refresh_token": self.refresh_token,
                "grant_type":    "refresh_token",
            },
            timeout=20,
        )
        if r.status_code != 200:
            raise RuntimeError(f"YouTube token refresh failed {r.status_code}: {r.text[:300]}")
        return r.json()["access_token"]

    def upload_video(
        self,
        video_path: str,
        title: str,
        description: str,
        tags: List[str],
        thumbnail_bytes: bytes = None,
    ) -> str:
        """
        Upload *video_path* to YouTube via resumable upload.
        Returns the YouTube video ID (e.g. 'dQw4w9WgXcQ') or empty string on failure.
        """
        try:
            access_token = self._get_access_token()
            auth_headers = {"Authorization": f"Bearer {access_token}"}

            # ── Step 1: Initiate resumable upload session ──────────────────
            file_size = os.path.getsize(video_path)
            metadata = {
                "snippet": {
                    "title": title[:100],
                    "description": description[:5000],
                    "tags": tags[:15],
                    "categoryId": "28",       # Science & Technology
                    "defaultLanguage": "en",
                },
                "status": {
                    "privacyStatus": "public",
                    "selfDeclaredMadeForKids": False,
                },
            }
            init_r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?uploadType=resumable&part=snippet,status",
                headers={
                    **auth_headers,
                    "Content-Type": "application/json; charset=UTF-8",
                    "X-Upload-Content-Type": "video/mp4",
                    "X-Upload-Content-Length": str(file_size),
                },
                json=metadata,
                timeout=30,
            )
            init_r.raise_for_status()
            upload_url = init_r.headers["Location"]

            # ── Step 2: PUT the video bytes ────────────────────────────────
            with open(video_path, "rb") as fh:
                video_bytes = fh.read()

            up_r = requests.put(
                upload_url,
                headers={
                    **auth_headers,
                    "Content-Type": "video/mp4",
                    "Content-Length": str(file_size),
                },
                data=video_bytes,
                timeout=300,
            )
            up_r.raise_for_status()
            video_id = up_r.json().get("id", "")
            logger.info("✅ YouTube upload complete: https://youtu.be/%s", video_id)

            # ── Step 3: Custom thumbnail ───────────────────────────────────
            if thumbnail_bytes and video_id:
                try:
                    th_r = requests.post(
                        f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
                        f"?videoId={video_id}&uploadType=media",
                        headers={**auth_headers, "Content-Type": "image/png"},
                        data=thumbnail_bytes,
                        timeout=60,
                    )
                    if th_r.status_code == 200:
                        logger.info("✅ YouTube thumbnail set for %s", video_id)
                    else:
                        logger.warning(
                            "Thumbnail upload HTTP %s: %s", th_r.status_code, th_r.text[:200]
                        )
                except Exception as te:
                    logger.warning("Thumbnail set error (non-fatal): %s", te)

            return video_id

        except Exception as e:
            logger.error("YouTube upload error: %s", e)
            return ""


class DIDApi:
    """D-ID Talks API — animated talking-head video from a source photo + narration text.

    Sign up at https://www.d-id.com — free trial includes ~5 minutes of video.
    Required env var: DID_API_KEY
    Optional env var: DID_PRESENTER_URL (defaults to GitHub Pages profile photo)
    """

    BASE_URL      = "https://api.d-id.com"
    DEFAULT_VOICE = "en-US-ChristopherNeural"   # professional male voice (Microsoft TTS)

    def __init__(self):
        self.api_key       = os.getenv("DID_API_KEY", "").strip()
        self.enabled       = bool(self.api_key)
        self.presenter_url = os.getenv(
            "DID_PRESENTER_URL",
            "https://maniswaroopsodadasi.github.io/profile_photo.png",
        )
        if not self.enabled:
            logger.info(
                "D-ID integration disabled (set DID_API_KEY to enable presenter videos)"
            )

    def _headers(self) -> Dict:
        token = base64.b64encode(f"{self.api_key}:".encode()).decode()
        return {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }

    def create_talk(self, narration: str, voice_id: str = DEFAULT_VOICE) -> str:
        """Submit a talk job. Returns talk_id string or '' on failure."""
        payload = {
            "source_url": self.presenter_url,
            "script": {
                "type": "text",
                "input": narration[:2000],
                "provider": {"type": "microsoft", "voice_id": voice_id},
            },
            "config": {"fluent": True, "pad_audio": 0.5, "stitch": True},
        }
        try:
            r = requests.post(
                f"{self.BASE_URL}/talks",
                headers=self._headers(),
                json=payload,
                timeout=30,
            )
            if r.status_code in (200, 201):
                talk_id = r.json().get("id", "")
                logger.info("D-ID talk submitted: %s", talk_id)
                return talk_id
            logger.error("D-ID create_talk %s: %s", r.status_code, r.text[:400])
        except Exception as e:
            logger.error("D-ID create_talk error: %s", e)
        return ""

    def wait_for_talk(self, talk_id: str, timeout: int = 360) -> str:
        """Poll until done. Returns result_url or '' on failure/timeout."""
        import time
        deadline = time.time() + timeout
        logger.info("Waiting for D-ID render (talk_id=%s, up to %ds)…", talk_id, timeout)
        while time.time() < deadline:
            try:
                r = requests.get(
                    f"{self.BASE_URL}/talks/{talk_id}",
                    headers=self._headers(),
                    timeout=15,
                )
                if r.status_code == 200:
                    data   = r.json()
                    status = data.get("status", "")
                    if status == "done":
                        url = data.get("result_url", "")
                        logger.info("✅ D-ID render complete: %s", url)
                        return url
                    if status == "error":
                        logger.error("D-ID render error: %s", data.get("error") or data.get("description"))
                        return ""
                    logger.debug("D-ID status: %s", status)
                elif r.status_code == 429:
                    logger.warning("D-ID rate limit — waiting 30s")
                    time.sleep(30)
            except Exception as e:
                logger.warning("D-ID poll error: %s", e)
            time.sleep(6)
        logger.error("D-ID talk %s timed out after %ds", talk_id, timeout)
        return ""

    def download_result(self, result_url: str, output_path: str) -> bool:
        """Stream-download the completed .mp4 to output_path."""
        try:
            r = requests.get(result_url, timeout=180, stream=True)
            r.raise_for_status()
            with open(output_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
            logger.info("✅ D-ID video saved: %s", output_path)
            return True
        except Exception as e:
            logger.error("D-ID download error: %s", e)
            return False


class AnimatedAvatarGenerator:
    """
    Completely FREE, unlimited animated avatar — no API, no model downloads.

    Creates a professional "podcast speaker" style animated avatar video:
      • Profile photo in a circle, gently breathing (zoom in/out)
      • Concentric accent rings that pulse with audio amplitude
      • Periodic eye-blink simulation (slight brightness dip)
      • Name card below the avatar
      • Audio-reactive glow around the circle

    Uses: Pillow (frames) + pydub or librosa (audio analysis) + ffmpeg (video)
    Speed: ~30–60 s on any hardware — works fine in GitHub Actions.
    """

    FPS = 30

    def __init__(self):
        self.avatar_size = 500          # output circle diameter in px
        self.bg_color    = (8, 52, 58)  # brand BG
        self.accent      = (56, 212, 196)
        self.gold        = (255, 200, 80)
        self.dark        = (6, 42, 48)

    # ── Audio analysis ────────────────────────────────────────────────────

    def _load_amplitude_envelope(self, audio_path: str, fps: int = FPS) -> List[float]:
        """
        Return a list of per-frame RMS amplitude values (0.0–1.0), one per video frame.
        Tries librosa first, falls back to pydub + numpy.
        """
        try:
            import librosa, numpy as np
            y, sr = librosa.load(audio_path, sr=None, mono=True)
            hop   = sr // fps
            rms   = librosa.feature.rms(y=y, hop_length=hop)[0].astype(np.float32)
            mx    = rms.max() or 1.0
            return [float(v / mx) for v in rms]
        except Exception:
            pass

        # Fallback: pydub + numpy
        try:
            from pydub import AudioSegment
            import numpy as np
            audio   = AudioSegment.from_file(audio_path).set_channels(1)
            samples = np.array(audio.get_array_of_samples()).astype(np.float32)
            sr      = audio.frame_rate
            hop     = sr // fps
            frames  = [
                float(np.sqrt(np.mean(samples[i:i+hop]**2)))
                for i in range(0, len(samples) - hop, hop)
            ]
            mx = max(frames) or 1.0
            return [v / mx for v in frames]
        except Exception as e:
            logger.warning("Audio amplitude analysis failed: %s — using silence", e)
            return []

    def _get_audio_duration(self, audio_path: str) -> float:
        """Return audio duration in seconds."""
        import subprocess
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", audio_path],
                capture_output=True, text=True, timeout=15,
            )
            return float(r.stdout.strip()) if r.returncode == 0 else 60.0
        except Exception:
            return 60.0

    # ── Frame rendering ───────────────────────────────────────────────────

    def _render_frame(
        self,
        photo_img,           # PIL Image, already square + RGBA
        frame_idx: int,
        amplitude: float,    # 0.0–1.0
        name: str,
        title_line: str,
        size: int,
    ):
        """Render one avatar frame as a PIL Image (RGBA, size×size+footer)."""
        try:
            from PIL import Image, ImageDraw, ImageFilter
        except ImportError:
            return None

        import math

        canvas_h = size + 90   # extra space for name card below
        img  = Image.new("RGBA", (size, canvas_h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        cx, cy = size // 2, size // 2
        r      = (size // 2) - 20   # face circle radius

        # ── Glow rings (amplitude-driven) ─────────────────────────────────
        n_rings = 3
        for i in range(n_rings, 0, -1):
            ring_r   = r + 10 + i * 14 + int(amplitude * 18 * i / n_rings)
            alpha    = int(60 * amplitude * (1 - (i - 1) / n_rings))
            ring_col = (*self.accent, max(10, alpha))
            draw.ellipse(
                [cx - ring_r, cy - ring_r, cx + ring_r, cy + ring_r],
                outline=ring_col,
                width=max(1, int(2 + amplitude * 3)),
            )

        # ── Accent border ring ─────────────────────────────────────────────
        border_r = r + 6
        draw.ellipse(
            [cx - border_r, cy - border_r, cx + border_r, cy + border_r],
            outline=(*self.accent, 255), width=4,
        )

        # ── Face photo (breathing: ±4px radius over ~4 s cycle) ───────────
        breathe   = 1.0 + 0.008 * math.sin(2 * math.pi * frame_idx / (4 * self.FPS))
        face_r    = max(1, int(r * breathe))
        face_diam = face_r * 2
        face_resized = photo_img.resize((face_diam, face_diam), Image.LANCZOS)

        # Circular mask
        mask = Image.new("L", (face_diam, face_diam), 0)
        ImageDraw.Draw(mask).ellipse([0, 0, face_diam, face_diam], fill=255)
        face_rgba = Image.new("RGBA", (face_diam, face_diam), (0, 0, 0, 0))
        face_rgba.paste(face_resized, (0, 0), mask)

        img.paste(face_rgba, (cx - face_r, cy - face_r), face_rgba)

        # ── Blink: dim eyes area briefly every ~4 s ────────────────────────
        blink_cycle = self.FPS * 4
        blink_phase = frame_idx % blink_cycle
        if blink_phase < 3:   # 3 frames ≈ 0.1 s blink
            eye_y = cy - r // 5
            blink_strip = Image.new("RGBA", (face_r * 2, max(1, r // 6)), (0, 0, 0, 120))
            img.paste(blink_strip, (cx - face_r, eye_y), blink_strip)

        # ── Name card ─────────────────────────────────────────────────────
        try:
            from PIL import ImageFont
            def lf(sz, bold=False):
                for p in (["/System/Library/Fonts/Helvetica.ttc",
                           "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
                          if bold else
                          ["/System/Library/Fonts/Helvetica.ttc",
                           "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]):
                    try: return ImageFont.truetype(p, sz)
                    except: pass
                return ImageFont.load_default()

            name_font  = lf(24, bold=True)
            title_font = lf(18)
            nb  = draw.textbbox((0, 0), name,       font=name_font)
            tb  = draw.textbbox((0, 0), title_line, font=title_font)
            nx  = (size - (nb[2]-nb[0])) // 2
            tx  = (size - (tb[2]-tb[0])) // 2
            y_base = size + 8
            draw.text((nx, y_base),      name,       font=name_font,  fill=(*self.accent, 255))
            draw.text((tx, y_base + 30), title_line, font=title_font, fill=(180, 220, 215, 200))
        except Exception:
            pass

        return img

    # ── Public method ─────────────────────────────────────────────────────

    def generate(
        self,
        photo_path: str,
        audio_path: str,
        output_path: str,
        name: str        = "Mani Swaroop",
        title_line: str  = "Senior Data & AI Engineer",
    ) -> bool:
        """
        Generate an animated avatar MP4 synced to audio_path.
        Returns True on success.
        """
        try:
            from PIL import Image
        except ImportError:
            logger.warning("Pillow not available — animated avatar skipped")
            return False

        import subprocess, math, os, tempfile as _tf

        # ── Load profile photo ────────────────────────────────────────────
        try:
            photo = Image.open(photo_path).convert("RGBA")
            aw, ah = photo.size
            s  = min(aw, ah)
            tl = (aw - s) // 2
            tp = max(0, ah // 6 - s // 8)
            tp = min(tp, ah - s)
            photo = photo.crop((tl, tp, tl + s, tp + s))
            photo = photo.resize((self.avatar_size, self.avatar_size), Image.LANCZOS)
        except Exception as e:
            logger.error("Avatar photo load error: %s", e)
            return False

        # ── Get audio info ────────────────────────────────────────────────
        duration   = self._get_audio_duration(audio_path)
        n_frames   = int(duration * self.FPS)
        amplitudes = self._load_amplitude_envelope(audio_path, self.FPS)
        # Pad/trim to exactly n_frames
        if len(amplitudes) < n_frames:
            amplitudes += [0.0] * (n_frames - len(amplitudes))
        amplitudes = amplitudes[:n_frames]

        logger.info("Animated avatar: %.1f s, %d frames", duration, n_frames)

        # ── Render frames to temp dir + combine with ffmpeg ───────────────
        with _tf.TemporaryDirectory() as fdir:
            frame_size  = self.avatar_size
            canvas_h    = frame_size + 90
            smooth_amp  = 0.0
            batch_size  = 150   # write frames in batches of 5s to save RAM

            # Write all frames as PNGs
            for i in range(n_frames):
                raw_amp    = amplitudes[i]
                smooth_amp = smooth_amp * 0.6 + raw_amp * 0.4   # smooth flicker
                frame_img  = self._render_frame(
                    photo, i, smooth_amp,
                    name, title_line, frame_size,
                )
                if frame_img is None:
                    continue
                # Composite onto brand background
                bg = Image.new("RGBA", (frame_size, canvas_h), (*self.bg_color, 255))
                bg.paste(frame_img, (0, 0), frame_img)
                bg.convert("RGB").save(
                    os.path.join(fdir, f"frame_{i:06d}.png"), "PNG"
                )

                if i % 300 == 0:
                    logger.debug("Avatar frames: %d/%d", i, n_frames)

            # ffmpeg: frames → video, mux audio
            cmd = [
                "ffmpeg", "-y",
                "-framerate", str(self.FPS),
                "-i", os.path.join(fdir, "frame_%06d.png"),
                "-i", audio_path,
                "-vf", "format=yuv420p",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest",
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error("Avatar ffmpeg error: %s", result.stderr[-600:])
                return False

        logger.info("✅ Animated avatar video: %s", output_path)
        return True


class HeyGenAPI:
    """HeyGen Talking Photo API — animates your real photo into a speaking avatar.

    Sign up at https://app.heygen.com — free trial included.
    Required env var: HEYGEN_API_KEY
    Optional:
      HEYGEN_VOICE_ID  — voice ID from https://app.heygen.com/voices
                         Default: en-US-GuyNeural (professional male)
    """

    BASE_URL         = "https://api.heygen.com"
    DEFAULT_VOICE_ID = "en-US-GuyNeural"   # professional male voice

    def __init__(self):
        self.api_key  = os.getenv("HEYGEN_API_KEY", "").strip()
        self.enabled  = bool(self.api_key)
        self.voice_id = os.getenv("HEYGEN_VOICE_ID", self.DEFAULT_VOICE_ID)
        self._photo_id_cache: str = ""   # avoid re-uploading same photo
        if not self.enabled:
            logger.info("HeyGen disabled (set HEYGEN_API_KEY to enable animated avatar)")

    def _h(self) -> Dict:
        return {"X-Api-Key": self.api_key, "Content-Type": "application/json"}

    def upload_photo(self, photo_path: str) -> str:
        """Upload profile photo → talking_photo_id (cached across calls)."""
        if self._photo_id_cache:
            return self._photo_id_cache
        mime = "image/png" if photo_path.lower().endswith(".png") else "image/jpeg"
        try:
            with open(photo_path, "rb") as fh:
                r = requests.post(
                    f"{self.BASE_URL}/v1/talking_photo",
                    headers={"X-Api-Key": self.api_key},
                    files={"file": (os.path.basename(photo_path), fh, mime)},
                    timeout=60,
                )
            if r.status_code in (200, 201):
                data = r.json()
                # response: {"data": {"talking_photo_id": "xxx"}} or {"talking_photo_id": "xxx"}
                pid = (
                    data.get("data", {}).get("talking_photo_id")
                    or data.get("talking_photo_id", "")
                )
                if pid:
                    self._photo_id_cache = pid
                    logger.info("✅ HeyGen photo uploaded: %s", pid)
                    return pid
            logger.error("HeyGen photo upload %s: %s", r.status_code, r.text[:400])
        except Exception as e:
            logger.error("HeyGen upload_photo error: %s", e)
        return ""

    def create_video(self, script: str, photo_id: str, dimension: Dict = None) -> str:
        """Submit video generation. Returns video_id or '' on failure."""
        dim = dimension or {"width": 512, "height": 512}
        payload = {
            "video_inputs": [{
                "character": {
                    "type": "talking_photo",
                    "talking_photo_id": photo_id,
                    "talking_photo_style": "circle",
                },
                "voice": {
                    "type": "text",
                    "input_text": script[:3000],
                    "voice_id": self.voice_id,
                    "speed": 1.0,
                },
                "background": {"type": "color", "value": "#08343a"},
            }],
            "dimension": dim,
        }
        try:
            r = requests.post(
                f"{self.BASE_URL}/v2/video/generate",
                headers=self._h(),
                json=payload,
                timeout=30,
            )
            if r.status_code in (200, 201):
                data     = r.json()
                video_id = (
                    data.get("data", {}).get("video_id")
                    or data.get("video_id", "")
                )
                logger.info("HeyGen video submitted: %s", video_id)
                return video_id
            logger.error("HeyGen create_video %s: %s", r.status_code, r.text[:400])
        except Exception as e:
            logger.error("HeyGen create_video error: %s", e)
        return ""

    def wait_for_video(self, video_id: str, timeout: int = 600) -> str:
        """Poll until completed. Returns video_url or '' on failure/timeout."""
        import time
        deadline = time.time() + timeout
        logger.info("⏳ Waiting for HeyGen render (up to %ds)…", timeout)
        while time.time() < deadline:
            try:
                r = requests.get(
                    f"{self.BASE_URL}/v1/video_status.get",
                    headers=self._h(),
                    params={"video_id": video_id},
                    timeout=15,
                )
                if r.status_code == 200:
                    data   = r.json().get("data", {})
                    status = data.get("status", "")
                    if status == "completed":
                        url = data.get("video_url", "")
                        logger.info("✅ HeyGen render complete: %s", url)
                        return url
                    if status == "failed":
                        logger.error("HeyGen render failed: %s", data.get("error") or data)
                        return ""
                    logger.debug("HeyGen status: %s", status)
                elif r.status_code == 429:
                    logger.warning("HeyGen rate limit — sleeping 30s")
                    time.sleep(30)
                    continue
            except Exception as e:
                logger.warning("HeyGen poll error: %s", e)
            time.sleep(8)
        logger.error("HeyGen video %s timed out after %ds", video_id, timeout)
        return ""

    def download_video(self, url: str, path: str) -> bool:
        """Stream-download completed video to path."""
        try:
            r = requests.get(url, timeout=300, stream=True)
            r.raise_for_status()
            with open(path, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            logger.info("✅ HeyGen video saved: %s", path)
            return True
        except Exception as e:
            logger.error("HeyGen download error: %s", e)
            return False


class GitHubAPI:
    """GitHub API for website management"""
    
    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo
        self.base_url = f"https://api.github.com/repos/{repo}"
        
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
    
    def create_or_update_file(self, file_path: str, content: str, message: str) -> bool:
        """Create or update a file in GitHub repository"""
        
        try:
            # Encode content
            encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
            
            # Check if file exists to get SHA
            sha = self._get_file_sha(file_path)
            
            data = {
                "message": message,
                "content": encoded_content
            }
            
            if sha:
                data["sha"] = sha
            
            path_enc = quote(file_path, safe="")
            response = requests.put(
                f"{self.base_url}/contents/{path_enc}",
                headers=self.headers,
                json=data,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ GitHub file updated: {file_path}")
                return True
            else:
                logger.error(f"GitHub API Error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"GitHub API error: {e}")
            return False
    
    def _get_file_sha(self, file_path: str) -> str:
        """Get SHA of existing file"""
        try:
            path_enc = quote(file_path, safe="")
            response = requests.get(
                f"{self.base_url}/contents/{path_enc}",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json().get("sha")
        except:
            pass
        
        return None

class ContentGenerator:
    """Generate Microsoft Fabric article content"""
    
    def __init__(self):
        self.content_bank = self._load_content_bank()
        self.anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    def _load_content_bank(self) -> List[Dict]:
        """Load the 100-day content bank"""
        try:
            with open("enhanced_fabric_schedule.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "days" in data:
                return data["days"]
            if isinstance(data, list):
                return data
            logger.error("enhanced_fabric_schedule.json must be a JSON array or {{\"days\": [...]}}")
            return []
        except FileNotFoundError:
            logger.error("Content bank not found. Run enhanced_fabric_system.py first.")
            return []
    
    def generate_article_markdown(
        self,
        day: int,
        day_content: Dict,
        article_url: str,
        website_url: str,
        slug: str,
    ) -> str:
        """
        Article body for the website (markdown → HTML later).

        Priority:
        1. `article_markdown` or `body_markdown` in schedule (your full predefined copy).
        2. If `FABRIC_USE_AI` is true and `ANTHROPIC_API_KEY` is set — Claude generation.
        3. Otherwise — assemble from predefined `linkedin_content` + `article` metadata
           (no AI; matches your prewritten LinkedIn + meta).
        """
        explicit = (
            day_content.get("article_markdown") or day_content.get("body_markdown") or ""
        ).strip()
        if explicit:
            body = self._apply_article_placeholders(
                explicit, day, day_content, article_url, website_url, slug
            )
            logger.info("Day %s: using explicit article_markdown from schedule", day)
            return body

        use_ai = os.getenv("FABRIC_USE_AI", "").lower() in ("1", "true", "yes")
        if use_ai and (self.anthropic_api_key or self.gemini_api_key):
            provider = "Gemini" if self.gemini_api_key and not self.anthropic_api_key else "Anthropic"
            logger.info("Day %s: generating article via %s (FABRIC_USE_AI=1)", day, provider)
            return self._generate_with_api(
                day, day_content["title"], day_content["category"]
            )

        logger.info("Day %s: using predefined schedule fields (linkedin_content + meta)", day)
        return self._markdown_from_predefined_schedule(
            day_content, day, article_url, website_url
        )

    def _apply_article_placeholders(
        self,
        md: str,
        day: int,
        day_content: Dict,
        article_url: str,
        website_url: str,
        slug: str,
    ) -> str:
        base = website_url.rstrip("/")
        title = day_content.get("title", "")
        cat = day_content.get("category", "")
        out = md
        for key, val in (
            ("{day}", str(day)),
            ("{title}", title),
            ("{slug}", slug),
            ("{article_url}", article_url),
            ("{website_url}", base),
            ("{category}", cat),
        ):
            out = out.replace(key, val)
        return out

    def _linkedin_to_article_body(self, linkedin_content: str) -> str:
        """Strip LinkedIn CTA / hashtags; keep the main predefined copy."""
        if not linkedin_content:
            return ""
        main = linkedin_content.split("📖", 1)[0].strip()
        if "---" in main:
            main = main.split("---", 1)[0].strip()
        lines = []
        for line in main.splitlines():
            s = line.strip()
            if s.startswith("#") and (
                "MicrosoftFabric" in s
                or "DataEngineering" in s
                or "100Days" in s
                or "Analytics" in s
            ):
                continue
            if s.startswith("👉"):
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    def _markdown_from_predefined_schedule(
        self,
        day_content: Dict,
        day: int,
        article_url: str,
        website_url: str,
    ) -> str:
        """Build markdown from schedule only (linkedin_content + article.*), no AI."""
        title = day_content.get("title", f"Day {day}")
        art = day_content.get("article") or {}
        meta = art.get("meta_description", "")
        cat = str(day_content.get("category", "foundations")).replace("_", " ").title()
        body = self._linkedin_to_article_body(day_content.get("linkedin_content") or "")
        if not body:
            logger.warning(
                "Day %s: no linkedin_content in schedule — using short fallback", day
            )
            body = f"**{title}** — key concepts and practical notes for Microsoft Fabric."
        base = website_url.rstrip("/")
        parts = [
            f"# {title}",
            "",
            f"*Microsoft Fabric — 100 Days · Day {day} · {cat}*",
            "",
        ]
        if meta:
            parts.extend([f"> {meta}", ""])
        parts.extend(
            [
                "## What to know",
                "",
                body,
                "",
                "---",
                "",
                f"**Read online:** [{article_url}]({article_url})",
                "",
                f"- [← Series hub]({base}/articles/fabric-100-days/)",
                f"- [Portfolio]({base}/)",
            ]
        )
        return "\n".join(parts)
    
    def _build_article_prompt(self, day: int, title: str, category: str) -> str:
        """Build the detailed HTML-output prompt for AI article generation."""
        title_lower = title.lower()
        no_code_topics = (
            "what is", " vs ", "overview", "introduction", "pricing",
            "comparison", "architecture", "capacities", "licensing",
            "administration", "governance", "security", "roles",
        )
        include_code = not any(kw in title_lower for kw in no_code_topics)

        code_instruction = (
            "Include 1-2 focused, realistic code examples (Python/PySpark, T-SQL, or KQL as appropriate) "
            "that a practitioner can actually run. Each snippet must be wrapped in "
            "<pre><code class=\"language-python\"> (or sql/kql) ... </code></pre>."
        ) if include_code else (
            "This is a conceptual/overview topic — do NOT include any code blocks. "
            "Use tables, bullet lists, and diagrams-in-text instead."
        )

        return f"""You are a senior Microsoft Fabric architect writing Day {day} of a 100-day technical blog series.

Topic: {title}
Category: {category}
Target audience: Data engineers, analysts, and BI professionals learning Microsoft Fabric

Write a comprehensive, expert-level article as raw HTML — only the inner body content (no <html>, <head>, or <body> tags).
Use these HTML elements: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <table>, <thead>, <tbody>, <tr>, <th>, <td>, <strong>, <em>, <code>, <pre><code>, <blockquote>.

STRUCTURE (follow this exactly):
1. <h2> opening section — real-world business problem or "why this matters"
2. Core concepts with clear <h2>/<h3> sub-sections
3. Detailed technical explanation with specifics (real feature names, real limits, real URLs where relevant)
4. Comparison table or feature matrix where applicable (use <table> with <thead>/<tbody>)
5. Best practices section — formatted as a <table> with Do / Avoid columns OR as a <ul> of clear actionable points
6. Common pitfalls table: | Pitfall | Cause | Fix |
7. A concrete real-world scenario (step-by-step, numbered <ol>)
8. Key Takeaways as a <ul>
9. Closing <p> teasing tomorrow's topic

{code_instruction}

QUALITY RULES:
- Every table must have real, specific data — no "Example value" or "TBD" cells
- Every bullet point must be specific — no vague statements like "improves performance"
- Use actual Microsoft Fabric feature names, SKU names, and limits (e.g. F2, F64, OneLake, DirectLake, Delta Parquet)
- Write from expertise — include non-obvious insights a beginner wouldn't know
- Minimum 1500 words of substantive content
- Do NOT include a title <h1> — the page already has one
- Do NOT wrap in ```html or any markdown fences — return raw HTML only
- Start directly with the first <h2> tag"""

    def _generate_with_api(self, day: int, title: str, category: str) -> str:
        """Generate article using Gemini (free) or Anthropic API."""
        try:
            prompt = self._build_article_prompt(day, title, category)
            if self.gemini_api_key:
                return self._generate_with_gemini(day, title, category, prompt)
            return self._generate_with_anthropic(day, title, category, prompt)
        except Exception as e:
            logger.warning(f"AI generation error: {e}, using template for Day {day}")
            return self._generate_template_article(day, title, category)

    def _generate_with_gemini(self, day: int, title: str, category: str, prompt: str) -> str:
        """Generate article using Google Gemini (free tier)."""
        # Try gemini-2.5-pro first (best quality); fall back on quota errors
        for model in [os.getenv("GEMINI_MODEL", "gemini-2.5-pro"), "gemini-2.5-flash", "gemini-2.0-flash"]:
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent?key={self.gemini_api_key}"
            )
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": 32768,
                    "temperature": 0.4,
                    "topP": 0.95,
                },
            }
            try:
                r = requests.post(url, json=payload, timeout=120)
                if r.status_code == 200:
                    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                    # Strip any accidental markdown fences the model may add
                    text = re.sub(r'^```html?\s*', '', text.strip(), flags=re.IGNORECASE)
                    text = re.sub(r'\s*```$', '', text.strip())
                    logger.info("✅ Generated article via Gemini (%s) for Day %s", model, day)
                    return text
                if r.status_code == 429:
                    logger.warning("Gemini %s quota exceeded, trying next model…", model)
                    continue
                logger.warning("Gemini %s failed (%s): %s", model, r.status_code, r.text[:200])
                break
            except Exception as e:
                logger.warning("Gemini %s error: %s", model, e)
                break
        logger.warning("All Gemini models failed — using template for Day %s", day)
        return self._generate_template_article(day, title, category)

    def _generate_with_anthropic(self, day: int, title: str, category: str, prompt: str) -> str:
        """Generate article using Anthropic Claude."""
        model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.anthropic_api_key,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": model,
            "max_tokens": int(os.getenv("ANTHROPIC_MAX_TOKENS", "8192")),
            "temperature": 0.4,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=120)
            if r.status_code == 200:
                text = r.json()["content"][0]["text"]
                # Strip any accidental markdown fences
                text = re.sub(r'^```html?\s*', '', text.strip(), flags=re.IGNORECASE)
                text = re.sub(r'\s*```$', '', text.strip())
                logger.info("✅ Generated article via Anthropic for Day %s", day)
                return text
            logger.warning("Anthropic failed (%s): %s — using template", r.status_code, r.text[:200])
            return self._generate_template_article(day, title, category)
        except Exception as e:
            logger.warning("Anthropic error: %s — using template", e)
            return self._generate_template_article(day, title, category)
    
    def _generate_template_article(self, day: int, title: str, category: str) -> str:
        """Generate template-based article content without placeholder code blocks."""

        title_lower = title.lower()
        # Topics that are conceptual — no code blocks needed
        no_code_keywords = (
            "what is", "vs ", " vs", "overview", "introduction", "understanding",
            "pricing", "comparison", "architecture", "capacities", "workspaces",
            "administration", "governance", "security", "roles", "licensing",
        )
        include_code = not any(kw in title_lower for kw in no_code_keywords)

        code_section = ""
        if include_code:
            safe_name = title_lower.replace(' ', '_').replace('&', 'and').replace('-', '_')
            safe_name = ''.join(c for c in safe_name if c.isalnum() or c == '_')
            code_section = f"""
## Hands-On Example

The following snippet shows a minimal starting point for working with {title} in a Fabric notebook:

```python
# Fabric notebook — {title}
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# TODO: replace with your actual lakehouse path
df = spark.read.format("delta").load("abfss://your-workspace@onelake.dfs.fabric.microsoft.com/your-lakehouse.Lakehouse/Tables/your_table")

df.printSchema()
df.show(5)
```

Adjust the path to match your workspace and lakehouse name. Run this in a Fabric notebook after attaching it to the correct lakehouse.
"""

        return f"""# {title}

## Introduction

{title} is an important concept in Microsoft Fabric for data engineers and analysts building modern data platforms. On Day {day} of this 100-day series we break it down — what it is, why it matters, and how to use it effectively.

## What You Need to Know

Understanding {title.lower()} comes down to a few core ideas:

- **What it does**: The primary role {title.lower()} plays in the Fabric ecosystem
- **When to use it**: The scenarios and workloads it is best suited for
- **How it connects**: How it integrates with other Fabric components like OneLake, pipelines, and Power BI

## Key Concepts
{chr(10)}
**Unified platform context** — {title} operates within Microsoft Fabric's unified SaaS model, sharing a single OneLake storage layer across all workloads. This means no data duplication and consistent governance across your organisation.

**Capacity-based compute** — All Fabric workloads, including {title.lower()}, consume capacity units (CUs) from your assigned Fabric capacity (F2 through F2048). Sizing your capacity correctly is key to performance and cost control.

**Workspace isolation** — {title} items live inside Fabric workspaces. Workspaces provide the security boundary, collaboration unit, and deployment target for all Fabric content.
{code_section}
## Best Practices

### Do
- Start with a clear understanding of your data requirements before configuring {title.lower()}
- Use managed identities and role-based access control (RBAC) rather than shared keys
- Monitor CU consumption via the Fabric Capacity Metrics app regularly

### Avoid
- Over-provisioning capacity for development workloads — use F2 or F4 for dev/test
- Mixing production and development items in the same workspace
- Ignoring lineage and impact analysis when making schema changes

## Common Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Unexpected CU spikes | Unoptimised queries or large scans | Use query folding and partition pruning |
| Permission errors | Missing workspace or item-level roles | Assign correct Fabric roles (Viewer/Contributor/Admin) |
| Slow refresh | Too many concurrent operations | Schedule refreshes with staggered timing |

## Real-World Scenario

A retail company uses {title.lower()} as part of their daily sales analytics pipeline:

1. Raw sales data lands in OneLake via a Data Factory pipeline
2. {title} processes and prepares the data
3. A Power BI semantic model reads the clean data
4. Executives see live dashboards by 8 AM every day

The result: reporting time dropped from 4 hours (legacy SSRS) to under 30 minutes.

## Next Steps

- Read the [official Microsoft Fabric documentation](https://learn.microsoft.com/en-us/fabric/) for {title}
- Try the free [Microsoft Fabric trial](https://app.fabric.microsoft.com/) to follow along hands-on
- Complete the [DP-600 learning path](https://learn.microsoft.com/en-us/credentials/certifications/fabric-analytics-engineer-associate/) on Microsoft Learn

## Conclusion

{title} is a building block of the Microsoft Fabric platform. Mastering it — along with the rest of the Fabric stack — gives you the foundation to build fast, reliable, and cost-effective data solutions at enterprise scale.

*Part of the Microsoft Fabric 100 Days series — one focused topic per day, 100 days total.*
"""

class FullAutomationSystem:
    """Main automation orchestrator"""
    
    def __init__(self):
        self._local_only = os.getenv("FABRIC_LOCAL_ONLY", "").lower() in (
            "1",
            "true",
            "yes",
        )
        # Initialize APIs
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.linkedin_token = os.getenv("LINKEDIN_ACCESS_TOKEN")
        self.person_id = os.getenv("LINKEDIN_PERSON_ID")

        if not self._local_only:
            if not self.github_token:
                raise ValueError("GITHUB_TOKEN environment variable required")
            if not self.linkedin_token or not self.person_id:
                raise ValueError(
                    "LinkedIn credentials required: LINKEDIN_ACCESS_TOKEN, LINKEDIN_PERSON_ID"
                )
        else:
            self.github_token = self.github_token or "local"
            self.linkedin_token = self.linkedin_token or "local"
            self.person_id = self.person_id or "0"
            logger.info("FABRIC_LOCAL_ONLY — writing files under repo root; no GitHub/LinkedIn API calls")

        # Initialize components
        self._github_repo = _resolve_github_repo()
        self.github_api = GitHubAPI(self.github_token, self._github_repo)
        self.linkedin_api = LinkedInAPI(self.linkedin_token, self.person_id)
        self.youtube_api  = YouTubeAPI()
        self.avatar_gen   = AnimatedAvatarGenerator()
        self.heygen_api   = HeyGenAPI()
        self.did_api      = DIDApi()
        self.content_generator = ContentGenerator()

        # State management
        self.ist_timezone = pytz.timezone("Asia/Kolkata")
        self.published_articles = self._load_published_articles()
        self.website_url = _resolve_website_url(self._github_repo)

        logger.info("Full automation system initialized")

    def _put_file(self, file_path: str, content: str, message: str) -> bool:
        """GitHub API or local filesystem when FABRIC_LOCAL_ONLY."""
        if self._local_only:
            p = Path(file_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")
            logger.info("Local: wrote %s", file_path)
            return True
        return self.github_api.create_or_update_file(file_path, content, message)
    
    def _load_published_articles(self) -> List[Dict]:
        """Load published articles tracking"""
        try:
            with open('published_articles.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return []
    
    def _save_published_articles(self):
        """Save published articles tracking"""
        with open('published_articles.json', 'w', encoding='utf-8') as f:
            json.dump(self.published_articles, f, indent=2)
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            if not self._sync_published_articles_to_github():
                raise RuntimeError(
                    "Failed to sync published_articles.json to GitHub; aborting to avoid duplicate days on next run."
                )

    def _sync_published_articles_to_github(self) -> bool:
        """Persist published_articles.json via GitHub API (required for scheduled CI runs)."""
        content = json.dumps(self.published_articles, indent=2, ensure_ascii=False)
        return self._put_file(
            "published_articles.json",
            content,
            f"Track Fabric 100 Days progress ({len(self.published_articles)} articles)",
        )

    def test_apis(self) -> Dict[str, bool]:
        """Test all API connections"""
        if self._local_only:
            return {"github": True, "linkedin": True}
        logger.info("Testing API connections...")
        
        results = {
            'github': False,
            'linkedin': False
        }
        
        # Test GitHub
        try:
            response = requests.get(
                f"https://api.github.com/repos/{self._github_repo}",
                headers={"Authorization": f"token {self.github_token}"},
                timeout=10
            )
            results['github'] = response.status_code == 200
        except:
            pass
        
        # Test LinkedIn
        results['linkedin'] = self.linkedin_api.test_connection()
        
        logger.info(f"API test results: {results}")
        return results
    
    def create_article_html(
        self, day: int, title: str, content: str, category: str, slug: str,
        published_date: datetime.datetime = None
    ) -> str:
        """Generate complete HTML for article page."""

        # If AI already returned HTML, use it directly; otherwise convert markdown
        if content.strip().startswith('<'):
            html_content = content.strip()
        else:
            html_content = self._markdown_to_html(content)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Day {day}: {title} | Microsoft Fabric 100 Days</title>
    <meta name="description" content="Day {day} of Microsoft Fabric 100 Days series: {title}. Comprehensive guide with practical examples and implementation details.">
    <meta name="keywords" content="Microsoft Fabric, {title.lower()}, data engineering, analytics, tutorial, {category}">
    
    <!-- Open Graph -->
    <meta property="og:title" content="Day {day}: {title}">
    <meta property="og:description" content="Day {day} of Microsoft Fabric 100 Days series: {title}">
    <meta property="og:type" content="article">
    <meta property="og:url" content="{self.website_url}/articles/fabric-100-days/{slug}.html">
    <meta property="og:image" content="{self.website_url}/assets/fabric-series-og.png">
    
    <!-- Twitter Card -->
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:title" content="Day {day}: {title}">
    <meta name="twitter:description" content="Day {day} of Microsoft Fabric 100 Days series: {title}">
    
    <style>
        :root {{
            --bg: #0a0a0a;
            --surface: #1a1a1a;
            --surface2: #242424;
            --text: #e5e5e5;
            --text-muted: #a0a0a0;
            --accent: #007acc;
            --accent2: #38bdf8;
            --success: #10b981;
            --border: #333;
            --code-bg: #1e1e1e;
        }}
        
        * {{ 
            margin: 0; 
            padding: 0; 
            box-sizing: border-box; 
        }}
        
        html {{
            scroll-behavior: smooth;
        }}
        
        body {{
            font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.7;
            font-size: 16px;
        }}
        
        .container {{
            max-width: 800px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        /* Header */
        .article-header {{
            text-align: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 3rem;
            margin-bottom: 3rem;
        }}
        
        .series-badge {{
            background: linear-gradient(135deg, var(--accent), var(--accent2));
            color: white;
            padding: 0.6rem 1.2rem;
            border-radius: 25px;
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: inline-block;
        }}
        
        .article-title {{
            font-size: clamp(2rem, 5vw, 3rem);
            color: white;
            margin-bottom: 1rem;
            line-height: 1.2;
            font-weight: 700;
        }}
        
        .article-meta {{
            color: var(--text-muted);
            margin-top: 1.5rem;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 1.5rem;
            flex-wrap: wrap;
        }}
        
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        /* Content */
        .article-content {{
            font-size: 1.1rem;
            line-height: 1.8;
        }}
        
        .article-content h1 {{
            color: white;
            font-size: 2.2rem;
            margin: 3rem 0 1.5rem;
            font-weight: 700;
        }}
        
        .article-content h2 {{
            color: var(--accent2);
            font-size: 1.8rem;
            margin: 2.5rem 0 1rem;
            font-weight: 600;
        }}
        
        .article-content h3 {{
            color: var(--success);
            font-size: 1.4rem;
            margin: 2rem 0 0.8rem;
            font-weight: 600;
        }}
        
        .article-content p {{
            margin-bottom: 1.5rem;
            color: var(--text);
        }}
        
        .article-content ul, 
        .article-content ol {{
            margin: 1.5rem 0;
            padding-left: 2rem;
        }}
        
        .article-content li {{
            margin-bottom: 0.8rem;
            color: var(--text);
        }}
        
        .article-content blockquote {{
            border-left: 4px solid var(--accent);
            padding: 1.5rem 2rem;
            margin: 2rem 0;
            background: var(--surface);
            border-radius: 0 8px 8px 0;
            color: var(--text-muted);
            font-style: italic;
        }}
        
        /* Code styling */
        .article-content code {{
            background: var(--code-bg);
            color: #ffd700;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            font-size: 0.9em;
        }}
        
        .article-content pre {{
            background: var(--code-bg);
            padding: 1.5rem;
            border-radius: 8px;
            overflow-x: auto;
            border: 1px solid var(--border);
            margin: 2rem 0;
        }}
        
        .article-content pre code {{
            background: none;
            padding: 0;
            color: #e5e5e5;
        }}
        
        /* Tables */
        .article-content table {{
            width: 100%;
            border-collapse: collapse;
            margin: 2rem 0;
            background: var(--surface);
            border-radius: 8px;
            overflow: hidden;
        }}
        
        .article-content th,
        .article-content td {{
            padding: 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        .article-content th {{
            background: var(--surface2);
            color: white;
            font-weight: 600;
        }}
        
        /* Links */
        .article-content a {{
            color: var(--accent);
            text-decoration: none;
            border-bottom: 1px solid transparent;
            transition: all 0.3s ease;
        }}
        
        .article-content a:hover {{
            border-bottom-color: var(--accent);
        }}
        
        /* Navigation */
        .article-navigation {{
            margin-top: 4rem;
            padding-top: 3rem;
            border-top: 1px solid var(--border);
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
        }}
        
        .nav-link {{
            background: var(--surface);
            color: var(--text);
            padding: 1rem 1.5rem;
            text-decoration: none;
            border-radius: 8px;
            border: 1px solid var(--border);
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            text-align: center;
            font-weight: 500;
        }}
        
        .nav-link:hover {{
            background: var(--accent);
            border-color: var(--accent);
            color: white;
            transform: translateY(-2px);
        }}
        
        /* Share section */
        .share-section {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 2rem;
            margin: 3rem 0;
            text-align: center;
        }}
        
        .share-section h3 {{
            margin-bottom: 1rem;
            color: white;
        }}
        
        .share-section p {{
            color: var(--text-muted);
            margin-bottom: 1.5rem;
        }}
        
        .share-buttons {{
            display: flex;
            gap: 1rem;
            justify-content: center;
            flex-wrap: wrap;
        }}
        
        .share-btn {{
            background: #0077b5;
            color: white;
            padding: 0.8rem 1.5rem;
            text-decoration: none;
            border-radius: 6px;
            font-weight: 500;
            transition: all 0.3s ease;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .share-btn:hover {{
            background: #005885;
            transform: translateY(-2px);
        }}
        
        /* Progress indicator */
        .progress-section {{
            background: var(--surface2);
            padding: 1.5rem;
            border-radius: 8px;
            margin: 2rem 0;
            text-align: center;
        }}
        
        .progress-bar {{
            background: var(--border);
            height: 8px;
            border-radius: 4px;
            overflow: hidden;
            margin: 1rem 0;
        }}
        
        .progress-fill {{
            background: linear-gradient(90deg, var(--accent), var(--success));
            height: 100%;
            width: {(day/100)*100}%;
            transition: width 0.3s ease;
        }}
        
        .progress-text {{
            color: var(--text-muted);
            font-size: 0.9rem;
        }}
        
        /* Responsive */
        @media (max-width: 768px) {{
            .container {{
                padding: 1rem;
            }}
            
            .article-header {{
                padding-bottom: 2rem;
                margin-bottom: 2rem;
            }}
            
            .article-meta {{
                flex-direction: column;
                gap: 0.5rem;
            }}
            
            .article-navigation {{
                grid-template-columns: 1fr;
            }}
            
            .share-buttons {{
                flex-direction: column;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header class="article-header">
            <div class="series-badge">Microsoft Fabric - Day {day} of 100</div>
            <h1 class="article-title">{title}</h1>
            
            <div class="article-meta">
                <div class="meta-item">
                    <span>📅</span>
                    <span>{(published_date or datetime.datetime.now(self.ist_timezone)).strftime('%B %d, %Y')}</span>
                </div>
                <div class="meta-item">
                    <span>🏷️</span>
                    <span>{category.replace('_', ' ').title()}</span>
                </div>
                <div class="meta-item">
                    <span>👤</span>
                    <span>Mani Swaroop</span>
                </div>
                <div class="meta-item">
                    <span>⏱️</span>
                    <span>10 min read</span>
                </div>
            </div>
        </header>
        
        <div class="progress-section">
            <div class="progress-text">Series Progress: {day}/100 Articles</div>
            <div class="progress-bar">
                <div class="progress-fill"></div>
            </div>
        </div>
        
        <main class="article-content">
            {html_content}
        </main>
        
        <div class="share-section">
            <h3>Found this helpful?</h3>
            <p>Share it with your network and follow the series for daily Microsoft Fabric insights!</p>
            <div class="share-buttons">
                <a href="https://linkedin.com/in/mani-swaroop-sodadasi-1a165820a" class="share-btn">
                    Follow on LinkedIn
                </a>
                <a href="https://linkedin.com/sharing/share-offsite/?url={self.website_url}/articles/fabric-100-days/{slug}.html" class="share-btn">
                    Share Article
                </a>
            </div>
        </div>
        
        <nav class="article-navigation">
            <a href="../" class="nav-link">
                ← Back to Series
            </a>
            <a href="{self.website_url}" class="nav-link">
                Portfolio Home
            </a>
            {f'<a href="day-{day+1}.html" class="nav-link">Next: Day {day+1} →</a>' if day < 100 else ''}
        </nav>
    </div>
</body>
</html>"""
    
    def _markdown_to_html(self, content: str) -> str:
        """Convert markdown content to HTML (fallback when AI doesn't return HTML)."""

        # Code blocks — must be handled before inline backticks
        def replace_code_block(match):
            lang = match.group(1) or ''
            code = match.group(2).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            return f'<pre><code class="language-{lang}">{code}</code></pre>'
        content = re.sub(r'```(\w+)?\n(.*?)```', replace_code_block, content, flags=re.DOTALL)

        # Markdown tables → HTML tables
        def replace_table(match):
            rows = [r.strip() for r in match.group(0).strip().splitlines() if r.strip()]
            html = ['<table>']
            for i, row in enumerate(rows):
                if re.match(r'^\|[-:| ]+\|$', row):
                    continue  # separator row
                cells = [c.strip() for c in row.strip('|').split('|')]
                tag = 'th' if i == 0 else 'td'
                section = '<thead>' if i == 0 else ('<tbody>' if i == 2 else '')
                close  = '</thead>' if i == 0 else ''
                if i == 0:
                    html.append('<thead><tr>' + ''.join(f'<{tag}>{c}</{tag}>' for c in cells) + '</tr></thead><tbody>')
                else:
                    html.append('<tr>' + ''.join(f'<td>{c}</td>' for c in cells) + '</tr>')
            html.append('</tbody></table>')
            return '\n'.join(html)
        content = re.sub(r'(\|.+\|\n)+', replace_table, content)

        # Headers
        content = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', content, flags=re.MULTILINE)
        content = re.sub(r'^### (.+)$', r'<h3>\1</h3>', content, flags=re.MULTILINE)
        content = re.sub(r'^## (.+)$', r'<h2>\1</h2>', content, flags=re.MULTILINE)
        content = re.sub(r'^# (.+)$', r'<h1>\1</h1>', content, flags=re.MULTILINE)

        # Inline code
        content = re.sub(r'`([^`]+)`', r'<code>\1</code>', content)

        # Bold / italic
        content = re.sub(r'\*\*\*(.+?)\*\*\*', r'<strong><em>\1</em></strong>', content)
        content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content)
        content = re.sub(r'\*(.+?)\*', r'<em>\1</em>', content)

        # Links
        content = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2" target="_blank">\1</a>', content)

        # Blockquotes
        content = re.sub(r'^> (.+)$', r'<blockquote>\1</blockquote>', content, flags=re.MULTILINE)

        # Ordered lists
        def replace_ol(m):
            items = re.findall(r'^\d+\. (.+)$', m.group(0), re.MULTILINE)
            return '<ol>\n' + '\n'.join(f'<li>{i}</li>' for i in items) + '\n</ol>'
        content = re.sub(r'((?:^\d+\. .+\n?)+)', replace_ol, content, flags=re.MULTILINE)

        # Unordered lists (-, *, •)
        def replace_ul(m):
            items = re.findall(r'^[-*•] (.+)$', m.group(0), re.MULTILINE)
            return '<ul>\n' + '\n'.join(f'<li>{i}</li>' for i in items) + '\n</ul>'
        content = re.sub(r'((?:^[-*•] .+\n?)+)', replace_ul, content, flags=re.MULTILINE)

        # Horizontal rules
        content = re.sub(r'^---+$', '<hr>', content, flags=re.MULTILINE)

        # Paragraphs — wrap any non-tag block in <p>
        paragraphs = content.split('\n\n')
        result = []
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            if para.startswith('<'):
                result.append(para)
            else:
                result.append(f'<p>{para.replace(chr(10), " ")}</p>')
        return '\n\n'.join(result)

    def _hub_excerpt_for_day(self, day: int) -> str:
        """Short blurb for articles hub cards."""
        try:
            row = self.content_generator.content_bank[day - 1]
            meta = (row.get("article") or {}).get("meta_description") or ""
            if meta and len(meta) > 40:
                return html.escape(meta[:280] + ("…" if len(meta) > 280 else ""))
        except (IndexError, TypeError, KeyError):
            pass
        return "Hands-on Microsoft Fabric guide with examples and best practices."

    def _build_articles_hub_main_html(self) -> str:
        """HTML between FABRIC_HUB markers: featured series + latest article cards."""
        pub = self.published_articles
        n = len(pub)
        pct = min(100, max(1, int(round(100 * n / 100)))) if n else 0
        if n == 0:
            pct = 0

        latest = pub[-1] if pub else None

        progress_line = "No articles yet — first publish coming soon."
        if latest:
            progress_line = f"Latest: Day {latest['day']} — {html.escape(latest['title'])}"

        latest_href = "/articles/fabric-100-days/"
        latest_btn = "View Complete Series"
        if latest:
            latest_href = f"/articles/fabric-100-days/{latest['slug']}.html"
            latest_btn = "Read Latest Article"

        cards_html = ""
        # Newest first (Day 2, then Day 1)
        for art in reversed(pub[-6:]):
            d = art["day"]
            cat = str(art.get("category", "foundations")).replace("_", " ").title()
            pdate = ""
            try:
                pdate = datetime.datetime.fromisoformat(
                    art["published_date"].replace("Z", "+00:00")
                ).strftime("%b %d, %Y")
            except (ValueError, TypeError, KeyError):
                pdate = datetime.datetime.now().strftime("%b %d, %Y")
            excerpt = self._hub_excerpt_for_day(d)
            href = f"/articles/fabric-100-days/{art['slug']}.html"
            cards_html += f"""
                                                <article class="article-card">
                                                                      <div class="article-header">
                                                                                                <div class="day-badge">Day {d}</div>
                                                                                                <div class="category-tag">{html.escape(cat)}</div>
                                                                      </div>
                                                                      <div class="article-content">
                                                                                                <h3 class="article-title">{html.escape(art['title'])}</h3>
                                                                                                <p class="article-excerpt">
                                                                                                                              {excerpt}
                                                                                                  </p>
                                                                                                <div class="article-meta">
                                                                                                                              <span>📅 {pdate}</span>
                                                                                                                              <span>⏱️ ~12–18 min read</span>
                                                                                                  </div>
                                                                      </div>
                                                                      <div class="article-footer">
                                                                                                <a href="{href}" class="article-link">
                                                                                                                              Read Complete Guide →
                                                                                                  </a>
                                                                      </div>
                                                </article>
            """

        if not cards_html.strip():
            cards_html = """
                                                <article class="article-card">
                                                                      <div class="article-content">
                                                                                                <p class="article-excerpt">Articles will appear here as the series publishes.</p>
                                                                      </div>
                                                </article>
            """

        return f"""
                            <div class="series-card">
                                              <div class="series-header">
                                                                    <div class="series-icon">📚</div>
                                                                    <div class="series-info">
                                                                                              <h3>Microsoft Fabric - 100 Days</h3>
                                                                                              <div class="series-status">Active Series</div>
                                                                    </div>
                                              </div>

                                              <p class="series-description">
                                                                    Master Microsoft Fabric with our comprehensive 100-day learning journey. From foundational concepts to advanced enterprise implementations, each day delivers practical insights, real-world examples, and hands-on tutorials.
                                              </p>

                                              <div class="progress-section">
                                                                    <div class="progress-header">
                                                                                              <span>Learning Progress</span>
                                                                                            <span class="progress-value">{n} of 100 articles</span>
                                                                    </div>
                                                                  <div class="progress-bar">
                                                                                          <div class="progress-fill" style="width:{pct}%"></div>
                                                                  </div>
                                                                  <div class="progress-latest">
                                                                                          {progress_line}
                                                                  </div>
                                              </div>

                                            <div>
                                                                  <a href="/articles/fabric-100-days/" class="btn btn-primary">View Complete Series</a>
                                                                  <a href="{latest_href}" class="btn btn-secondary">{html.escape(latest_btn)}</a>
                                            </div>
                            </div>
              </section>

                <section>
                              <h2 class="section-title">Latest Articles</h2>

                              <div class="articles-grid">
{cards_html}
                              </div>
                </section>
"""

    def update_articles_hub_page(self) -> bool:
        """Keep articles/index.html in sync (series progress + latest cards)."""
        hub_path = Path("articles/index.html")
        if not hub_path.is_file():
            logger.warning("articles/index.html not found — skipping hub update")
            return True
        try:
            text = hub_path.read_text(encoding="utf-8")
        except OSError as e:
            logger.error("Could not read articles/index.html: %s", e)
            return False

        if FABRIC_HUB_BEGIN not in text or FABRIC_HUB_END not in text:
            logger.warning(
                "FABRIC_HUB markers missing in articles/index.html — "
                "add %s ... %s for auto-updates",
                FABRIC_HUB_BEGIN,
                FABRIC_HUB_END,
            )
            return True

        inner = self._build_articles_hub_main_html()
        pattern = re.compile(
            re.escape(FABRIC_HUB_BEGIN)
            + r".*?"
            + re.escape(FABRIC_HUB_END),
            re.DOTALL,
        )
        new_text, nsub = pattern.subn(
            FABRIC_HUB_BEGIN + "\n" + inner + "\n" + FABRIC_HUB_END,
            text,
            count=1,
        )
        if nsub != 1:
            logger.error("Failed to replace FABRIC_HUB block in articles/index.html")
            return False

        ok = self._put_file(
            "articles/index.html",
            new_text,
            f"Auto-update articles hub — {len(self.published_articles)}/100 days",
        )
        if ok:
            logger.info("✅ Main articles hub (articles/index.html) updated")
        return ok

    def update_portfolio_page(self) -> bool:
        """Keep root index.html in sync with Fabric 100 Days progress."""
        portfolio_path = Path("index.html")
        if not portfolio_path.is_file():
            logger.warning("index.html not found — skipping portfolio update")
            return True
        try:
            text = portfolio_path.read_text(encoding="utf-8")
        except OSError as e:
            logger.error("Could not read index.html: %s", e)
            return False

        if FABRIC_PORTFOLIO_BEGIN not in text or FABRIC_PORTFOLIO_END not in text:
            logger.warning(
                "FABRIC_PORTFOLIO markers missing in index.html — "
                "add %s ... %s for auto-updates",
                FABRIC_PORTFOLIO_BEGIN,
                FABRIC_PORTFOLIO_END,
            )
            return True

        count = len(self.published_articles)
        latest = max(self.published_articles, key=lambda a: a["day"])
        pct = round(count / 100 * 100)
        latest_slug = latest["slug"]
        latest_title = latest["title"]
        latest_day = latest["day"]
        latest_url = f"/articles/fabric-100-days/{latest_slug}.html"
        published_date = latest.get("published_date", "")
        try:
            pdate = datetime.datetime.fromisoformat(
                published_date.replace("Z", "+00:00")
            ).strftime("%b %d, %Y")
        except Exception:
            pdate = datetime.datetime.now().strftime("%b %d, %Y")

        inner = f"""\
                    <div class="progress-section">
                        <div class="progress-header">
                            <span>Progress</span>
                            <span class="progress-value">{count} of 100</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {pct}%;"></div>
                        </div>
                        <div class="progress-latest">Latest: Day {latest_day} - {latest_title}</div>
                    </div>

                    <div class="article-actions">
                        <a href="/articles/fabric-100-days/" class="btn btn-primary">View Series</a>
                        <a href="{latest_url}" class="btn btn-secondary">Latest Article</a>
                    </div>"""

        stats_inner = f"""\
            <div class="articles-stats">
                <div class="stat-item">
                    <div class="stat-number">{count}</div>
                    <div class="stat-label">Articles Published</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{count * 5}K+</div>
                    <div class="stat-label">Words Written</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{100 - count}</div>
                    <div class="stat-label">More Coming</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">Daily</div>
                    <div class="stat-label">Publishing</div>
                </div>
            </div>"""

        # Replace progress block
        pattern = re.compile(
            re.escape(FABRIC_PORTFOLIO_BEGIN)
            + r".*?"
            + re.escape(FABRIC_PORTFOLIO_END),
            re.DOTALL,
        )
        new_text, nsub = pattern.subn(
            FABRIC_PORTFOLIO_BEGIN + "\n" + inner + "\n" + FABRIC_PORTFOLIO_END,
            text,
            count=1,
        )
        if nsub != 1:
            logger.error("Failed to replace FABRIC_PORTFOLIO block in index.html")
            return False

        # Also update the topic-status badge and stat-number for articles count
        new_text = re.sub(
            r'(<span class="topic-status">)Day \d+ Published(</span>)',
            rf'\1Day {latest_day} Published\2',
            new_text,
            count=1,
        )
        # Update articles-stats block
        stats_pattern = re.compile(
            r'<div class="articles-stats">.*?</div>\s*</div>',
            re.DOTALL,
        )
        new_text = stats_pattern.sub(stats_inner + "\n        </div>", new_text, count=1)

        ok = self._put_file(
            "index.html",
            new_text,
            f"Auto-update portfolio — {count}/100 days",
        )
        if ok:
            logger.info("✅ Portfolio page (index.html) updated")
        return ok

    def update_series_index(self):
        """Update the series index page with latest articles"""
        
        # Generate article cards
        article_cards = ""
        for article in self.published_articles[-12:]:  # Show latest 12
            published_date = datetime.datetime.fromisoformat(article['published_date'])
            
            article_cards += f"""
            <div class="article-card">
                <div class="day-badge">Day {article['day']}</div>
                <h3 class="article-title">{html.escape(article['title'])}</h3>
                <div class="article-meta">
                    <span class="meta-date">📅 {published_date.strftime('%b %d, %Y')}</span>
                    <span class="meta-category">🏷️ {article['category'].replace('_', ' ').title()}</span>
                </div>
                <p class="article-excerpt">
                    Comprehensive guide covering {article['title'].lower()} with practical examples, implementation details, and best practices for Microsoft Fabric.
                </p>
                <a href="{article['slug']}.html" class="article-link">
                    Read Full Article →
                </a>
            </div>
            """
        
        index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Microsoft Fabric - 100 Days Series | Mani Swaroop</title>
    <meta name="description" content="Complete 100-day learning series on Microsoft Fabric. Daily comprehensive articles covering everything from basics to advanced implementations.">
    <meta name="keywords" content="Microsoft Fabric, 100 days, learning series, data engineering, analytics, tutorials">
    
    <!-- Open Graph -->
    <meta property="og:title" content="Microsoft Fabric - 100 Days Series">
    <meta property="og:description" content="Master Microsoft Fabric with our comprehensive 100-day learning series">
    <meta property="og:type" content="website">
    <meta property="og:url" content="{self.website_url}/articles/fabric-100-days/">
    
    <style>
        :root {{
            --bg: #0a0a0a;
            --surface: #1a1a1a;
            --surface2: #242424;
            --text: #e5e5e5;
            --text-muted: #a0a0a0;
            --accent: #007acc;
            --accent2: #38bdf8;
            --success: #10b981;
            --border: #333;
        }}
        
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: system-ui, -apple-system, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, var(--accent), #005999);
            text-align: center;
            padding: 4rem 2rem;
            color: white;
        }}
        
        .header h1 {{
            font-size: clamp(2.5rem, 6vw, 4rem);
            margin-bottom: 1rem;
            font-weight: 700;
        }}
        
        .header p {{
            font-size: 1.2rem;
            max-width: 600px;
            margin: 0 auto;
            opacity: 0.9;
        }}
        
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 2rem;
            max-width: 1000px;
            margin: 3rem auto;
            padding: 0 2rem;
        }}
        
        .stat-card {{
            background: var(--surface);
            padding: 2rem;
            border-radius: 12px;
            text-align: center;
            border: 1px solid var(--border);
            transition: transform 0.3s ease;
        }}
        
        .stat-card:hover {{
            transform: translateY(-4px);
        }}
        
        .stat-number {{
            font-size: 2.5rem;
            font-weight: bold;
            color: var(--accent);
            margin-bottom: 0.5rem;
        }}
        
        .stat-label {{
            color: var(--text-muted);
            font-size: 0.9rem;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 2rem;
        }}
        
        .section-title {{
            font-size: 2.5rem;
            text-align: center;
            margin: 4rem 0 3rem;
            color: white;
            font-weight: 700;
        }}
        
        .article-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 2rem;
            margin-bottom: 4rem;
        }}
        
        .article-card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 2rem;
            transition: all 0.3s ease;
            position: relative;
        }}
        
        .article-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent), var(--success));
            border-radius: 12px 12px 0 0;
        }}
        
        .article-card:hover {{
            transform: translateY(-6px);
            border-color: var(--accent);
        }}
        
        .day-badge {{
            background: linear-gradient(135deg, var(--accent), var(--accent2));
            color: white;
            padding: 0.4rem 1rem;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            display: inline-block;
            margin-bottom: 1rem;
        }}
        
        .article-title {{
            color: white;
            margin-bottom: 1rem;
            font-size: 1.3rem;
            font-weight: 600;
            line-height: 1.3;
        }}
        
        .article-meta {{
            display: flex;
            gap: 1rem;
            margin-bottom: 1rem;
            color: var(--text-muted);
            font-size: 0.85rem;
            flex-wrap: wrap;
        }}
        
        .article-excerpt {{
            color: var(--text-muted);
            margin-bottom: 1.5rem;
            line-height: 1.6;
        }}
        
        .article-link {{
            color: var(--accent);
            text-decoration: none;
            font-weight: 500;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            transition: all 0.3s ease;
        }}
        
        .article-link:hover {{
            gap: 0.8rem;
            color: var(--accent2);
        }}
        
        .cta-section {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 3rem;
            text-align: center;
            margin: 4rem auto;
            max-width: 800px;
        }}
        
        .cta-section h3 {{
            color: white;
            font-size: 2rem;
            margin-bottom: 1rem;
            font-weight: 600;
        }}
        
        .cta-section p {{
            color: var(--text-muted);
            margin-bottom: 2rem;
            font-size: 1.1rem;
            line-height: 1.6;
        }}
        
        .cta-buttons {{
            display: flex;
            gap: 1rem;
            justify-content: center;
            flex-wrap: wrap;
        }}
        
        .linkedin-btn {{
            background: #0077b5;
            color: white;
            padding: 1rem 2rem;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 500;
            transition: all 0.3s ease;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .linkedin-btn:hover {{
            background: #005885;
            transform: translateY(-2px);
        }}
        
        .newsletter-btn {{
            background: var(--accent);
            color: white;
            padding: 1rem 2rem;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 500;
            transition: all 0.3s ease;
        }}
        
        .newsletter-btn:hover {{
            background: var(--accent2);
            transform: translateY(-2px);
        }}
        
        .footer {{
            text-align: center;
            padding: 3rem;
            border-top: 1px solid var(--border);
            color: var(--text-muted);
            margin-top: 4rem;
        }}
        
        .footer-links {{
            margin-bottom: 1rem;
        }}
        
        .footer-links a {{
            color: var(--accent);
            text-decoration: none;
            margin: 0 1rem;
        }}
        
        .footer-links a:hover {{
            color: var(--accent2);
        }}
        
        @media (max-width: 768px) {{
            .header {{
                padding: 3rem 1rem;
            }}
            
            .stats {{
                grid-template-columns: repeat(2, 1fr);
                gap: 1rem;
                padding: 0 1rem;
            }}
            
            .container {{
                padding: 0 1rem;
            }}
            
            .article-grid {{
                grid-template-columns: 1fr;
                gap: 1.5rem;
            }}
            
            .cta-buttons {{
                flex-direction: column;
                align-items: center;
            }}
        }}
    </style>
</head>
<body>
    <header class="header">
        <h1>Microsoft Fabric - 100 Days</h1>
        <p>Master Microsoft's unified analytics platform with daily comprehensive guides and practical tutorials</p>
    </header>
    
    <div class="stats">
        <div class="stat-card">
            <div class="stat-number">{len(self.published_articles)}</div>
            <div class="stat-label">Articles Published</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">100</div>
            <div class="stat-label">Total Days</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">2500+</div>
            <div class="stat-label">Words per Article</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">Daily</div>
            <div class="stat-label">New Content</div>
        </div>
    </div>
    
    <div class="container">
        <h2 class="section-title">Latest Articles</h2>
        <div class="article-grid">
            {article_cards}
        </div>
        
        <div class="cta-section">
            <h3>Join the Microsoft Fabric Journey</h3>
            <p>Follow along as we explore every aspect of Microsoft Fabric with daily in-depth articles, practical examples, and real-world implementations.</p>
            <div class="cta-buttons">
                <a href="https://linkedin.com/in/mani-swaroop-sodadasi-1a165820a" class="linkedin-btn">
                    📱 Follow on LinkedIn
                </a>
                <a href="{self.website_url}" class="newsletter-btn">
                    🏠 Visit Portfolio
                </a>
            </div>
        </div>
    </div>
    
    <footer class="footer">
        <div class="footer-links">
            <a href="{self.website_url}">Portfolio</a>
            <a href="https://linkedin.com/in/mani-swaroop-sodadasi-1a165820a">LinkedIn</a>
            <a href="mailto:sodadasiswaroop@gmail.com">Contact</a>
        </div>
        <p>&copy; 2026 Mani Swaroop - Senior Data & AI Engineer<br>
           Microsoft Fabric 100 Days Learning Series</p>
    </footer>
</body>
</html>"""
        
        # Update series index
        success = self._put_file(
            "articles/fabric-100-days/index.html",
            index_html,
            f"Update series index - {len(self.published_articles)} articles published",
        )
        
        if success:
            logger.info(f"✅ Series index updated with {len(self.published_articles)} articles")
        else:
            logger.error("❌ Failed to update series index")
        
        return success
    
    def create_linkedin_post(self, day: int, day_content: Dict, article_url: str) -> str:
        """Create LinkedIn post content"""

        title = day_content.get("title", "Microsoft Fabric")
        # Get pro tip based on the title
        pro_tips = {
            "What is Microsoft Fabric": "Think of Fabric as 'Azure Synapse + Power BI Premium + Data Factory' - but designed from the ground up for unified analytics.",
            "Fabric vs Synapse": "Fabric isn't replacing Synapse/Power BI - it's the next evolution with unified OneLake storage.",
            "Understanding Fabric Capacities": "Start with F64 (64 CU) for development. F2-F32 have limitations on Spark and advanced features.",
            "Fabric Workspaces": "Use security groups, not individual users for permissions. Create DEV/TEST/PROD workspaces for proper lifecycle management.",
            "OneLake": "OneLake shortcuts let you reference external data without copying - works with ADLS Gen2 accounts too!",
        }

        pro_tip = pro_tips.get(
            title,
            f"Master {title.lower()} to build more efficient and scalable data solutions in Microsoft Fabric.",
        )

        raw_li = day_content.get("linkedin_content") or ""
        # Only use predefined linkedin_content if it's genuinely hand-crafted (not boilerplate)
        raw_body = raw_li.split("📖")[0].strip() if raw_li else ""
        if raw_body and not self._is_boilerplate_linkedin(raw_body):
            main_content = raw_body
        else:
            # Build a clean body from schedule hashtags as key topics
            hashtags = [h.replace("_", " ") for h in (day_content.get("hashtags") or [])
                        if h not in ("MicrosoftFabric", "100DaysChallenge", "Azure", "Analytics",
                                     "Foundations", "GettingStarted", "DataPlatform")]
            if hashtags:
                bullets = "\n".join(f"• {h}" for h in hashtags[:4])
                main_content = f"Today's deep-dive covers everything you need to know about {title}.\n\n{bullets}"
            else:
                main_content = f"Today's deep-dive: {title} — key concepts, best practices, and common pitfalls."
        
        cat = str(day_content.get("category", "foundations")).replace("_", "").title()
        linkedin_post = f"""🧵 Microsoft Fabric — Day {day}/100: {title}

{main_content}

💡 Pro Tip: {pro_tip}

📖 Full article with code examples and best practices:
{article_url}

---
#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge #{cat}

👉 What's your experience with {title.lower()}? Share your thoughts below!"""

        return linkedin_post

    # Phrases that identify boilerplate auto-generated schedule content (not worth using)
    _LINKEDIN_BOILERPLATE_MARKERS = (
        "is a fundamental concept in Microsoft Fabric that enables:",
        "Understanding " and "comes down to a few core ideas",
        "What if you could unify data engineering, analytics, and AI in ONE platform?",
    )

    def _is_boilerplate_linkedin(self, text: str) -> bool:
        """Return True if the linkedin_content looks like auto-generated boilerplate."""
        return any(marker in text for marker in self._LINKEDIN_BOILERPLATE_MARKERS)

    def _generate_linkedin_with_ai(self, day: int, day_content: Dict, article_url: str) -> str:
        """Use Gemini (or Anthropic) to write a compelling LinkedIn post."""
        title = day_content.get("title", "")
        category = day_content.get("category", "foundations")
        gemini_key = getattr(self.content_generator, "gemini_api_key", None)
        anthropic_key = getattr(self.content_generator, "anthropic_api_key", None)

        prompt = f"""Write a LinkedIn post for Day {day} of a "Microsoft Fabric 100 Days" series.

Topic: {title}
URL: {article_url}

Write exactly this structure (replace the [bracket] parts with real content):

🧵 Microsoft Fabric - Day {day}/100

[One hook sentence — surprising fact or common mistake about {title}]

• [Real specific fact about {title} — use actual feature names, numbers, or comparisons]
• [Real specific fact — different angle]
• [Real specific fact — practical implication]
• [Real specific fact — non-obvious insight]

💡 Pro Tip: [Expert insight most practitioners miss about {title}]

📖 Full guide: {article_url}

---
#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge [1-2 topic-specific tags]

👉 [Specific question to engage readers about {title}]

RULES: 150-220 words total. Every bullet must state a real, specific Microsoft Fabric fact. No generic phrases like "game-changer" or "revolutionize". Plain text only."""

        for model in ["gemini-2.0-flash", "gemini-2.5-flash"]:
            if not gemini_key:
                break
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent?key={gemini_key}"
            )
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 800, "temperature": 0.5},
            }
            try:
                r = requests.post(url, json=payload, timeout=60)
                if r.status_code == 200:
                    resp = r.json()
                    finish = resp.get("candidates", [{}])[0].get("finishReason", "")
                    if finish in ("STOP", "MAX_TOKENS"):
                        text = resp["candidates"][0]["content"]["parts"][0]["text"].strip()
                        logger.info("✅ LinkedIn post generated via Gemini (%s) for Day %s (finish=%s)", model, day, finish)
                        return self._ensure_linkedin_url_hashtags(text, article_url, day_content)
                if r.status_code == 429:
                    logger.warning("Gemini %s quota exceeded for LinkedIn, trying next…", model)
                    continue
            except Exception as e:
                logger.warning("Gemini LinkedIn error (%s): %s", model, e)
                break

        if anthropic_key:
            headers = {
                "Content-Type": "application/json",
                "x-api-key": anthropic_key,
                "anthropic-version": "2023-06-01",
            }
            payload = {
                "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            }
            try:
                r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=60)
                if r.status_code == 200:
                    text = r.json()["content"][0]["text"].strip()
                    logger.info("✅ LinkedIn post generated via Anthropic for Day %s", day)
                    return self._ensure_linkedin_url_hashtags(text, article_url, day_content)
            except Exception as e:
                logger.warning("Anthropic LinkedIn error: %s", e)

        # Final fallback
        return self.create_linkedin_post(day, day_content, article_url)

    def _ensure_linkedin_url_hashtags(self, post_text: str, article_url: str, day_content: Dict) -> str:
        """
        Guarantee the article URL and core hashtags are always present in the post.
        Appends them if the AI forgot to include them.
        Also trims the body if the total post exceeds LinkedIn's 3000-char limit so
        the URL and hashtags are never silently truncated.
        """
        text = post_text

        # Ensure article URL is present
        if article_url and article_url not in text:
            text = text.rstrip() + f"\n\n📖 Full article: {article_url}"
            logger.warning("AI LinkedIn post missing URL — appended: %s", article_url)

        # Ensure core hashtags are present
        core_tags = "#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge"
        if "#MicrosoftFabric" not in text:
            text = text.rstrip() + f"\n\n---\n{core_tags}"
            logger.warning("AI LinkedIn post missing hashtags — appended")

        # Enforce LinkedIn's ~3000-char limit: if over, trim the body while
        # keeping the footer (URL + hashtags) intact at the end.
        LINKEDIN_LIMIT = 2900
        if len(text) > LINKEDIN_LIMIT:
            # Locate the start of the footer block (whichever comes first)
            footer_pos = len(text)
            for marker in ["\n\n📖", "\n📖", "\n\n---\n#", "\n---\n#"]:
                idx = text.find(marker)
                if 0 < idx < footer_pos:
                    footer_pos = idx
            footer = text[footer_pos:]
            body = text[:footer_pos].rstrip()
            budget = LINKEDIN_LIMIT - len(footer)
            if budget > 50:
                body = body[:budget].rsplit("\n", 1)[0].rstrip()
            else:
                body = body[:50]
            text = body + footer
            logger.warning(
                "LinkedIn post trimmed from %d to %d chars to stay within limit",
                len(post_text), len(text),
            )

        return text

    def _generate_diagram_data(self, day: int, title: str, category: str, article_summary: str = "") -> dict | None:
        """
        Ask AI to pick the best diagram type and produce the data dict for the
        LinkedIn post image right panel. Returns a diagram dict or None on failure.

        Diagram types:
          hub_spoke  — architecture / component overview posts
          comparison — vs / difference / compare posts
          tiers      — pricing / capacity / scale posts
          flow       — pipeline / process / step-by-step posts
        """
        gemini_key   = getattr(self.content_generator, "gemini_api_key",   None)
        anthropic_key = getattr(self.content_generator, "anthropic_api_key", None)
        if not gemini_key and not anthropic_key:
            return None

        prompt = f"""You are generating diagram data for a LinkedIn post image about Microsoft Fabric.

Topic: Day {day} — {title}
Category: {category}
{('Article summary: ' + article_summary[:400]) if article_summary else ''}

Choose ONE diagram type that best represents this topic and return ONLY valid JSON (no markdown, no explanation):

Option A — hub_spoke (best for: architecture, components, ecosystem overviews)
{{"type":"hub_spoke","center":"short label\\nor two lines","nodes":["Node 1","Node 2","Node 3","Node 4","Node 5","Node 6"]}}
Rules: center max 2 lines (use \\n), 5-7 nodes, each node max 12 chars per line, use \\n for 2-line nodes.

Option B — comparison (best for: vs, difference, compare, choose between)
{{"type":"comparison","columns":["Category","Option A","Option B","Option C"],"rows":[["Row label","val","val","val"],["Row label","val","val","val"],["Row label","val","val","val"],["Row label","val","val","val"]]}}
Rules: exactly 4 columns (first is row label), 3-5 rows, cell values max 14 chars, use abbreviations.

Option C — tiers (best for: pricing, capacity, scale, SKU tiers, levels)
{{"type":"tiers","tiers":[{{"name":"Tier1","detail":"label"}},{{"name":"Tier2","detail":"label"}},{{"name":"Tier3","detail":"label"}},{{"name":"Tier4","detail":"label"}},{{"name":"Tier5","detail":"label"}},{{"name":"Tier6","detail":"label"}}]}}
Rules: 4-6 tiers left=smallest right=largest, name max 6 chars, detail max 10 chars (split long words with space).

Option D — flow (best for: pipeline, process, steps, workflow, ingestion, ETL)
{{"type":"flow","steps":["Step1","Step2","Step3","Step4","Step5"]}}
Rules: 4-6 steps, each step max 10 chars (2-word labels OK), shown left→right with arrows.

Return ONLY the JSON object. No markdown fences, no explanation text."""

        # Try Gemini first (faster, cheaper)
        if gemini_key:
            for model in ["gemini-2.0-flash", "gemini-2.5-flash"]:
                try:
                    r = requests.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}",
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {"maxOutputTokens": 512, "temperature": 0.3},
                        },
                        timeout=30,
                    )
                    if r.status_code == 200:
                        resp = r.json()
                        finish = (resp.get("candidates", [{}])[0].get("finishReason", ""))
                        if finish in ("STOP", ""):
                            raw = resp["candidates"][0]["content"]["parts"][0]["text"].strip()
                            # Strip markdown fences if present
                            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("` \n")
                            diagram = json.loads(raw)
                            logger.info("✅ Diagram data generated via Gemini (%s) for Day %s: type=%s", model, day, diagram.get("type"))
                            return diagram
                except json.JSONDecodeError as e:
                    logger.warning("Diagram JSON parse error (Gemini %s): %s", model, e)
                except Exception as e:
                    logger.warning("Diagram generation error (Gemini %s): %s", model, e)

        # Anthropic fallback
        if anthropic_key:
            try:
                r = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                        "max_tokens": 512,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=30,
                )
                if r.status_code == 200:
                    raw = r.json()["content"][0]["text"].strip()
                    raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("` \n")
                    diagram = json.loads(raw)
                    logger.info("✅ Diagram data generated via Anthropic for Day %s: type=%s", day, diagram.get("type"))
                    return diagram
            except json.JSONDecodeError as e:
                logger.warning("Diagram JSON parse error (Anthropic): %s", e)
            except Exception as e:
                logger.warning("Diagram generation error (Anthropic): %s", e)

        logger.warning("Day %s: diagram generation failed — falling back to pills", day)
        return None

    def resolve_linkedin_post_text(
        self, day: int, day_content: Dict, article_url: str
    ) -> str:
        """Return the best LinkedIn post text available.

        Priority:
        1. Predefined schedule content — if present AND not boilerplate
        2. AI-generated post (Gemini / Anthropic) — if FABRIC_USE_AI=true and key available
        3. Template fallback (create_linkedin_post)
        """
        raw = (day_content.get("linkedin_content") or "").strip()
        use_ai = os.getenv("FABRIC_USE_AI", "").lower() in ("1", "true", "yes")

        # Use predefined content only if it looks hand-crafted (not boilerplate)
        if raw and not self._is_boilerplate_linkedin(raw):
            logger.info("Day %s: using predefined linkedin_content from schedule", day)
            normalized = normalize_fabric_article_urls(raw, article_url)
            return self._ensure_linkedin_url_hashtags(normalized, article_url, day_content)

        # Try AI generation
        if use_ai and (
            getattr(self.content_generator, "gemini_api_key", None)
            or getattr(self.content_generator, "anthropic_api_key", None)
        ):
            logger.info("Day %s: generating LinkedIn post via AI", day)
            return self._generate_linkedin_with_ai(day, day_content, article_url)

        logger.info("Day %s: using LinkedIn post template", day)
        return self.create_linkedin_post(day, day_content, article_url)

    # ================================================================== #
    #  YouTube video helpers                                               #
    # ================================================================== #

    def _generate_narration_script(
        self, day: int, title: str, category: str, day_content: Dict, article_url: str
    ) -> str:
        """Generate a full 4-5 minute narration covering the complete article.

        Targets ~500-550 words (spoken at ~130 words/min = ~4 minutes).
        Tries Gemini → Anthropic → fallback template.
        """
        use_ai        = os.getenv("FABRIC_USE_AI", "").lower() in ("1", "true", "yes")
        gemini_key    = getattr(self.content_generator, "gemini_api_key",    None)
        anthropic_key = getattr(self.content_generator, "anthropic_api_key", None)

        prompt = (
            f"Write a 4-5 minute spoken YouTube narration for Day {day}/100 of Microsoft Fabric 100 Days.\n\n"
            f"Topic: {title}\nCategory: {category}\n\n"
            f"Follow this exact structure (each section is one paragraph, plain text, no bullets):\n\n"
            f"INTRO (25 words): Start exactly with 'Day {day} of 100 Days of Microsoft Fabric. "
            f"Today we are covering {title}.' Then add one surprising fact or common pain point.\n\n"
            f"WHAT IT IS (70 words): Define {title} in plain English. Explain where it fits in "
            f"Microsoft Fabric's architecture. Use a real analogy if helpful.\n\n"
            f"HOW IT WORKS (100 words): Explain the core mechanics — real Fabric feature names, "
            f"actual numbers (storage limits, SKU sizes, performance metrics), how components connect.\n\n"
            f"KEY CAPABILITIES (100 words): Cover 3 specific capabilities using real Microsoft "
            f"Fabric feature names. Explain what each one enables in practice.\n\n"
            f"REAL WORLD EXAMPLE (80 words): One concrete scenario — a company type, "
            f"the problem they faced, and exactly how {title} solved it. Be specific.\n\n"
            f"BEST PRACTICES (80 words): Three things experienced Fabric engineers always do "
            f"when working with {title}. Actionable, specific, technical.\n\n"
            f"COMMON MISTAKES (60 words): Two mistakes practitioners make with {title} and how to avoid them.\n\n"
            f"WRAP UP (35 words): Summarise the single most important thing about {title}. "
            f"End with: 'The full article with code examples is linked in the description. "
            f"See you tomorrow for Day {day + 1}.'\n\n"
            f"Rules: 500-560 words total. Plain text only, no markdown, no bullet symbols. "
            f"Conversational but authoritative. Every sentence must contain real, specific Fabric details."
        )

        def _call_gemini(p):
            for model in ["gemini-2.0-flash", "gemini-2.5-flash"]:
                try:
                    r = requests.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/"
                        f"{model}:generateContent?key={gemini_key}",
                        json={
                            "contents": [{"parts": [{"text": p}]}],
                            "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.65},
                        },
                        timeout=45,
                    )
                    if r.status_code == 200:
                        resp   = r.json()
                        finish = resp.get("candidates", [{}])[0].get("finishReason", "")
                        if finish in ("STOP", ""):
                            text = resp["candidates"][0]["content"]["parts"][0]["text"].strip()
                            logger.info("✅ Narration via Gemini (%s): %d words", model, len(text.split()))
                            return text
                except Exception as e:
                    logger.warning("Narration Gemini (%s) error: %s", model, e)
            return ""

        def _call_anthropic(p):
            try:
                r = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                        "max_tokens": 1024,
                        "messages": [{"role": "user", "content": p}],
                    },
                    timeout=45,
                )
                if r.status_code == 200:
                    text = r.json()["content"][0]["text"].strip()
                    logger.info("✅ Narration via Anthropic: %d words", len(text.split()))
                    return text
            except Exception as e:
                logger.warning("Narration Anthropic error: %s", e)
            return ""

        if use_ai:
            if gemini_key:
                result = _call_gemini(prompt)
                if result:
                    return result
            if anthropic_key:
                result = _call_anthropic(prompt)
                if result:
                    return result

        # Fallback template (~500 words, covers all sections)
        hashtags = [h for h in (day_content.get("hashtags") or [])
                    if h not in ("MicrosoftFabric", "100DaysChallenge", "Azure", "Analytics")]
        features = ", ".join(hashtags[:3]) if hashtags else f"{title} features"
        return (
            f"Day {day} of 100 Days of Microsoft Fabric. Today we are covering {title}. "
            f"If you have ever struggled to understand where {title} fits in the Microsoft Fabric ecosystem, this video is for you. "
            f"\n\n"
            f"{title} is a core component of {category} in Microsoft Fabric. "
            f"Microsoft Fabric is a unified analytics platform that brings together data engineering, data science, "
            f"real-time analytics, and business intelligence into a single SaaS product built on OneLake. "
            f"{title} plays a critical role in this unified experience by providing a standardised, governed way "
            f"to work with data at enterprise scale. "
            f"\n\n"
            f"Here is how {title} works in practice. When you create a workspace in Microsoft Fabric, "
            f"all Fabric items within that workspace automatically store their data in OneLake using Delta Parquet format. "
            f"This means there is no data duplication between workloads. "
            f"{title} connects directly to this unified storage layer, making it possible to work across "
            f"lakehouses, warehouses, and reports without any data movement. "
            f"\n\n"
            f"The key capabilities you get with {title} include {features}. "
            f"Each of these capabilities is designed to work seamlessly with the rest of the Fabric platform. "
            f"You get enterprise-grade security and governance built in through Microsoft Purview integration, "
            f"role-based access control at the workspace and item level, and full audit logging. "
            f"\n\n"
            f"Let me give you a real world example. Consider a retail company with data spread across "
            f"Azure Data Lake Storage, on-premises SQL Server, and third-party SaaS tools. "
            f"By adopting {title} in Microsoft Fabric, their data engineering team was able to consolidate "
            f"all pipelines into a single platform, cut their reporting pipeline from four hours to under thirty minutes, "
            f"and give business analysts self-service access to live data without any extra infrastructure. "
            f"\n\n"
            f"From a best practices perspective, experienced Fabric engineers always start with a dedicated development "
            f"workspace before moving to production. They use F64 or higher SKU capacity for full feature access, "
            f"since features like Spark and advanced analytics are restricted below F64. "
            f"They also set up Git integration from day one so all Fabric items are version controlled. "
            f"\n\n"
            f"The two most common mistakes people make with {title} are trying to lift and shift existing "
            f"Azure Synapse workloads without redesigning for Fabric's cost unit model, "
            f"and ignoring capacity planning until they hit throttling in production. "
            f"Both of these are avoidable if you spend time on capacity sizing before you go live. "
            f"\n\n"
            f"The single most important thing to remember about {title} is that it is designed to eliminate "
            f"the boundaries between data engineering and analytics. "
            f"If you master {title}, you will be able to build faster, more reliable, "
            f"and significantly cheaper data solutions on Microsoft Fabric. "
            f"The full article with step-by-step examples and architecture diagrams is linked in the description. "
            f"See you tomorrow for Day {day + 1}."
        )

    def _create_video_from_image_audio(
        self, img_path: str, audio_path: str, output_path: str
    ) -> bool:
        """Combine a static PNG and an MP3 into an MP4 using ffmpeg.

        ffmpeg is pre-installed on GitHub Actions ubuntu-latest.
        On macOS: brew install ffmpeg.
        """
        import subprocess

        try:
            cmd = [
                "ffmpeg", "-y",
                # Input: loop the image for the duration of the audio
                "-loop", "1", "-framerate", "1", "-i", img_path,
                # Input: narration audio
                "-i", audio_path,
                # Scale to 1280x720, letterbox with dark background, convert pixel format
                "-vf", (
                    "scale=1280:670:force_original_aspect_ratio=decrease,"
                    "pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=#08343a,"
                    "format=yuv420p"
                ),
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-tune", "stillimage",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest",          # end when audio ends
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            if result.returncode != 0:
                logger.error("ffmpeg stderr: %s", result.stderr[-800:])
                return False
            logger.info("✅ Video created: %s", output_path)
            return True

        except FileNotFoundError:
            logger.warning(
                "ffmpeg not found — install it to enable YouTube video generation. "
                "Ubuntu: sudo apt-get install ffmpeg | macOS: brew install ffmpeg"
            )
            return False
        except subprocess.TimeoutExpired:
            logger.error("ffmpeg timed out while creating video")
            return False
        except Exception as e:
            logger.error("Video creation error: %s", e)
            return False

    # ================================================================== #
    #  Presenter-video helpers (D-ID + multi-slide)                        #
    # ================================================================== #

    def _build_slide_prompt(self, day: int, title: str, category: str) -> str:
        """
        Build the same quality prompt used for article generation, but targeting
        structured JSON for 12 slides instead of HTML.

        Mirrors _build_article_prompt's depth: real feature names, real limits,
        expert-level specifics, no filler.
        """
        title_lower = title.lower()
        no_code = any(kw in title_lower for kw in (
            "what is", " vs ", "overview", "introduction", "pricing",
            "comparison", "architecture", "capacities", "licensing",
            "administration", "governance", "security", "roles",
        ))

        extra_slide_note = "" if no_code else (
            "Include one slide (feature1 or feature2) that covers a hands-on code concept "
            "— e.g. a PySpark pattern, T-SQL snippet concept, or KQL approach. "
            "Express it as 3 concise bullets describing what the code does, not the code itself."
        )

        return f"""You are a senior Microsoft Fabric architect creating content for Day {day}/100 of a 100-day technical YouTube series.

Topic: {title}
Category: {category}
Audience: Data engineers and analysts learning Microsoft Fabric

Generate structured content for a 12-slide presentation. Return ONLY valid JSON — no markdown fences, no explanation text.

The JSON must match this exact schema:
{{
  "hook": "One arresting question or non-obvious fact about {title}. Must include a real number, limit, or comparison. Max 22 words.",
  "what_is": "Expert plain-English definition of {title} in 2 sentences. Include where it sits in Fabric's architecture and what problem it solves. Max 45 words.",
  "overview": [
    "What {title} is and the problem it solves (max 10 words)",
    "The internal mechanism — how it actually works (max 10 words)",
    "The two most important capabilities with real feature names (max 10 words)",
    "What practitioners must know: limits, SKUs, or pitfalls (max 10 words)"
  ],
  "how_works_heading": "Precise technical heading, 3-5 words",
  "how_works_bullets": [
    "Core mechanic with real Fabric feature name and specific behaviour (max 14 words)",
    "Second mechanic — different layer or integration point (max 14 words)",
    "Third mechanic — storage format, security model, or compute path (max 14 words)"
  ],
  "feature1_heading": "First key capability name, 3-5 words",
  "feature1_bullets": [
    "Specific capability with real limit or metric (max 14 words)",
    "Second aspect — integration or compatibility detail (max 14 words)",
    "Third aspect — performance or cost implication (max 14 words)"
  ],
  "feature2_heading": "Second key capability name, 3-5 words",
  "feature2_bullets": [
    "Specific capability — different dimension from feature1 (max 14 words)",
    "Second aspect (max 14 words)",
    "Third aspect (max 14 words)"
  ],
  "usecase_heading": "Concrete enterprise scenario, 5-7 words",
  "usecase_bullets": [
    "The business problem this company faced — specific industry and scale (max 14 words)",
    "The specific {title} feature or configuration they used (max 14 words)",
    "Measurable outcome: time saved, cost reduced, or throughput gained (max 14 words)"
  ],
  "best_practices": [
    "Practice 1 — starts with an action verb, includes a specific recommendation (max 14 words)",
    "Practice 2 — different aspect, expert-level non-obvious tip (max 14 words)",
    "Practice 3 — operational or governance consideration (max 14 words)"
  ],
  "mistakes": [
    "Mistake 1 — the exact wrong thing people do and why it fails (max 14 words)",
    "Mistake 2 — different mistake, different failure mode (max 14 words)"
  ],
  "takeaways": [
    "Most important insight — starts with Use/Avoid/Remember/Always/Never (max 14 words)",
    "Second insight — different dimension (max 14 words)",
    "Third insight — action for the viewer (max 14 words)"
  ]
}}

{extra_slide_note}

RULES (same as our article quality bar):
- Every value must contain real Microsoft Fabric feature names: OneLake, Delta Parquet, DirectLake, F-SKU names (F2/F8/F64/F256), Lakehouse, Warehouse, Data Factory, Spark, KQL, Purview, Shortcuts, etc.
- No generic phrases: "improves performance", "enhances productivity", "game-changer", "revolutionize"
- Numbers and specifics beat adjectives: "F64 required for full Spark" beats "higher SKU recommended"
- If a fact is uncertain, omit it rather than guess
- Return ONLY the JSON object — nothing before or after it"""

    def _extract_article_content(self, day: int, title: str) -> Dict:
        """
        Parse the published article HTML for this day and return a slide_content dict.
        Returns {} if the file is not found or parsing fails.
        """
        import re as _re

        base         = os.path.dirname(os.path.abspath(__file__))
        articles_dir = os.path.join(base, "articles", "fabric-100-days")
        html_path    = None
        if os.path.isdir(articles_dir):
            for fn in sorted(os.listdir(articles_dir)):
                if fn.startswith(f"day-{day}-") and fn.endswith(".html"):
                    html_path = os.path.join(articles_dir, fn)
                    break
        if not html_path:
            return {}

        try:
            with open(html_path, encoding="utf-8") as f:
                html = f.read()
        except Exception:
            return {}

        def strip_tags(t):
            t = _re.sub(r"<[^>]+>", " ", t)
            return _re.sub(r"\s+", " ", t).strip()

        # Column headers to skip when extracting table bullets
        _TABLE_HEADERS = {
            "workload","what it does","primary persona","feature","description",
            "old azure stack","old stack","fabric equivalent","service","component",
            "item","name","value","metric","type","category","detail",
        }

        def extract_table_bullets(html_chunk):
            """Extract <table> data rows as 'Label — description' bullets, skipping header rows."""
            rows = _re.findall(r"<tr>(.*?)</tr>", html_chunk, _re.S)
            results = []
            for row in rows:
                # Skip header rows (th tags)
                if "<th" in row:
                    continue
                cells = [strip_tags(c) for c in _re.findall(r"<td[^>]*>(.*?)</td>", row, _re.S)]
                cells = [c for c in cells if c]
                # Skip rows where the first cell looks like a column header
                if cells and cells[0].lower().rstrip(":").strip() in _TABLE_HEADERS:
                    continue
                if len(cells) >= 2:
                    results.append(f"{cells[0]} — {cells[1]}")
                elif len(cells) == 1 and len(cells[0]) > 8:
                    results.append(cells[0])
            return results[:5]

        # Split into sections by h2/h3
        parts    = _re.split(r"(<h[23][^>]*>.*?</h[23]>)", html, flags=_re.S)
        sections = []
        heading  = None
        for part in parts:
            if _re.match(r"<h[23]", part):
                heading = strip_tags(part)
            elif heading:
                bullets = [strip_tags(b) for b in _re.findall(r"<li>(.*?)</li>", part, _re.S)]
                paras   = [strip_tags(p) for p in _re.findall(r"<p>(.*?)</p>",  part, _re.S)]
                # Also extract table rows as bullets if no <li> found
                if not bullets and "<table" in part:
                    bullets = extract_table_bullets(part)
                bullets = [b for b in bullets if len(b) > 10]
                paras   = [p for p in paras   if len(p) > 20]
                if bullets or paras:
                    sections.append({
                        "heading": heading,
                        "bullets": bullets[:5],
                        "para":    paras[0][:300] if paras else "",
                    })
                heading = None

        if not sections:
            return {}

        # First paragraph = definition
        m          = _re.search(r"<p>(.*?)</p>", html, _re.S)
        first_para = strip_tags(m.group(1))[:300] if m else ""

        def find_s(*kws):
            kws_l = [k.lower() for k in kws]
            for s in sections:
                if any(k in s["heading"].lower() for k in kws_l):
                    return s
            return None

        def clean_bullet(b):
            """Strip checklist/icon prefixes from bullet text."""
            return _re.sub(r"^[☑✓✗▶◆•\-→\s]+", "", b).strip()

        def content(s, n=3):
            if not s:
                return []
            items = [clean_bullet(b) for b in s["bullets"] if clean_bullet(b)][:n]
            if not items and s["para"]:
                words = s["para"].split()
                chunk = " ".join(words[:25])
                items = [chunk]
            return items[:n]

        takeaways_s  = find_s("takeaway", "key point", "key take", "summary", "conclusion")
        best_pract_s = find_s("best practice", "checklist", "getting started", "recommendation", "quick start")
        mistakes_s   = find_s("mistake", "avoid", "pitfall", "common error", "don't")
        usecase_s    = find_s("use case", "example", "enterprise", "real world", "scenario", "why it matters", "impact")
        how_works_s  = find_s("how it work", "architecture", "how fabric", "mechanism")

        special_list  = [s for s in [takeaways_s, best_pract_s, mistakes_s, usecase_s, how_works_s] if s]
        content_sects = [s for s in sections if s not in special_list]

        overview = [s["heading"] for s in sections if s["heading"].lower() != title.lower()][:4]
        while len(overview) < 4:
            overview.append(["Key features and capabilities", "Best practices", "Real-world use cases", "Getting started"][len(overview)])

        # Prefer sections that have actual bullets over paragraph-only sections
        def has_bullets(s):
            return bool(s and s.get("bullets"))

        # Sections with real bullets, not in special list
        bullet_sects = [s for s in content_sects if has_bullets(s)]

        hw   = how_works_s or (bullet_sects[0] if bullet_sects else content_sects[0] if content_sects else None)
        # Don't use the article title itself as the how-it-works heading
        if hw and hw["heading"].lower().rstrip("?").strip() == title.lower().rstrip("?").strip():
            hw_h = "Core Architecture & Components"
        else:
            hw_h = hw["heading"] if hw else f"How {title} Works"
        hw_b = content(hw, 3)

        remaining = [s for s in bullet_sects if s is not hw] or \
                    [s for s in content_sects if s is not hw]
        f1 = remaining[0] if remaining else None
        f2 = remaining[1] if len(remaining) > 1 else None

        uc   = usecase_s or (content_sects[-1] if content_sects else None)
        uc_h = uc["heading"] if uc else f"Enterprise Use of {title}"
        uc_b = content(uc, 3)

        bp = content(best_pract_s, 3)
        if not bp:
            # fall back to checklist items
            checklist = [clean_bullet(strip_tags(c)) for c in _re.findall(r"☑[^<\n]*", html)]
            bp = [c[:130] for c in checklist[:3] if len(c) > 10]

        mistakes = content(mistakes_s, 3)
        if not mistakes:
            # Build sensible mistakes from best practices (invert their advice)
            if bp:
                # Turn "Do X" into "Skipping X" or use a topic-specific fallback
                first_bp = bp[0]
                # Strip leading verbs to make "Skipping <noun phrase>"
                first_bp_noun = first_bp.lstrip("Create Enable Set Use Build Add Run").strip().lstrip()
                mistakes = [
                    f"Skipping {first_bp_noun[:80]}" if first_bp_noun else "Skipping initial capacity planning",
                    "Not sizing capacity in dev before promoting to production — F2/F4 throttle under load",
                ][:2]
            else:
                mistakes = [
                    "Skipping capacity planning before deploying Fabric workloads",
                    "Not enabling Microsoft Purview data governance from day one",
                ]

        takeaways = content(takeaways_s, 3)
        if not takeaways:
            # Use the last non-special sections' headings as takeaways
            takeaways = [s["para"][:100] if s["para"] else s["heading"]
                         for s in sections[-4:] if s.get("para") or s.get("heading")][:3]
            if not takeaways:
                takeaways = [s["heading"] for s in sections[-3:]][:3]

        # Hook: don't repeat "Microsoft Fabric" if it's already in title
        short_title = title.replace("What is ", "").replace("Understanding ", "").rstrip("?").strip()
        if "Microsoft Fabric" in short_title or "Fabric" in short_title:
            hook = f"Everything you need to know about {short_title}."
        else:
            hook = f"Your complete guide to {short_title} in Microsoft Fabric."

        return {
            "hook":              hook,
            "what_is":           first_para,
            "overview":          overview,
            "how_works_heading": hw_h,
            "how_works_bullets": hw_b,
            "feature1_heading":  f1["heading"] if f1 else "Core Features",
            "feature1_bullets":  content(f1, 3),
            "feature2_heading":  f2["heading"] if f2 else "Advanced Capabilities",
            "feature2_bullets":  content(f2, 3),
            "usecase_heading":   uc_h,
            "usecase_bullets":   uc_b,
            "best_practices":    bp,
            "mistakes":          mistakes,
            "takeaways":         takeaways,
        }

    def _generate_tts_audio(self, text: str, output_path: str) -> bool:
        """
        Generate TTS speech audio.
        Priority: edge-tts (en-US-ChristopherNeural, male neural) → gTTS fallback.
        """
        # edge-tts: free, unlimited, high-quality Microsoft neural TTS
        try:
            import asyncio
            import edge_tts  # pip install edge-tts

            async def _speak():
                communicate = edge_tts.Communicate(text, "en-US-ChristopherNeural")
                await communicate.save(output_path)

            asyncio.run(_speak())
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                logger.info("✅ TTS via edge-tts (en-US-ChristopherNeural, male neural)")
                return True
        except Exception as e:
            logger.warning("edge-tts failed (%s) — falling back to gTTS", e)

        # gTTS fallback
        try:
            from gtts import gTTS
            gTTS(text=text, lang="en", slow=False).save(output_path)
            logger.info("✅ TTS via gTTS (fallback)")
            return True
        except Exception as e:
            logger.error("TTS error: %s", e)
            return False

    def _generate_slide_content(
        self, day: int, title: str, category: str,
        day_content: Dict, article_url: str,
    ) -> Dict:
        """
        Generate rich, article-quality content for all 12 slides.

        Priority:
          1. Parse published article HTML (always real content, no AI needed)
          2. FABRIC_USE_AI=true + Gemini key → Gemini (gemini-2.5-pro → 2.5-flash → 2.0-flash)
          3. FABRIC_USE_AI=true + Anthropic key → Claude
          4. Schedule-aware template fallback
        """
        # ── 1. Article HTML extraction (highest priority) ──────────────────
        article_data = self._extract_article_content(day, title)
        if article_data:
            logger.info("Day %s slides: content extracted from published article HTML", day)
            return article_data

        use_ai        = os.getenv("FABRIC_USE_AI", "").lower() in ("1", "true", "yes")
        gemini_key    = getattr(self.content_generator, "gemini_api_key",    None)
        anthropic_key = getattr(self.content_generator, "anthropic_api_key", None)

        # ── Template fallback (built from schedule data) ───────────────────
        lines = [
            ln.strip().lstrip("•◆-→ ").strip()
            for ln in (day_content.get("linkedin_content") or "").splitlines()
            if ln.strip().startswith(("•", "◆", "-", "→")) and len(ln.strip()) > 6
        ]
        hashtags = [
            h.replace("_", " ")
            for h in (day_content.get("hashtags") or [])
            if h not in ("MicrosoftFabric", "100DaysChallenge", "Azure", "Analytics")
        ]
        ov = (lines[:4] if len(lines) >= 2 else hashtags[:4]) or [
            f"What {title} is and why it matters",
            f"How {title} fits into Microsoft Fabric",
            "Key features, capabilities, and limits",
            "Best practices and common mistakes",
        ]
        fallback = {
            "hook":               f"Most data engineers don't realise how much {title} can simplify their architecture.",
            "what_is":            f"{title} is a core component of {category} in Microsoft Fabric that enables unified, governed data operations without data duplication.",
            "overview":           ov[:4],
            "how_works_heading":  f"How {title} Works",
            "how_works_bullets":  [f"{title} stores data in OneLake as Delta Parquet", "No data movement between Fabric workloads", "Security and governance via Microsoft Purview"],
            "feature1_heading":   "Enterprise Integration",
            "feature1_bullets":   ["Connects to ADLS Gen2, S3, GCS via Shortcuts", "OneLake acts as a single logical data lake", "All Fabric experiences share the same data"],
            "feature2_heading":   "Governance & Security",
            "feature2_bullets":   ["Role-based access at workspace and item level", "Full audit logging via Microsoft Purview", "Data lineage tracked automatically"],
            "usecase_heading":    "Retail Chain Unifies 12 Data Sources",
            "usecase_bullets":    ["Retail chain had data across ADLS Gen2, SQL, and SaaS tools", f"Used {title} to consolidate into a single Fabric workspace", "Reporting time cut from 4 hours to under 30 minutes"],
            "best_practices":     ["Start in a dev F2 workspace before promoting to F64 production", "Use F64 or higher SKU — Spark and advanced features require it", "Enable Git integration from day one for all Fabric items"],
            "mistakes":           [f"Lifting Synapse workloads to {title} without redesigning for Fabric CU model", "Skipping capacity planning — F2/F4 throttle under real workloads"],
            "takeaways":          [f"Use {title} to eliminate data silos across your Fabric workspace", "Always size capacity in dev before going to production", "Read the full article for hands-on code and architecture diagrams"],
        }

        if not (use_ai and (gemini_key or anthropic_key)):
            logger.info("Day %s slides: using template (no AI key or FABRIC_USE_AI not set)", day)
            return fallback

        prompt = self._build_slide_prompt(day, title, category)

        # ── Gemini (same model cascade as article generation) ──────────────
        if gemini_key:
            for model in [os.getenv("GEMINI_MODEL", "gemini-2.5-pro"), "gemini-2.5-flash", "gemini-2.0-flash"]:
                try:
                    r = requests.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}",
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.35, "topP": 0.95},
                        },
                        timeout=90,
                    )
                    if r.status_code == 429:
                        logger.warning("Gemini %s quota — trying next model", model)
                        continue
                    if r.status_code == 200:
                        resp = r.json()
                        if resp.get("candidates", [{}])[0].get("finishReason", "") in ("STOP", ""):
                            raw = resp["candidates"][0]["content"]["parts"][0]["text"].strip()
                            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("` \n")
                            result = json.loads(raw)
                            logger.info("✅ Slide content via Gemini (%s) for Day %s", model, day)
                            return result
                except json.JSONDecodeError as e:
                    logger.warning("Slide JSON parse error (Gemini %s): %s", model, e)
                except Exception as e:
                    logger.warning("Slide content Gemini (%s) error: %s", model, e)
                    break

        # ── Anthropic (same as article generation) ─────────────────────────
        if anthropic_key:
            try:
                r = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                        "max_tokens": 2048,
                        "temperature": 0.35,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=90,
                )
                if r.status_code == 200:
                    raw = r.json()["content"][0]["text"].strip()
                    raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("` \n")
                    result = json.loads(raw)
                    logger.info("✅ Slide content via Anthropic for Day %s", day)
                    return result
                logger.warning("Anthropic slide content failed (%s): %s", r.status_code, r.text[:200])
            except json.JSONDecodeError as e:
                logger.warning("Slide JSON parse error (Anthropic): %s", e)
            except Exception as e:
                logger.warning("Slide content Anthropic error: %s", e)

        logger.warning("Day %s: AI slide content failed — using template fallback", day)
        return fallback

    def _render_presentation_slides(
        self,
        day: int,
        title: str,
        category: str,
        slide_content: Dict,
        diagram: Dict,
        tmp_dir: str,
    ) -> List[str]:
        """
        Render 12 branded Full-HD PNG slides (1920×1080) into tmp_dir.
        Returns list of file paths in order. Returns [] if Pillow unavailable.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            logger.warning("Pillow unavailable — cannot render slides")
            return []

        import io as _io, math as _math

        W, H = 1920, 1080   # Full HD
        PAD  = 80

        # ── Brand palette ──────────────────────────────────────────────────
        BG       = (8,   52,  58)
        BG2      = (12,  68,  75)
        BG3      = (18,  85,  92)
        ACCENT   = (56, 212, 196)
        ACCENT2  = (34, 170, 155)
        GOLD     = (255, 200,  80)
        GREEN    = (30, 185,  90)
        RED      = (220,  70,  70)
        WHITE    = (255, 255, 255)
        OFFWHITE = (210, 238, 235)
        MUTED    = (120, 175, 170)
        DARK     = (6,   42,  48)

        # ── Font loader (same cascade as generate_post_image) ──────────────
        def lf(size, bold=False):
            paths = (
                ["/System/Library/Fonts/Helvetica.ttc",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                 "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"]
                if bold else
                ["/System/Library/Fonts/Helvetica.ttc",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                 "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"]
            )
            for p in paths:
                try: return ImageFont.truetype(p, size)
                except: pass
            return ImageFont.load_default()

        # ── Profile photo bytes ────────────────────────────────────────────
        _photo_bytes = None
        for _pp in ["profile_photo.png", "profile_photo.jpg",
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile_photo.png")]:
            if os.path.exists(_pp):
                try:
                    with open(_pp, "rb") as _f: _photo_bytes = _f.read()
                    break
                except: pass

        def make_canvas() -> tuple:
            img  = Image.new("RGB", (W, H))
            draw = ImageDraw.Draw(img)
            for y in range(H):
                t = y / H
                draw.line([(0,y),(W,y)], fill=(
                    int(BG[0]+t*8), int(BG[1]+t*14), int(BG[2]+t*14)
                ))
            for gx in range(30, W, 40):
                for gy in range(30, H, 40):
                    draw.ellipse([gx-1,gy-1,gx+1,gy+1], fill=(22,82,88))
            return img, draw

        def header_footer(draw, day, title_short=""):
            # Header bar
            draw.rectangle([0,0,W,54], fill=(5,38,44))
            draw.line([(0,54),(W,54)], fill=ACCENT2, width=2)
            draw.text((PAD, 16), "100 DAYS OF MICROSOFT FABRIC", font=lf(18), fill=ACCENT2)
            d_str = f"Day {day} / 100"
            d_bb  = draw.textbbox((0,0), d_str, font=lf(18, bold=True))
            draw.text((W-(d_bb[2]-d_bb[0])-PAD, 16), d_str, font=lf(18, bold=True), fill=GOLD)
            # Footer: progress bar
            draw.rectangle([0, H-44, W, H], fill=(5,38,44))
            draw.line([(0, H-44),(W, H-44)], fill=ACCENT2, width=1)
            prog = min(1.0, day/100)
            draw.rounded_rectangle([PAD, H-22, W-PAD, H-10], radius=4, fill=(22,70,78))
            if prog > 0:
                draw.rounded_rectangle([PAD, H-22, PAD+int((W-PAD*2)*prog), H-10], radius=4, fill=ACCENT)

        def avatar_circle(img, draw, cx, cy, r):
            if _photo_bytes:
                try:
                    ph = Image.open(_io.BytesIO(_photo_bytes)).convert("RGBA")
                    aw, ah = ph.size
                    s = min(aw,ah)
                    ph = ph.crop(((aw-s)//2, max(0,ah//6-s//8), (aw-s)//2+s, max(0,ah//6-s//8)+s))
                    ph = ph.resize((r*2,r*2), Image.LANCZOS)
                    mask = Image.new("L",(r*2,r*2),0)
                    ImageDraw.Draw(mask).ellipse([0,0,r*2,r*2], fill=255)
                    av = Image.new("RGBA",(r*2,r*2),(0,0,0,0))
                    av.paste(ph,(0,0),mask)
                    img.paste(av,(cx-r,cy-r),av)
                    draw.ellipse([cx-r-2,cy-r-2,cx+r+2,cy+r+2], outline=ACCENT, width=2)
                    return
                except: pass
            draw.ellipse([cx-r,cy-r,cx+r,cy+r], fill=ACCENT)
            draw.text((cx-10,cy-13), "MS", font=lf(18,bold=True), fill=DARK)

        def wrap_text(draw, text, font, max_w):
            words, lines, cur = text.split(), [], []
            for w in words:
                test = " ".join(cur+[w])
                if draw.textbbox((0,0),test,font=font)[2] > max_w and cur:
                    lines.append(" ".join(cur)); cur=[w]
                else: cur.append(w)
            if cur: lines.append(" ".join(cur))
            return lines

        def save_slide(img, idx):
            path = os.path.join(tmp_dir, f"slide_{idx:02d}.png")
            img.save(path, "PNG", optimize=False)
            return path

        def section_header(draw, text, y=84, color=None):
            col = color or ACCENT2
            draw.text((PAD, y), text, font=lf(26, bold=True), fill=col)
            bb = draw.textbbox((0, 0), text, font=lf(26, bold=True))
            draw.line([(PAD, y + bb[3]-bb[1]+8), (W-PAD, y + bb[3]-bb[1]+8)], fill=col, width=2)

        def content_bullets(draw, items, y_start, icon=None, icon_col=ACCENT,
                            font_size=32, line_gap=20):
            """Draw a bullet list. icon=None uses a filled rounded square marker."""
            y = y_start
            for item in items:
                # Bullet marker: filled rounded rectangle (works with all fonts)
                bsize = max(10, font_size // 2)
                bx, by = PAD, y + (font_size - bsize) // 2 + 2
                if icon == "✗":
                    # Red X mark
                    draw.line([(bx, by), (bx+bsize, by+bsize)], fill=icon_col, width=3)
                    draw.line([(bx+bsize, by), (bx, by+bsize)], fill=icon_col, width=3)
                elif icon == "✓":
                    # Green check
                    draw.line([(bx, by+bsize//2), (bx+bsize//3, by+bsize)], fill=icon_col, width=3)
                    draw.line([(bx+bsize//3, by+bsize), (bx+bsize, by)], fill=icon_col, width=3)
                else:
                    draw.rounded_rectangle([bx, by, bx+bsize, by+bsize], radius=3, fill=icon_col)
                lines = wrap_text(draw, item, lf(font_size), W-PAD*2-bsize-16)[:2]
                for ln in lines:
                    draw.text((PAD+bsize+12, y), ln, font=lf(font_size), fill=OFFWHITE)
                    y += font_size + 8
                y += line_gap
            return y

        paths = []

        # ── SLIDE 1: Title ─────────────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        # Day badge top-left
        draw.rounded_rectangle([PAD, 74, PAD+130, 114], radius=20, fill=ACCENT)
        draw.text((PAD+14, 82), f"DAY {day}/100", font=lf(22, bold=True), fill=DARK)
        # Series label
        draw.text((PAD+150, 84), "100 DAYS OF MICROSOFT FABRIC", font=lf(22), fill=ACCENT2)
        # Big title — two lines max
        ty = 130
        for ln in wrap_text(draw, title, lf(88, bold=True), W-PAD*2)[:2]:
            draw.text((PAD, ty), ln, font=lf(88, bold=True), fill=OFFWHITE)
            ty += 106
        # Divider
        draw.line([(PAD, ty+10), (W-PAD, ty+10)], fill=ACCENT2, width=2)
        ty += 30
        # Hook line
        hook = slide_content.get("hook", "")
        if hook:
            for ln in wrap_text(draw, hook, lf(36), W-PAD*2)[:2]:
                draw.text((PAD, ty), ln, font=lf(36), fill=MUTED)
                ty += 50
        ty += 20
        # Three key overview pills
        overview = slide_content.get("overview") or []
        max_pills = 3
        pill_w = (W - PAD*2 - 20*(max_pills-1)) // max_pills
        for i, label in enumerate(overview[:max_pills]):
            px = PAD + i*(pill_w+20)
            draw.rounded_rectangle([px, ty, px+pill_w, ty+52], radius=10, fill=BG3, outline=ACCENT2, width=2)
            lbl = wrap_text(draw, label, lf(22), pill_w-20)[:1]
            if lbl:
                bb = draw.textbbox((0,0), lbl[0], font=lf(22))
                draw.text((px+(pill_w-(bb[2]-bb[0]))//2, ty+14), lbl[0], font=lf(22), fill=ACCENT)
        # Category pill
        cat_t = f"  {category.upper()}  "
        cat_bb = draw.textbbox((0, 0), cat_t, font=lf(24))
        cw2 = cat_bb[2]-cat_bb[0]+12; ch2 = cat_bb[3]-cat_bb[1]+14
        draw.rounded_rectangle([PAD, ty+72, PAD+cw2, ty+72+ch2], radius=ch2//2, fill=GOLD)
        draw.text((PAD+6, ty+78), cat_t.strip(), font=lf(24), fill=DARK)
        # Author strip bottom
        avatar_circle(img, draw, PAD+36, H-80, 30)
        draw.text((PAD+82, H-100), "Mani Swaroop Sodadasi", font=lf(26, bold=True), fill=WHITE)
        draw.text((PAD+82, H-68),  "Senior Data & AI Engineer  •  Day %d/100" % day, font=lf(20), fill=MUTED)
        paths.append(save_slide(img, 1))

        # ── SLIDE 2: What Is It ────────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        # Strip "What is / What Is" prefix before adding our own "WHAT IS"
        import re as _re2
        title_clean = _re2.sub(r"(?i)^what\s+is\s+", "", title).strip().rstrip("?")
        section_header(draw, "WHAT IS " + title_clean.upper()[:44])
        # Left accent border for definition
        draw.rectangle([PAD, 158, PAD+6, H-80], fill=GOLD)
        what_is = slide_content.get("what_is", f"{title} is a core component of {category} in Microsoft Fabric.")
        y = 168
        for ln in wrap_text(draw, what_is, lf(38), W-PAD*2-30)[:5]:
            draw.text((PAD+26, y), ln, font=lf(38), fill=OFFWHITE)
            y += 58
        # Category tag
        cat_t = f"  {category.upper()}  "
        cat_bb = draw.textbbox((0, 0), cat_t, font=lf(24))
        cw2 = cat_bb[2]-cat_bb[0]+12
        draw.rounded_rectangle([PAD+26, y+20, PAD+26+cw2, y+56], radius=16, fill=ACCENT2)
        draw.text((PAD+32, y+28), cat_t.strip(), font=lf(24, bold=True), fill=DARK)
        paths.append(save_slide(img, 2))

        # ── SLIDE 3: What You'll Learn ─────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        section_header(draw, "IN THIS VIDEO")
        overview = (slide_content.get("overview") or [])[:4]
        while len(overview) < 4: overview.append("Key Fabric capability")
        numbers = ["01", "02", "03", "04"]
        y = 160
        for num, pt in zip(numbers, overview):
            # Number box
            draw.rounded_rectangle([PAD, y, PAD+72, y+60], radius=10, fill=ACCENT)
            nb = draw.textbbox((0,0), num, font=lf(30, bold=True))
            draw.text((PAD+(72-(nb[2]-nb[0]))//2, y+10), num, font=lf(30, bold=True), fill=DARK)
            # Point text
            lines_pt = wrap_text(draw, pt, lf(36), W-PAD*2-100)[:2]
            for i, ln in enumerate(lines_pt):
                draw.text((PAD+92, y+i*44), ln, font=lf(36), fill=WHITE)
            y += max(70, len(lines_pt)*44+10) + 20
        paths.append(save_slide(img, 3))

        # ── SLIDE 4: How It Works ─────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        hw_heading = slide_content.get("how_works_heading", f"How {title} Works")
        section_header(draw, hw_heading.upper()[:55], color=ACCENT)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=ACCENT, width=3)
        hw_bullets = (slide_content.get("how_works_bullets") or [])[:3]
        while len(hw_bullets) < 3: hw_bullets.append("See full article for details")
        y = 160
        for i, bullet in enumerate(hw_bullets):
            # Step number circle
            cx, cy = PAD+28, y+28
            draw.ellipse([cx-24, cy-24, cx+24, cy+24], fill=ACCENT)
            draw.text((cx-8, cy-14), str(i+1), font=lf(26, bold=True), fill=DARK)
            for ln in wrap_text(draw, bullet, lf(36), W-PAD*2-80)[:2]:
                draw.text((PAD+68, y), ln, font=lf(36), fill=OFFWHITE)
                y += 48
            y += 32
        paths.append(save_slide(img, 4))

        # ── SLIDE 5: Key Feature 1 ────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        f1_heading = slide_content.get("feature1_heading", "Core Feature")
        section_header(draw, f1_heading.upper()[:55], color=ACCENT)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=ACCENT, width=3)
        f1_bullets = (slide_content.get("feature1_bullets") or [])[:3]
        while len(f1_bullets) < 3: f1_bullets.append("See full article")
        content_bullets(draw, f1_bullets, y_start=160, icon="▶", icon_col=ACCENT, font_size=38, line_gap=28)
        paths.append(save_slide(img, 5))

        # ── SLIDE 6: Key Feature 2 ────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        f2_heading = slide_content.get("feature2_heading", "Advanced Feature")
        section_header(draw, f2_heading.upper()[:55], color=GOLD)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=GOLD, width=3)
        f2_bullets = (slide_content.get("feature2_bullets") or [])[:3]
        while len(f2_bullets) < 3: f2_bullets.append("See full article")
        content_bullets(draw, f2_bullets, y_start=160, icon="▶", icon_col=GOLD, font_size=38, line_gap=28)
        paths.append(save_slide(img, 6))

        # ── SLIDE 7: Architecture Diagram ─────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        section_header(draw, "ARCHITECTURE & CONCEPT MAP")
        RL, RR = PAD, W-PAD
        RT, RB = 130, H-70

        if diagram:
            dtype = diagram.get("type", "")
            if dtype == "hub_spoke":
                nodes = diagram.get("nodes", [])
                center_label = diagram.get("center", "Hub")
                N   = max(len(nodes), 1)
                cx  = (RL+RR)//2; cy = (RT+RB)//2
                rad = min((RR-RL),(RB-RT))//2 - 100
                cl  = center_label.split("\n")
                cw3 = max(draw.textbbox((0,0),l,font=lf(28,bold=True))[2] for l in cl)+40
                ch3 = len(cl)*40+24
                draw.rounded_rectangle([cx-cw3//2,cy-ch3//2,cx+cw3//2,cy+ch3//2], radius=14, fill=ACCENT, outline=WHITE, width=3)
                for i, ln in enumerate(cl):
                    bb = draw.textbbox((0,0),ln,font=lf(28,bold=True))
                    draw.text((cx-(bb[2]-bb[0])//2, cy-ch3//2+12+i*40), ln, font=lf(28,bold=True), fill=DARK)
                for i, node in enumerate(nodes):
                    angle = 2*_math.pi*i/N - _math.pi/2
                    nx = max(RL+80,min(RR-80,int(cx+rad*_math.cos(angle))))
                    ny = max(RT+40,min(RB-40,int(cy+rad*_math.sin(angle))))
                    draw.line([(cx,cy),(nx,ny)], fill=ACCENT2, width=3)
                    nl = node.split("\n")
                    nw3=max(draw.textbbox((0,0),l,font=lf(24))[2] for l in nl)+30
                    nh3=len(nl)*34+18
                    draw.ellipse([nx-nw3//2,ny-nh3//2,nx+nw3//2,ny+nh3//2], fill=BG3, outline=ACCENT2, width=2)
                    for j, ln in enumerate(nl):
                        bb=draw.textbbox((0,0),ln,font=lf(24))
                        draw.text((nx-(bb[2]-bb[0])//2,ny-nh3//2+9+j*34), ln, font=lf(24), fill=OFFWHITE)
            elif dtype == "flow":
                steps = diagram.get("steps",[])
                n = len(steps)
                if n:
                    sw = min(280,(RR-RL-40*(n-1))//n); sh=80; cy3=(RT+RB)//2
                    sx = RL+(RR-RL-(sw*n+40*(n-1)))//2
                    for i, step in enumerate(steps):
                        bx = sx+i*(sw+40)
                        draw.rounded_rectangle([bx,cy3-sh//2,bx+sw,cy3+sh//2], radius=12, fill=ACCENT if i==0 else BG3, outline=ACCENT, width=2)
                        st_lines=wrap_text(draw,step,lf(26,bold=True),sw-20)[:2]
                        th2=len(st_lines)*32
                        for j,ln in enumerate(st_lines):
                            bb=draw.textbbox((0,0),ln,font=lf(26,bold=True))
                            draw.text((bx+(sw-(bb[2]-bb[0]))//2,cy3-th2//2+j*32), ln, font=lf(26,bold=True), fill=DARK if i==0 else OFFWHITE)
                        if i<n-1:
                            ax=bx+sw+4; ay=cy3
                            draw.polygon([(ax,ay-12),(ax+24,ay),(ax,ay+12)], fill=GOLD)
            elif dtype == "tiers":
                tiers=diagram.get("tiers",[]); n=len(tiers)
                if n:
                    mbh=RB-RT-80; bw=min(200,(RR-RL-30*(n-1))//n)
                    sx2=RL+(RR-RL-(bw*n+30*(n-1)))//2
                    for i,tier in enumerate(tiers):
                        bh=int(mbh*(i+1)/n); bx=sx2+i*(bw+30); by=RB-bh
                        t2=i/(n-1) if n>1 else 1.0
                        bc=(int(ACCENT2[0]+t2*(ACCENT[0]-ACCENT2[0])),int(ACCENT2[1]+t2*(ACCENT[1]-ACCENT2[1])),int(ACCENT2[2]+t2*(ACCENT[2]-ACCENT2[2])))
                        draw.rectangle([bx,by,bx+bw,RB], fill=bc)
                        nm=tier.get("name",""); dt=tier.get("detail","")
                        nb2=draw.textbbox((0,0),nm,font=lf(26,bold=True))
                        draw.text((bx+(bw-(nb2[2]-nb2[0]))//2,by-38),nm,font=lf(26,bold=True),fill=OFFWHITE)
                        db2=draw.textbbox((0,0),dt,font=lf(20))
                        draw.text((bx+(bw-(db2[2]-db2[0]))//2,by-64),dt,font=lf(20),fill=MUTED)
            elif dtype == "comparison":
                cols=diagram.get("columns",[]); rows=diagram.get("rows",[])
                if cols and rows:
                    nc=len(cols); cw4=(RR-RL)//nc; row_h=min(70,(RB-RT-50)//(len(rows)+1))
                    for ci,col in enumerate(cols):
                        x0=RL+ci*cw4; y0=RT+10
                        draw.rectangle([x0,y0,x0+cw4-2,y0+row_h], fill=ACCENT if ci==0 else ACCENT2)
                        bb=draw.textbbox((0,0),col[:14],font=lf(24,bold=True))
                        draw.text((x0+(cw4-(bb[2]-bb[0]))//2,y0+(row_h-(bb[3]-bb[1]))//2),col[:14],font=lf(24,bold=True),fill=DARK)
                    for ri,row in enumerate(rows[:6]):
                        y0=RT+10+(ri+1)*(row_h+2)
                        for ci,cell in enumerate(row[:nc]):
                            x0=RL+ci*cw4
                            fill=(BG2 if ri%2==0 else BG3) if ci>0 else (14,72,80)
                            draw.rectangle([x0,y0,x0+cw4-2,y0+row_h],fill=fill)
                            ct=str(cell)[:16]
                            bb=draw.textbbox((0,0),ct,font=lf(22))
                            draw.text((x0+10,y0+(row_h-(bb[3]-bb[1]))//2),ct,font=lf(22),fill=OFFWHITE if ci>0 else ACCENT)
        else:
            bullets_d = (slide_content.get("how_works_bullets") or slide_content.get("feature1_bullets") or [])[:5]
            y = 150
            for pt in bullets_d:
                bb2 = draw.textbbox((0, 0), f"  {pt}  ", font=lf(30))
                pw2 = bb2[2]-bb2[0]+16; ph2=bb2[3]-bb2[1]+16
                if y+ph2 > H-80: break
                draw.rounded_rectangle([PAD,y,PAD+pw2,y+ph2], radius=ph2//2, fill=BG3, outline=ACCENT2, width=2)
                draw.text((PAD+8,y+8), f"  {pt}  ".strip(), font=lf(30), fill=OFFWHITE)
                y += ph2 + 18
        paths.append(save_slide(img, 7))

        # ── SLIDE 8: Real-World Use Case ──────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        uc_heading = slide_content.get("usecase_heading", f"Enterprise Use of {title}")
        section_header(draw, uc_heading.upper()[:55], color=GREEN)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=GREEN, width=3)
        uc_bullets = (slide_content.get("usecase_bullets") or [])[:3]
        while len(uc_bullets) < 3: uc_bullets.append("Improved data delivery speed")
        content_bullets(draw, uc_bullets, y_start=160, icon="✓", icon_col=GREEN, font_size=38, line_gap=28)
        paths.append(save_slide(img, 8))

        # ── SLIDE 9: Best Practices ───────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        section_header(draw, "BEST PRACTICES", color=GREEN)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=GREEN, width=3)
        bp = (slide_content.get("best_practices") or [])[:3]
        while len(bp) < 3: bp.append("Follow Fabric documentation")
        y = 160
        for i, pt in enumerate(bp, 1):
            draw.rounded_rectangle([PAD, y, PAD+52, y+52], radius=10, fill=GREEN)
            bb = draw.textbbox((0,0), str(i), font=lf(30,bold=True))
            draw.text((PAD+(52-(bb[2]-bb[0]))//2, y+10), str(i), font=lf(30,bold=True), fill=WHITE)
            for ln in wrap_text(draw, pt, lf(36), W-PAD*2-76)[:2]:
                draw.text((PAD+72, y), ln, font=lf(36), fill=OFFWHITE)
                y += 44
            y += 30
        paths.append(save_slide(img, 9))

        # ── SLIDE 10: Common Mistakes ─────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        section_header(draw, "AVOID THESE MISTAKES", color=RED)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=RED, width=3)
        mistakes = (slide_content.get("mistakes") or [])[:3]
        while len(mistakes) < 2: mistakes.append("Skipping capacity planning")
        content_bullets(draw, mistakes, y_start=160, icon="✗", icon_col=RED, font_size=38, line_gap=28)
        paths.append(save_slide(img, 10))

        # ── SLIDE 11: Key Takeaways ───────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        section_header(draw, "KEY TAKEAWAYS", color=GOLD)
        draw.line([(PAD, 128), (W-PAD, 128)], fill=GOLD, width=3)
        takeaways = (slide_content.get("takeaways") or [])[:3]
        while len(takeaways) < 3: takeaways.append("Read the full article for more")
        y = 168
        for i, pt in enumerate(takeaways):
            # Gold diamond marker
            mx, my = PAD+22, y+26
            draw.polygon([(mx, my-18), (mx+18, my), (mx, my+18), (mx-18, my)], fill=GOLD)
            lines = wrap_text(draw, pt, lf(40), W-PAD*2-70)[:2]
            total_h = len(lines) * 52
            start_y = y + max(0, (56 - total_h) // 2)
            for ln in lines:
                draw.text((PAD+56, start_y), ln, font=lf(40), fill=OFFWHITE)
                start_y += 52
            y += max(80, total_h + 16) + 20
        paths.append(save_slide(img, 11))

        # ── SLIDE 12: CTA ─────────────────────────────────────────────────
        img, draw = make_canvas()
        header_footer(draw, day)
        # Headline
        draw.text((PAD, 74), "Read the Full Article", font=lf(72, bold=True), fill=OFFWHITE)
        draw.line([(PAD, 162), (W-PAD, 162)], fill=ACCENT, width=3)
        # Three value props with drawn check icons
        perks = [
            "Code examples and architecture diagrams included",
            "Step-by-step implementation guide you can follow today",
            "Best-practice checklist for production deployments",
        ]
        y = 188
        for perk in perks:
            # Draw filled green circle with a tick
            cx2, cy2 = PAD+22, y+26
            draw.ellipse([cx2-20, cy2-20, cx2+20, cy2+20], fill=GREEN)
            draw.line([(cx2-9, cy2), (cx2-2, cy2+8)], fill=WHITE, width=3)
            draw.line([(cx2-2, cy2+8), (cx2+10, cy2-6)], fill=WHITE, width=3)
            for ln in wrap_text(draw, perk, lf(38), W-PAD*2-64)[:1]:
                draw.text((PAD+56, y+8), ln, font=lf(38), fill=OFFWHITE)
            y += 76
        # URL pill
        art_url = slide_content.get("article_url", "See link in description")
        draw.rounded_rectangle([PAD, y+20, W-PAD, y+76], radius=14, fill=(10,60,68), outline=ACCENT, width=3)
        url_bb2 = draw.textbbox((0,0), art_url[:85], font=lf(28))
        draw.text((max(PAD+20, (W-(url_bb2[2]-url_bb2[0]))//2), y+36), art_url[:85], font=lf(28), fill=ACCENT)
        y += 100
        # Subscribe call-to-action bar
        draw.rounded_rectangle([PAD, y+14, W-PAD, y+76], radius=16, fill=GOLD)
        sub_t = f"Subscribe for Day {day+1} tomorrow  —  100 Days of Microsoft Fabric"
        sub_bb2 = draw.textbbox((0,0), sub_t, font=lf(30, bold=True))
        draw.text(((W-(sub_bb2[2]-sub_bb2[0]))//2, y+30), sub_t, font=lf(30, bold=True), fill=DARK)
        # Social handles
        draw.text((PAD, H-120), "linkedin.com/in/mani-swaroop-sodadasi-1a165820a", font=lf(24), fill=MUTED)
        draw.text((PAD, H-86),  "#MicrosoftFabric  #DataEngineering  #Azure  #100DaysChallenge", font=lf(24), fill=MUTED)
        paths.append(save_slide(img, 12))

        logger.info("✅ Rendered %d Full-HD slides (1920×1080) in %s", len(paths), tmp_dir)
        return paths

    def _get_video_duration(self, video_path: str) -> float:
        """Return video duration in seconds via ffprobe, or 0.0 on failure."""
        import subprocess
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error",
                 "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1",
                 video_path],
                capture_output=True, text=True, timeout=30,
            )
            return float(r.stdout.strip()) if r.returncode == 0 else 0.0
        except Exception as e:
            logger.warning("ffprobe error: %s", e)
            return 0.0

    def _create_slide_video(
        self,
        slide_paths: List[str],
        total_duration: float,
        output_path: str,
    ) -> bool:
        """
        Build a slide-show MP4 (no audio) using ffmpeg concat demuxer.
        Each slide is shown for (total_duration / n_slides) seconds.
        """
        import subprocess

        n          = len(slide_paths)
        per_slide  = round(total_duration / n, 3)
        concat_txt = output_path + ".concat.txt"

        try:
            with open(concat_txt, "w") as fh:
                for path in slide_paths:
                    fh.write(f"file '{path}'\n")
                    fh.write(f"duration {per_slide}\n")
                fh.write(f"file '{slide_paths[-1]}'\n")  # required trailing entry

            cmd = [
                "ffmpeg", "-y",
                "-f", "concat", "-safe", "0", "-i", concat_txt,
                "-vf", (
                    "scale=1920:1080:force_original_aspect_ratio=decrease,"
                    "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=#08343a,"
                    "format=yuv420p"
                ),
                "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                "-r", "30",
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                logger.error("Slide video ffmpeg error: %s", result.stderr[-600:])
                return False
            logger.info("✅ Slide video created: %s", output_path)
            return True
        except Exception as e:
            logger.error("_create_slide_video error: %s", e)
            return False
        finally:
            try: os.unlink(concat_txt)
            except: pass

    def _composite_presenter_video(
        self,
        slides_path: str,
        avatar_path: str,
        output_path: str,
        avatar_size: int = 500,
    ) -> bool:
        """
        Overlay animated avatar (avatar_size × avatar_size) bottom-right of slide video.
        Adds a 4px ACCENT (#38d4c4) border ring around the avatar.
        Audio comes from the avatar video.
        Slides are 1920×1080 — avatar positioned at (1920-avatar_size-40, 1080-avatar_size-40).
        """
        import subprocess
        margin   = 40
        border   = 4
        total    = avatar_size + border * 2
        ox       = 1920 - total - margin
        oy       = 1080 - total - margin
        filter_g = (
            f"[1:v]scale={avatar_size}:{avatar_size},"
            f"pad={total}:{total}:{border}:{border}:color=#38d4c4[av];"
            f"[0:v][av]overlay={ox}:{oy}[outv]"
        )
        try:
            cmd = [
                "ffmpeg", "-y",
                "-i", slides_path,
                "-i", avatar_path,
                "-filter_complex", filter_g,
                "-map", "[outv]",
                "-map", "1:a",
                "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error("Composite ffmpeg error: %s", result.stderr[-800:])
                return False
            logger.info("✅ Presenter video composited: %s", output_path)
            return True
        except Exception as e:
            logger.error("_composite_presenter_video error: %s", e)
            return False

    def _build_heygen_video(
        self,
        day: int,
        narration: str,
        slide_paths: List[str],
        slide_duration: float,
        tmp_dir: str,
    ) -> str:
        """
        Upload photo → animate with HeyGen → composite with slides.
        Returns path to final MP4 or '' on failure.
        """
        # Find profile photo
        photo_path = ""
        for pp in ["profile_photo.png", "profile_photo.jpg",
                   os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile_photo.png")]:
            if os.path.exists(pp):
                photo_path = pp
                break
        if not photo_path:
            logger.error("HeyGen: profile_photo.png not found")
            return ""

        # 1. Upload photo (animated avatar)
        photo_id = self.heygen_api.upload_photo(photo_path)
        if not photo_id:
            return ""

        # 2. Submit HeyGen render while slides are already built
        video_id = self.heygen_api.create_video(
            narration, photo_id,
            dimension={"width": 512, "height": 512},
        )
        if not video_id:
            return ""

        # 3. Wait for HeyGen
        video_url = self.heygen_api.wait_for_video(video_id)
        if not video_url:
            return ""

        # 4. Download avatar video
        avatar_path = os.path.join(tmp_dir, f"day{day}_heygen.mp4")
        if not self.heygen_api.download_video(video_url, avatar_path):
            return ""

        # 5. Get real duration from avatar video (source of truth)
        duration = self._get_video_duration(avatar_path)
        if duration <= 0:
            logger.error("HeyGen video has invalid duration: %s", duration)
            return ""
        logger.info("HeyGen avatar duration: %.1f s", duration)

        # 6. Build slide video at avatar duration
        slides_vid = os.path.join(tmp_dir, f"day{day}_slides.mp4")
        if not self._create_slide_video(slide_paths, duration, slides_vid):
            return ""

        # 7. Composite
        final_path = os.path.join(tmp_dir, f"day{day}_final.mp4")
        if not self._composite_presenter_video(slides_vid, avatar_path, final_path):
            return ""

        return final_path

    def _build_presenter_video(
        self,
        day: int,
        title: str,
        category: str,
        day_content: Dict,
        article_url: str,
        narration: str,
        tmp_dir: str,
    ) -> str:
        """
        Build full 12-slide presenter video with animated avatar overlay.

        Priority (best quality → free/unlimited):
          1. HeyGen (realistic talking head — limited free tier)
          2. D-ID (talking head fallback)
          3. AnimatedAvatarGenerator (FREE, UNLIMITED — audio-reactive animated photo)

        Slides (12 × 1920×1080) are generated once and reused across all avatar paths.
        """
        # ── Generate slides (shared across all avatar paths) ───────────────
        slide_content = self._generate_slide_content(
            day, title, category, day_content, article_url
        )
        slide_content["article_url"] = article_url

        diagram = day_content.get("diagram") or self._generate_diagram_data(
            day, title, category
        )
        slide_paths = self._render_presentation_slides(
            day, title, category, slide_content, diagram, tmp_dir
        )
        if not slide_paths:
            logger.error("No slides rendered — aborting presenter pipeline")
            return ""

        # ── Generate TTS audio first (needed for animated avatar duration) ─
        audio_path = os.path.join(tmp_dir, f"day{day}_narration.mp3")
        if not self._generate_tts_audio(narration, audio_path):
            logger.error("TTS failed — aborting presenter pipeline")
            return ""
        logger.info("✅ TTS audio ready for avatar sync")

        # ── Path 1: HeyGen (if key set) ────────────────────────────────────
        if self.heygen_api.enabled:
            logger.info("📺 Trying HeyGen animated avatar…")
            result = self._build_heygen_video(day, narration, slide_paths, 0, tmp_dir)
            if result:
                return result
            logger.warning("HeyGen failed — trying D-ID")

        # ── Path 2: D-ID (if key set) ─────────────────────────────────────
        if self.did_api.enabled:
            logger.info("📺 Trying D-ID avatar…")
            try:
                talk_id = self.did_api.create_talk(narration)
                if not talk_id:
                    raise RuntimeError("no talk_id")
                result_url = self.did_api.wait_for_talk(talk_id)
                if not result_url:
                    raise RuntimeError("no result_url")
                did_path = os.path.join(tmp_dir, f"day{day}_did.mp4")
                if not self.did_api.download_result(result_url, did_path):
                    raise RuntimeError("download failed")
                duration = self._get_video_duration(did_path)
                if duration <= 0:
                    raise RuntimeError(f"bad duration {duration}")
                slides_vid = os.path.join(tmp_dir, f"day{day}_slides_did.mp4")
                if not self._create_slide_video(slide_paths, duration, slides_vid):
                    raise RuntimeError("slide video failed")
                final_path = os.path.join(tmp_dir, f"day{day}_final_did.mp4")
                if self._composite_presenter_video(slides_vid, did_path, final_path):
                    return final_path
            except Exception as e:
                logger.warning("D-ID failed: %s — using free animated avatar", e)

        # ── Path 3: FREE animated avatar (always available, no API needed) ─
        logger.info("📺 Generating free animated avatar (audio-reactive, unlimited)…")

        # Find profile photo
        photo_path = ""
        for pp in ["profile_photo.png", "profile_photo.jpg",
                   os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile_photo.png")]:
            if os.path.exists(pp):
                photo_path = pp
                break
        if not photo_path:
            logger.warning("profile_photo.png not found — using name-only avatar")

        avatar_path = os.path.join(tmp_dir, f"day{day}_avatar.mp4")

        if photo_path:
            ok = self.avatar_gen.generate(photo_path, audio_path, avatar_path)
        else:
            # No photo — use gTTS audio + a minimal text avatar (ffmpeg drawtext)
            ok = False

        if ok:
            duration = self._get_video_duration(avatar_path)
            if duration > 0:
                slides_vid = os.path.join(tmp_dir, f"day{day}_slides.mp4")
                if self._create_slide_video(slide_paths, duration, slides_vid):
                    final_path = os.path.join(tmp_dir, f"day{day}_final.mp4")
                    if self._composite_presenter_video(slides_vid, avatar_path, final_path):
                        return final_path

        # Final fallback: slides + audio (no avatar overlay), still much better than static image
        logger.warning("Avatar composite failed — building slides-only video")
        duration = self._get_video_duration(audio_path)
        if duration > 0:
            slides_only = os.path.join(tmp_dir, f"day{day}_slides_only.mp4")
            if self._create_slide_video(slide_paths, duration, slides_only):
                # Add audio track to slides-only video
                final_path = os.path.join(tmp_dir, f"day{day}_slides_audio.mp4")
                import subprocess
                r = subprocess.run([
                    "ffmpeg", "-y",
                    "-i", slides_only, "-i", audio_path,
                    "-map", "0:v", "-map", "1:a",
                    "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
                    "-shortest", final_path,
                ], capture_output=True, timeout=120)
                if r.returncode == 0:
                    return final_path

        return ""

    def generate_and_upload_youtube(
        self,
        day: int,
        day_content: Dict,
        article_url: str,
        post_image: bytes = None,
    ) -> str:
        """
        Generate a YouTube video and upload it.

        If DID_API_KEY is set:
          → Multi-slide presentation (6 branded slides) + D-ID talking-head
            male presenter overlaid bottom-right, narrating the article.

        Fallback (no D-ID key):
          → Original static-image + gTTS voiceover.

        Returns YouTube video ID or '' on failure.
        """
        import tempfile

        title    = day_content.get("title", f"Day {day} Microsoft Fabric")
        category = day_content.get("category", "Data Engineering")

        yt_title = f"Day {day}/100: {title} | Microsoft Fabric 100 Days"
        yt_desc  = (
            f"Day {day} of the Microsoft Fabric 100 Days series.\n\n"
            f"📖 Full article: {article_url}\n\n"
            f"Topic: {title}\nCategory: {category}\n\n"
            f"Follow along as we explore every aspect of Microsoft Fabric — "
            f"from foundational concepts to advanced enterprise patterns, "
            f"one topic per day for 100 days.\n\n"
            f"🔔 Subscribe so you don't miss a day!\n\n"
            f"#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge"
        )
        yt_tags = [
            "MicrosoftFabric", "DataEngineering", "Azure", "Analytics",
            "100DaysChallenge", "Fabric", "DataPlatform", "Tutorial",
            "Microsoft", "CloudData",
            category.replace("_", " ").title(),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            # ── Thumbnail (LinkedIn branded image) ────────────────────────
            img_path = os.path.join(tmp, f"day{day}_thumb.png")
            if post_image:
                with open(img_path, "wb") as fh:
                    fh.write(post_image)
            else:
                img_bytes = self.linkedin_api.generate_post_image(day, title, category)
                if not img_bytes:
                    logger.warning("Day %s: image unavailable — skipping YouTube", day)
                    return ""
                with open(img_path, "wb") as fh:
                    fh.write(img_bytes)

            # ── Narration script ───────────────────────────────────────────
            narration = self._generate_narration_script(
                day, title, category, day_content, article_url
            )
            logger.info("Narration (%d words): %s…", len(narration.split()), narration[:80])

            # ── Presenter video (always attempted — animated avatar is free + unlimited) ─
            final_video_path = ""

            logger.info("📺 Building presenter video (12 slides + animated avatar)…")
            final_video_path = self._build_presenter_video(
                day, title, category, day_content,
                article_url, narration, tmp
            )

            if not final_video_path:
                # Hard fallback: static image + TTS (should rarely happen)
                logger.warning("Presenter pipeline failed — falling back to static image + TTS")
                audio_path = os.path.join(tmp, f"day{day}_narration.mp3")
                video_path = os.path.join(tmp, f"day{day}_video.mp4")
                if not self._generate_tts_audio(narration, audio_path):
                    return ""
                if not self._create_video_from_image_audio(img_path, audio_path, video_path):
                    return ""
                final_video_path = video_path

            # ── Upload to YouTube ──────────────────────────────────────────
            with open(img_path, "rb") as fh:
                thumb_bytes = fh.read()

            return self.youtube_api.upload_video(
                final_video_path, yt_title, yt_desc, yt_tags, thumb_bytes
            )

    def publish_single_day(self, day: int) -> bool:
        """Publish or refresh one day: article file, progress JSON, indices, LinkedIn."""
        current_time = datetime.datetime.now(self.ist_timezone)

        logger.info(
            f"🚀 PUBLISH Day {day} — {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
        )

        if day > 100:
            logger.info("🎉 100 Days series completed!")
            return True

        if day < 1 or day > len(self.content_generator.content_bank):
            logger.error(f"❌ Invalid day {day} or no schedule entry")
            return False

        try:
            day_content = self.content_generator.content_bank[day - 1]

            logger.info(f"📝 Publishing Day {day}: {day_content['title']}")

            slug = slugify_fabric_article(day, day_content["title"])
            article_url = f"{self.website_url}/articles/fabric-100-days/{slug}.html"

            article_content = self.content_generator.generate_article_markdown(
                day,
                day_content,
                article_url,
                self.website_url,
                slug,
            )

            article_html = self.create_article_html(
                day,
                day_content["title"],
                article_content,
                day_content["category"],
                slug,
                published_date=current_time,
            )

            file_path = f"articles/fabric-100-days/{slug}.html"
            github_success = self._put_file(
                file_path,
                article_html,
                f"Add Day {day}: {day_content['title']}",
            )

            if not github_success:
                logger.error(f"❌ Failed to write article file for Day {day}")
                return False

            article_info = {
                "day": day,
                "title": day_content["title"],
                "slug": slug,
                "url": article_url,
                "published_date": current_time.isoformat(),
                "category": day_content["category"],
                "linkedin_post_id": None,
                "youtube_video_id": None,
            }

            self.published_articles = [
                a for a in self.published_articles if a.get("day") != day
            ]
            self.published_articles.append(article_info)
            self.published_articles.sort(key=lambda x: x["day"])
            self._save_published_articles()

            index_success = self.update_series_index()
            hub_success = self.update_articles_hub_page()
            self.update_portfolio_page()

            linkedin_text = self.resolve_linkedin_post_text(
                day, day_content, article_url
            )

            # Generate branded infographic image for the post
            post_image: bytes | None = None
            try:
                concepts = [
                    h.replace("_", " ").replace("-", " ")
                    for h in (day_content.get("hashtags") or [])
                    if h not in ("MicrosoftFabric", "100DaysChallenge", "Azure", "Analytics")
                ]
                # Use diagram from schedule if defined, otherwise generate with AI
                diagram = day_content.get("diagram")
                use_ai = os.getenv("FABRIC_USE_AI", "").lower() in ("1", "true", "yes")
                if not diagram and use_ai:
                    logger.info("Day %s: generating diagram data via AI", day)
                    diagram = self._generate_diagram_data(
                        day,
                        day_content["title"],
                        day_content.get("category", "Data Engineering"),
                    )
                post_image = self.linkedin_api.generate_post_image(
                    day, day_content["title"],
                    day_content.get("category", "Data Engineering"),
                    concepts,
                    diagram=diagram,
                )
                if post_image:
                    logger.info("🖼️  Post image generated (%d bytes)", len(post_image))
            except Exception as img_err:
                logger.warning("Image generation error: %s — posting without image", img_err)

            if self._local_only:
                Path("last_linkedin_post.txt").write_text(linkedin_text, encoding="utf-8")
                if post_image:
                    Path("last_linkedin_post.png").write_bytes(post_image)
                    logger.info("Image saved to last_linkedin_post.png")
                logger.info(
                    "LinkedIn copy saved to last_linkedin_post.txt (paste manually or run without --local-only)"
                )
                linkedin_result = {"success": True}
            else:
                linkedin_result = self.linkedin_api.post_to_linkedin(linkedin_text, post_image)

            if linkedin_result["success"] and not self._local_only:
                linkedin_post_id = linkedin_result.get("post_id", "")
                logger.info(
                    f"✅ LinkedIn post successful - Post ID: {linkedin_post_id or 'Unknown'}"
                )
                # Save the ugcPost URN (x-restli-id header) so future edits can use the API
                if linkedin_post_id:
                    for a in self.published_articles:
                        if a.get("day") == day:
                            a["linkedin_post_id"] = linkedin_post_id
                            break
                    self._save_published_articles()
            elif not linkedin_result["success"]:
                logger.error(
                    f"❌ LinkedIn posting failed: {linkedin_result.get('error', 'Unknown error')}"
                )

            logger.info(f"📄 Article: {article_url}")
            logger.info(
                f"📱 LinkedIn: {'✅ Posted' if linkedin_result['success'] else '❌ Failed'}"
            )
            logger.info(
                f"🌐 Series index: {'✅' if index_success else '❌'} | Hub: {'✅' if hub_success else '❌'}"
            )
            logger.info(f"📊 Day {day}/100")

            # ── YouTube video ─────────────────────────────────────────────
            youtube_video_id = ""
            try:
                if self.youtube_api.enabled and not self._local_only:
                    logger.info("📺 Generating YouTube video for Day %s…", day)
                    youtube_video_id = self.generate_and_upload_youtube(
                        day, day_content, article_url, post_image
                    )
                    if youtube_video_id:
                        for a in self.published_articles:
                            if a.get("day") == day:
                                a["youtube_video_id"] = youtube_video_id
                                break
                        self._save_published_articles()
                        logger.info("📺 YouTube: https://youtu.be/%s", youtube_video_id)
                    else:
                        logger.warning("📺 YouTube upload returned no video ID")
                elif self._local_only and self.youtube_api.enabled:
                    logger.info("📺 YouTube: skipped in local-only mode")
            except Exception as yt_err:
                logger.warning("📺 YouTube posting error (non-fatal): %s", yt_err)

            linkedin_ok = linkedin_result["success"]
            if not linkedin_ok and os.getenv("LINKEDIN_OPTIONAL", "").lower() in (
                "1",
                "true",
                "yes",
            ):
                logger.warning(
                    "LINKEDIN_OPTIONAL set — continuing despite LinkedIn failure (article is live)."
                )
                linkedin_ok = True

            ok = github_success and index_success and hub_success and linkedin_ok
            if ok:
                logger.info(f"✅ Day {day} publish completed successfully")
            else:
                logger.error(f"❌ Day {day} publish finished with errors (see logs above)")
            return ok

        except Exception as e:
            logger.error(f"❌ Publish failed for Day {day}: {e}")
            raise

    def daily_automation_task(self) -> bool:
        """Publish the next sequential day (for cron / scheduler)."""
        current_time = datetime.datetime.now(self.ist_timezone)
        today = current_time.date()

        # Guard: skip if we already published something today (prevents double-runs)
        if self.published_articles:
            last = max(self.published_articles, key=lambda a: a["day"])
            try:
                last_date = datetime.datetime.fromisoformat(
                    last["published_date"].replace("Z", "+00:00")
                ).astimezone(self.ist_timezone).date()
            except Exception:
                last_date = None
            if last_date == today:
                logger.info(
                    f"⏭️  Day {last['day']} was already published today ({today} IST) — skipping duplicate run."
                )
                return True

        day = len(self.published_articles) + 1

        logger.info(
            f"🚀 DAILY AUTOMATION — next Day {day} — {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
        )

        if day > 100:
            logger.info("🎉 100 Days series completed!")
            return True

        if day > len(self.content_generator.content_bank):
            logger.error(f"❌ No content available for Day {day}")
            return False

        return self.publish_single_day(day)
    
    def start_automation(self):
        """Start the full automation system"""
        
        logger.info("🚀 MICROSOFT FABRIC 100 DAYS - FULL AUTOMATION SYSTEM")
        logger.info("=" * 60)
        
        # Test API connections
        api_status = self.test_apis()
        
        if not api_status['github']:
            logger.error("❌ GitHub API connection failed")
            return
        
        if not api_status["linkedin"]:
            if os.getenv("LINKEDIN_STRICT_PREFLIGHT", "").lower() in (
                "1",
                "true",
                "yes",
            ):
                logger.error("❌ LinkedIn preflight failed (LINKEDIN_STRICT_PREFLIGHT=true)")
                return
            logger.warning(
                "LinkedIn preflight failed — scheduler will still try to post "
                "(w_member_social-only tokens). Set LINKEDIN_STRICT_PREFLIGHT=true to stop here."
            )

        logger.info("✅ API check done (GitHub OK; LinkedIn post will be tried on each run)")
        logger.info(f"📊 Current progress: {len(self.published_articles)}/100 articles")
        logger.info("⏰ Scheduled to run daily at 9:00 AM IST")
        
        # Schedule daily task
        schedule.every().day.at("09:00").do(self.daily_automation_task)
        
        # Run immediately if no articles published (for testing)
        if len(self.published_articles) == 0:
            logger.info("🚀 Running first automation task now...")
            try:
                self.daily_automation_task()
            except Exception as e:
                logger.error(f"❌ First automation task failed: {e}")
        
        logger.info("⏰ Automation system is running...")
        logger.info("📱 LinkedIn posts will be published automatically")
        logger.info("🌐 Website will be updated automatically")
        logger.info("🛑 Press Ctrl+C to stop")
        
        try:
            while len(self.published_articles) < 100:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            
            logger.info("🎉 100 Days series completed! Automation stopped.")
            
        except KeyboardInterrupt:
            logger.info("⏹️  Automation system stopped by user")
        except Exception as e:
            logger.error(f"❌ Automation system error: {e}")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Microsoft Fabric 100 Days — website + LinkedIn automation",
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run a single daily publish cycle and exit (for GitHub Actions cron)',
    )
    parser.add_argument(
        "--only-day",
        type=int,
        metavar="N",
        help="Publish or refresh a specific day (1–100), e.g. --once --only-day 1",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Write article + indices + published_articles.json under repo root only (no GitHub/LinkedIn API); saves LinkedIn text to last_linkedin_post.txt",
    )
    args = parser.parse_args()

    if args.local_only:
        os.environ["FABRIC_LOCAL_ONLY"] = "1"

    if not args.once:
        print("🚀 MICROSOFT FABRIC 100 DAYS - FULL AUTOMATION SYSTEM")
        print("=" * 60)
        print()
        print("🎯 This system will automatically:")
        print("  ✅ Publish the next day from enhanced_fabric_schedule.json (predefined copy)")
        print("  ✅ Update your website + post the schedule's LinkedIn text (links fixed to the live URL)")
        print("  ✅ Update series index, articles hub, and progress tracking")
        print("  ✅ Run daily at 9 AM IST (or use --once in CI)")
        print()
        print("🔑 Required environment variables:")
        print("  • GITHUB_TOKEN — repo write")
        print("  • LINKEDIN_ACCESS_TOKEN, LINKEDIN_PERSON_ID — posting")
        print()
        print("Optional:")
        print("  • FABRIC_USE_AI=true + ANTHROPIC_API_KEY — AI-written articles instead of schedule")
        print("  • Per-day article_markdown in JSON — full long-form page body")
        print()

    # Check required files
    if not Path('enhanced_fabric_schedule.json').exists():
        print("❌ Content bank missing: enhanced_fabric_schedule.json")
        print("   Run enhanced_fabric_system.py first to generate content.")
        sys.exit(1)

    try:
        automation_system = FullAutomationSystem()

        if args.once:
            logger.info("Running single publish cycle (--once, e.g. GitHub Actions)")
            api_status = automation_system.test_apis()
            if not api_status.get("github"):
                logger.error("GitHub API connection failed")
                sys.exit(1)
            # In CI, LinkedIn "read" APIs (/me, profile) often fail with w_member_social-only tokens.
            # Preflight is skipped; the real check is post_to_linkedin below.
            in_ci = os.getenv("GITHUB_ACTIONS") == "true"
            if not automation_system._local_only and not api_status.get("linkedin"):
                if in_ci:
                    logger.warning(
                        "Skipping LinkedIn preflight in GitHub Actions (read scopes often unavailable). "
                        "UGC post will still be attempted."
                    )
                elif os.getenv("LINKEDIN_SKIP_CONNECTION_TEST", "").lower() in (
                    "1",
                    "true",
                    "yes",
                ):
                    logger.warning(
                        "LINKEDIN_SKIP_CONNECTION_TEST set — continuing without LinkedIn preflight"
                    )
                elif os.getenv("LINKEDIN_STRICT_PREFLIGHT", "").lower() in (
                    "1",
                    "true",
                    "yes",
                ):
                    logger.error(
                        "LinkedIn profile preflight failed (LINKEDIN_STRICT_PREFLIGHT=true). "
                        "Unset it to allow UGC posting with w_member_social-only tokens, "
                        "or set LINKEDIN_SKIP_CONNECTION_TEST=true."
                    )
                    sys.exit(1)
                else:
                    logger.warning(
                        "LinkedIn profile preflight failed (403 is common for tokens with only "
                        "w_member_social — they cannot call /me). UGC post will still be attempted."
                    )
            if args.only_day is not None:
                d = args.only_day
                if d < 1 or d > 100:
                    logger.error("--only-day must be between 1 and 100")
                    sys.exit(1)
                ok = automation_system.publish_single_day(d)
            else:
                ok = automation_system.daily_automation_task()
            if ok is False:
                logger.error(
                    "Publish cycle reported failure (GitHub upload, index, or LinkedIn post)."
                )
                sys.exit(1)
            logger.info("Single publish cycle finished successfully")
            return

        automation_system.start_automation()

    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("   Please set the required environment variables.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ System Error: {e}")
        logger.error(f"System startup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

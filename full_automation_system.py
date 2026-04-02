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

    def _post_via_rest_posts(self, author_urn: str, content: str) -> Dict:
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

    def _post_via_ugc_posts(self, author_urn: str, content: str) -> Dict:
        """Legacy POST /v2/ugcPosts (Share on LinkedIn consumer doc)."""
        payload = {
            "author": author_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": {
                    "shareCommentary": {"text": content},
                    "shareMediaCategory": "NONE",
                }
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

    def post_to_linkedin(self, content: str) -> Dict:
        """Post to LinkedIn: try REST Posts API first, then legacy ugcPosts."""

        try:
            try:
                author_urn = self._author_urn_for_ugc()
            except ValueError as ve:
                logger.error("%s", ve)
                return {"success": False, "error": str(ve)}

            logger.info("LinkedIn UGC author URN: %s", author_urn)

            use_rest_first = os.getenv("LINKEDIN_USE_REST_POSTS", "true").lower() in (
                "1",
                "true",
                "yes",
            )
            if use_rest_first:
                rest = self._post_via_rest_posts(author_urn, content)
                if rest.get("success"):
                    return rest
                logger.warning(
                    "REST Posts API failed (%s). Trying legacy /v2/ugcPosts…",
                    (rest.get("error") or "")[:400],
                )

            ugc = self._post_via_ugc_posts(author_urn, content)
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
    
    def _generate_with_api(self, day: int, title: str, category: str) -> str:
        """Generate article using Gemini (free) or Anthropic API."""

        try:
            prompt = f"""
            Write a comprehensive, practical 2500-word article about "{title}" for Day {day} of a Microsoft Fabric 100 Days series.

            Category: {category}
            Target audience: Data engineers, analysts, and BI professionals

            Structure:
            1. Introduction with real-world business problem
            2. Core concepts explained clearly
            3. Technical deep-dive with specific implementation steps
            4. Best practices and common pitfalls to avoid
            5. Real-world use case or scenario
            6. Next steps and related topics

            Code block rules (IMPORTANT):
            - Only include code if it directly demonstrates something specific to THIS topic
            - For conceptual/overview topics (e.g. "What is X", comparisons, pricing, architecture overviews): NO code blocks
            - For hands-on topics (pipelines, notebooks, SQL, DAX, APIs, transformations): include 1-2 focused, realistic examples
            - Never include placeholder or dummy code that doesn't teach anything real
            - Prefer diagrams-in-text, tables, and bullet points over filler code

            Requirements:
            - Provide step-by-step explanations where applicable
            - Share expert insights and pro tips
            - Reference official Microsoft documentation
            - Make it highly actionable for practitioners
            - Write from experience, not just theory

            Format as markdown with proper headers, tables, and lists.
            """

            # Gemini (free tier) takes priority if key is set; fall back to Anthropic
            if self.gemini_api_key:
                return self._generate_with_gemini(day, title, category, prompt)
            return self._generate_with_anthropic(day, title, category, prompt)

        except Exception as e:
            logger.warning(f"AI generation error: {e}, using template for Day {day}")
            return self._generate_template_article(day, title, category)

    def _generate_with_gemini(self, day: int, title: str, category: str, prompt: str) -> str:
        """Generate article using Google Gemini (free tier)."""
        model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent?key={self.gemini_api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": 8192, "temperature": 0.7},
        }
        try:
            r = requests.post(url, json=payload, timeout=120)
            if r.status_code == 200:
                text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                logger.info("✅ Generated article via Gemini for Day %s", day)
                return text
            logger.warning("Gemini failed (%s): %s — trying template", r.status_code, r.text[:200])
            return self._generate_template_article(day, title, category)
        except Exception as e:
            logger.warning("Gemini error: %s — using template", e)
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
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=120)
            if r.status_code == 200:
                text = r.json()["content"][0]["text"]
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
        self, day: int, title: str, content: str, category: str, slug: str
    ) -> str:
        """Generate complete HTML for article page."""

        # Convert markdown to basic HTML
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
                    <span>{datetime.datetime.now(self.ist_timezone).strftime('%B %d, %Y')}</span>
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
        """Convert markdown content to HTML"""
        import re
        
        # Headers
        content = re.sub(r'^# (.+)$', r'<h1>\1</h1>', content, flags=re.MULTILINE)
        content = re.sub(r'^## (.+)$', r'<h2>\1</h2>', content, flags=re.MULTILINE)
        content = re.sub(r'^### (.+)$', r'<h3>\1</h3>', content, flags=re.MULTILINE)
        
        # Code blocks with language detection
        def replace_code_block(match):
            lang = match.group(1) if match.group(1) else ''
            code = match.group(2)
            return f'<pre><code class="language-{lang}">{code}</code></pre>'
        
        content = re.sub(r'```(\w+)?\n(.*?)\n```', replace_code_block, content, flags=re.DOTALL)
        
        # Inline code
        content = re.sub(r'`([^`]+)`', r'<code>\1</code>', content)
        
        # Bold and italic
        content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content)
        content = re.sub(r'\*(.+?)\*', r'<em>\1</em>', content)
        
        # Links
        content = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', content)
        
        # Lists
        content = re.sub(r'^\* (.+)$', r'<li>\1</li>', content, flags=re.MULTILINE)
        content = re.sub(r'(<li>.*</li>)', r'<ul>\1</ul>', content, flags=re.DOTALL)
        
        # Fix nested list tags
        content = re.sub(r'</ul>\s*<ul>', '', content)
        
        # Blockquotes
        content = re.sub(r'^> (.+)$', r'<blockquote>\1</blockquote>', content, flags=re.MULTILINE)
        
        # Paragraphs - split by double newlines and wrap non-tag content
        paragraphs = content.split('\n\n')
        html_paragraphs = []
        
        for para in paragraphs:
            para = para.strip()
            if para and not para.startswith('<'):
                # Replace single newlines with spaces in paragraphs
                para = para.replace('\n', ' ')
                html_paragraphs.append(f'<p>{para}</p>')
            elif para:
                html_paragraphs.append(para)
        
        return '\n\n'.join(html_paragraphs)

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
        # Extract the main content before the read more link
        main_content = raw_li.split("📖")[0].strip() if raw_li else ""
        if not main_content:
            main_content = (
                f"Day {day}/100 of Microsoft Fabric: exploring {title} with practical notes and examples."
            )
        
        cat = str(day_content.get("category", "foundations")).replace("_", "").title()
        linkedin_post = f"""🧵 Microsoft Fabric - Day {day}/100

{main_content}

💡 Pro Tip: {pro_tip}

📖 Read the complete guide with step-by-step examples and best practices:
{article_url}

---
#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge #{cat}

👉 What's your experience with {title.lower()}? Share your thoughts below!"""

        return linkedin_post

    def resolve_linkedin_post_text(
        self, day: int, day_content: Dict, article_url: str
    ) -> str:
        """Prefer predefined `linkedin_content` from the schedule; normalize links; else template."""
        raw = (day_content.get("linkedin_content") or "").strip()
        if raw:
            return normalize_fabric_article_urls(raw, article_url)
        logger.info("No linkedin_content in schedule — using generated LinkedIn template")
        return self.create_linkedin_post(day, day_content, article_url)

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
            }

            self.published_articles = [
                a for a in self.published_articles if a.get("day") != day
            ]
            self.published_articles.append(article_info)
            self.published_articles.sort(key=lambda x: x["day"])
            self._save_published_articles()

            index_success = self.update_series_index()
            hub_success = self.update_articles_hub_page()

            linkedin_text = self.resolve_linkedin_post_text(
                day, day_content, article_url
            )
            if self._local_only:
                Path("last_linkedin_post.txt").write_text(
                    linkedin_text, encoding="utf-8"
                )
                logger.info(
                    "LinkedIn copy saved to last_linkedin_post.txt (paste manually or run without --local-only)"
                )
                linkedin_result = {"success": True}
            else:
                linkedin_result = self.linkedin_api.post_to_linkedin(linkedin_text)

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

# Microsoft Fabric 100 Days - Full Automation Setup Guide

## 🎯 Complete Automation Features

✅ **Daily article generation** using Claude API (2500+ words each)
✅ **Professional website pages** with SEO optimization  
✅ **Automatic GitHub commits** and website updates
✅ **LinkedIn posting** with engagement optimization
✅ **Series progress tracking** and analytics
✅ **Error handling** and comprehensive logging

---

## Fully automated 100-day pipeline (GitHub Actions + LinkedIn)

The repo is wired for **hands-off** operation:

| Step | What runs |
|------|-----------|
| **Schedule** | `.github/workflows/fabric-100-days.yml` runs daily at **09:00 IST** (`cron: 30 3 * * *` UTC). |
| **Script** | `python full_automation_system.py --once` — one publish cycle then exit (safe for CI). |
| **Content** | Next day is derived from `published_articles.json` + `enhanced_fabric_schedule.json` (100 topics). |
| **Predefined copy** | **Default:** Website markdown is built from `linkedin_content` + `article.meta_description` in `enhanced_fabric_schedule.json` (no API). Optional per-day field **`article_markdown`** (or `body_markdown`) is used as the full page if set — placeholders: `{day}`, `{title}`, `{slug}`, `{article_url}`, `{website_url}`, `{category}`. |
| **Anthropic (optional)** | Set **`FABRIC_USE_AI=true`** (repo variable or env) **and** `ANTHROPIC_API_KEY` to generate long articles with Claude instead of the predefined path. |
| **Site** | New HTML under `articles/fabric-100-days/`, **`articles/fabric-100-days/index.html`**, and **`articles/index.html`** (between `<!-- FABRIC_HUB_AUTO_BEGIN -->` … `END` markers). |
| **Tracking** | `published_articles.json` is updated and pushed via the GitHub API on each run (required so the next day is correct). |
| **LinkedIn** | `post_to_linkedin` uses the UGC Posts API. If posting fails but you still want the workflow green, set repo variable **`LINKEDIN_OPTIONAL=true`**. |

**Manual run:** Actions → “Fabric 100 Days — daily publish” → **Run workflow**.

**Publish a specific day from your laptop (e.g. Day 1):**

```bash
# With GitHub + LinkedIn tokens in the environment:
python full_automation_system.py --once --only-day 1

# Without API keys — writes HTML, indices, published_articles.json locally; LinkedIn text → last_linkedin_post.txt
python full_automation_system.py --once --only-day 1 --local-only
```

Then `git add`, `commit`, and `push` (or use the first command so the script pushes files via the GitHub API).

---

## 🔑 Step 1: Environment Variables

You need these variables (local) or **GitHub Actions secrets** (cloud):

### Required Variables:
```bash
GITHUB_TOKEN=your_github_personal_access_token
LINKEDIN_ACCESS_TOKEN=your_linkedin_access_token
LINKEDIN_PERSON_ID=your_linkedin_person_id
ANTHROPIC_API_KEY=your_claude_api_key  # Strongly recommended for real articles (not templates)
```

### How to Set Environment Variables:

**Windows (Command Prompt):**
```cmd
set GITHUB_TOKEN=ghp_your_token_here
set LINKEDIN_ACCESS_TOKEN=your_linkedin_token
set LINKEDIN_PERSON_ID=your_person_id
set ANTHROPIC_API_KEY=your_claude_api_key
```

**Windows (PowerShell):**
```powershell
$env:GITHUB_TOKEN="ghp_your_token_here"
$env:LINKEDIN_ACCESS_TOKEN="your_linkedin_token"
$env:LINKEDIN_PERSON_ID="your_person_id"
$env:ANTHROPIC_API_KEY="your_claude_api_key"
```

**Mac/Linux:**
```bash
export GITHUB_TOKEN=ghp_your_token_here
export LINKEDIN_ACCESS_TOKEN=your_linkedin_token
export LINKEDIN_PERSON_ID=your_person_id
export ANTHROPIC_API_KEY=your_claude_api_key
```

---

## 🔑 Step 2: Get Your LinkedIn Person ID

Since you have LinkedIn API access, you can get your Person ID:

### Option A: API Call
```bash
curl -H "Authorization: Bearer YOUR_LINKEDIN_TOKEN" \
     "https://api.linkedin.com/v2/people/(id:me)"
```

Look for the `id` field in the response. It will look like: `ABC123XYZ`

### Option B: From LinkedIn URL
If your LinkedIn profile URL is: `https://linkedin.com/in/yourname/`
Your Person ID is typically the encoded version of your profile identifier.

---

## 🔑 Step 3: Verify Your Tokens

### Test GitHub Token:
```bash
curl -H "Authorization: token YOUR_GITHUB_TOKEN" \
     https://api.github.com/user
```

### Test LinkedIn Token:
```bash
curl -H "Authorization: Bearer YOUR_LINKEDIN_TOKEN" \
     "https://api.linkedin.com/v2/people/(id:me)"
```

### Test Claude API (Optional):
```bash
curl -X POST https://api.anthropic.com/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_ANTHROPIC_KEY" \
  -d '{"model":"claude-3-sonnet-20240229","max_tokens":100,"messages":[{"role":"user","content":"Hello"}]}'
```

---

## 📦 Step 4: Install Dependencies

```bash
pip install requests schedule pytz
```

---

## 🚀 Step 5: Run the Full Automation System

1. **Make sure you have the content bank:**
   - `enhanced_fabric_schedule.json` should be in your directory
   - If not, run `enhanced_fabric_system.py` first

2. **Start the automation:**
   ```bash
   python full_automation_system.py
   ```

3. **The system will:**
   - ✅ Validate all your API tokens
   - ✅ Run initial automation immediately (Day 1)
   - ✅ Schedule daily automation at 9:00 AM IST
   - ✅ Generate 2500+ word technical articles
   - ✅ Create professional website pages
   - ✅ Commit to your GitHub repository
   - ✅ Post to LinkedIn with article links
   - ✅ Continue for 100 days automatically

---

## ⚡ What Happens Daily (9:00 AM IST)

### Automatic Process:
1. **9:00 AM IST** - System activates
2. **Generate Article** - Creates 2500+ word technical article using Claude API
3. **Create Webpage** - Builds professional HTML page with SEO optimization
4. **GitHub Commit** - Pushes to your repository automatically
5. **LinkedIn Post** - Posts with engagement-optimized content + article link
6. **Update Index** - Updates series index page
7. **Log Progress** - Tracks analytics and progress

### Example Daily Output:

**Website Article:** 
- URL: `https://yourname.github.io/articles/fabric-100-days/day-1-what-is-microsoft-fabric.html`
- 2500+ words with code examples, best practices, implementation guides
- Professional design with SEO optimization

**LinkedIn Post:**
```
🧵 Microsoft Fabric - Day 1/100

🤔 What if you could unify data engineering, analytics, and AI in ONE platform?

Microsoft Fabric combines data integration, analytics, AI, and BI in one unified platform.

💡 Pro Tip: Think of it as "Azure Synapse + Power BI + Data Factory" built from the ground up.

📖 Complete guide with implementation examples: 
https://yourname.github.io/articles/fabric-100-days/day-1-what-is-microsoft-fabric.html

#MicrosoftFabric #DataEngineering #100DaysChallenge #Analytics

👉 What's your experience with Microsoft Fabric? Share your thoughts below!
```

---

## 📊 System Monitoring

### Log Files:
- **fabric_automation.log** - Detailed system logs
- **published_articles.json** - Progress tracking

### Progress Tracking:
- Series index page shows completion percentage
- Daily logs show success/failure status
- Article analytics and engagement tracking

---

## 🛑 Stopping/Restarting

**To Stop:**
- Press `Ctrl+C` in the terminal

**To Restart:**
- Run `python full_automation_system.py` again
- System will resume from where it left off
- Never duplicates or skips days

---

## 🎯 Expected Results

### After 100 Days:
✅ **100 comprehensive articles** (250,000+ words total)
✅ **Complete learning series** on Microsoft Fabric
✅ **SEO-optimized website** with full article collection
✅ **100 LinkedIn posts** with high engagement potential
✅ **Thought leadership** position in Microsoft Fabric space
✅ **Increased website traffic** from article backlinks
✅ **Professional networking** opportunities from LinkedIn visibility

### Immediate Benefits:
✅ **Hands-off automation** - runs for 100 days without intervention
✅ **Professional content** - enterprise-grade articles and website
✅ **Consistent publishing** - daily content at optimal time (9 AM IST)
✅ **Brand building** - positions you as Microsoft Fabric expert
✅ **Lead generation** - drives traffic to your portfolio website

---

## 🔧 Troubleshooting

### Common Issues:

**"Missing environment variables"**
- Double-check variable names match exactly
- Restart terminal after setting variables
- Use `echo $VARIABLE_NAME` (Mac/Linux) or `echo %VARIABLE_NAME%` (Windows) to verify

**"GitHub API error"**
- Verify token has repository write permissions
- Check repository name format: `username/repository-name`
- Ensure repository exists and is accessible

**"LinkedIn API error"**
- Verify token hasn't expired (LinkedIn tokens typically last 60 days)
- Check Person ID format (should be alphanumeric)
- Ensure you have posting permissions

**"Content bank missing"**
- Run `enhanced_fabric_system.py` first to generate content
- Verify `enhanced_fabric_schedule.json` exists in current directory

---

## 🏆 Success Tips

1. **Test tokens individually** before running full automation
2. **Start on Day 1** for maximum impact and consistency  
3. **Monitor first few days** to ensure everything works smoothly
4. **Engage with comments** on LinkedIn posts to boost visibility
5. **Share progress** on other social platforms with links back to articles
6. **Track analytics** using Google Analytics on your website
7. **Repurpose content** - turn articles into YouTube videos, podcasts, etc.

---

## 🎉 You're Ready!

Once your environment variables are set and you run `python full_automation_system.py`, you'll have:

✅ **100% automated content generation and publishing system**
✅ **Professional Microsoft Fabric learning series**
✅ **Daily LinkedIn thought leadership**  
✅ **SEO-optimized website traffic**
✅ **Established expertise in Microsoft Fabric**

**The system runs itself for 100 days - making you the go-to Microsoft Fabric expert!** 🚀

---

**Questions? Run into issues? The system includes comprehensive error logging and recovery mechanisms.**

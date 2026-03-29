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
import os
import json
import requests
import schedule
import sys
import time
import datetime
import pytz
from typing import Dict, List
import base64
from pathlib import Path
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
    
    def __init__(self, access_token: str, person_id: str):
        self.access_token = access_token
        self.person_id = person_id
        self.base_url = "https://api.linkedin.com/v2"
        
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0"
        }
    
    def post_to_linkedin(self, content: str) -> Dict:
        """Post content to LinkedIn"""
        
        try:
            payload = {
                "author": f"urn:li:person:{self.person_id}",
                "lifecycleState": "PUBLISHED",
                "specificContent": {
                    "com.linkedin.ugc.ShareContent": {
                        "shareCommentary": {
                            "text": content
                        },
                        "shareMediaCategory": "NONE"
                    }
                },
                "visibility": {
                    "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
                }
            }
            
            response = requests.post(
                f"{self.base_url}/ugcPosts",
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 201:
                post_data = response.json()
                post_id = post_data.get("id", "")
                
                return {
                    "success": True,
                    "post_id": post_id,
                    "message": "Posted successfully to LinkedIn"
                }
            else:
                logger.error(f"LinkedIn API Error: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"API Error {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            logger.error(f"LinkedIn posting error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def test_connection(self) -> bool:
        """Test LinkedIn API connection"""
        try:
            response = requests.get(
                f"{self.base_url}/people/{self.person_id}",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except:
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
            
            response = requests.put(
                f"{self.base_url}/contents/{file_path}",
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
            response = requests.get(
                f"{self.base_url}/contents/{file_path}",
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
    
    def _load_content_bank(self) -> List[Dict]:
        """Load the 100-day content bank"""
        try:
            with open('enhanced_fabric_schedule.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error("Content bank not found. Run enhanced_fabric_system.py first.")
            return []
    
    def generate_detailed_article(self, day: int, title: str, category: str) -> str:
        """Generate detailed article content"""
        
        if self.anthropic_api_key:
            return self._generate_with_api(day, title, category)
        else:
            return self._generate_template_article(day, title, category)
    
    def _generate_with_api(self, day: int, title: str, category: str) -> str:
        """Generate article using Anthropic API"""
        
        try:
            prompt = f"""
            Write a comprehensive, practical 2500-word article about "{title}" for Day {day} of a Microsoft Fabric 100 Days series.
            
            Category: {category}
            Target audience: Data engineers, analysts, and BI professionals
            
            Structure:
            1. Introduction with real-world business problem
            2. Technical deep-dive with specific implementation steps
            3. Code examples (Python, SQL, DAX as appropriate)
            4. Architecture diagrams descriptions
            5. Best practices and common pitfalls to avoid
            6. Real-world use case with detailed implementation
            7. Performance optimization tips
            8. Next steps and related topics
            
            Requirements:
            - Include practical Python/SQL/PowerShell code examples
            - Provide step-by-step tutorials
            - Share expert insights and pro tips
            - Reference official Microsoft documentation
            - Make it highly actionable for practitioners
            - Write from experience, not just theory
            
            Format as markdown with proper headers, code blocks, and lists.
            """
            
            headers = {
                'Content-Type': 'application/json',
                'x-api-key': self.anthropic_api_key
            }
            
            payload = {
                'model': 'claude-3-sonnet-20240229',
                'max_tokens': 4000,
                'messages': [
                    {'role': 'user', 'content': prompt}
                ]
            }
            
            response = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers=headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['content'][0]['text']
                logger.info(f"✅ Generated article content using AI for Day {day}")
                return content
            else:
                logger.warning(f"AI generation failed, using template for Day {day}")
                return self._generate_template_article(day, title, category)
                
        except Exception as e:
            logger.warning(f"AI generation error: {e}, using template for Day {day}")
            return self._generate_template_article(day, title, category)
    
    def _generate_template_article(self, day: int, title: str, category: str) -> str:
        """Generate template-based article content"""
        
        return f"""# {title}

## Introduction

{title} is a fundamental concept in Microsoft Fabric that enables organizations to build robust, scalable data solutions. As we explore Day {day} of our comprehensive 100-day journey, we'll dive deep into the practical aspects that make this topic essential for modern data professionals.

## Why {title} Matters

In today's data-driven landscape, understanding {title.lower()} is crucial for:

- **Scalability**: Building solutions that grow with your organization
- **Efficiency**: Optimizing performance and reducing costs
- **Integration**: Seamlessly connecting with existing data infrastructure
- **Innovation**: Enabling advanced analytics and AI capabilities

## Technical Deep Dive

### Core Concepts

Understanding {title.lower()} requires mastering several key principles:

**1. Architecture Foundation**
The underlying structure that makes {title.lower()} effective in Microsoft Fabric environments.

**2. Implementation Patterns**
Proven approaches for deploying {title.lower()} in production scenarios.

**3. Performance Optimization**
Techniques for maximizing efficiency and ensuring optimal performance.

### Practical Implementation

Here's how to implement {title.lower()} in Microsoft Fabric:

```python
# Microsoft Fabric implementation for {title}
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd

# Initialize connection
credential = DefaultAzureCredential()
service_client = DataLakeServiceClient(
    account_url="https://youraccount.dfs.core.windows.net",
    credential=credential
)

def implement_{title.lower().replace(' ', '_').replace('&', 'and')}():
    \"\"\"
    Implementation example for {title}
    This function demonstrates core concepts and patterns
    \"\"\"
    try:
        print(f"🚀 Starting {title} implementation...")
        
        # Core logic for {title.lower()}
        # Add your specific implementation here
        
        # Example data processing
        data = {{
            'component': '{title}',
            'status': 'active',
            'performance_metrics': {{
                'throughput': 1000,
                'latency_ms': 50,
                'success_rate': 0.99
            }}
        }}
        
        print(f"✅ {title} implementation completed successfully")
        return data
        
    except Exception as e:
        print(f"❌ Error in {title} implementation: {{e}}")
        return None

# Execute implementation
result = implement_{title.lower().replace(' ', '_').replace('&', 'and')}()
if result:
    print(f"Implementation successful: {{result}}")
```

### SQL Configuration

For data warehouse scenarios, configure {title.lower()} using SQL:

```sql
-- SQL configuration for {title} in Microsoft Fabric
-- Optimized for performance and scalability

CREATE OR ALTER PROCEDURE sp_Configure{title.replace(' ', '').replace('&', 'And')}
AS
BEGIN
    PRINT 'Configuring {title} for optimal performance...'
    
    -- Example configuration based on {title.lower()}
    IF OBJECT_ID('dbo.{title.replace(' ', '_').replace('&', 'And')}_Config', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.{title.replace(' ', '_').replace('&', 'And')}_Config (
            ConfigId INT IDENTITY(1,1) PRIMARY KEY,
            ComponentName NVARCHAR(100) NOT NULL,
            ConfigValue NVARCHAR(500),
            LastModified DATETIME2 DEFAULT GETDATE(),
            IsActive BIT DEFAULT 1
        )
        
        -- Insert default configuration
        INSERT INTO dbo.{title.replace(' ', '_').replace('&', 'And')}_Config 
        (ComponentName, ConfigValue)
        VALUES 
        ('{title}', 'Default configuration for {title.lower()}'),
        ('PerformanceMode', 'Optimized'),
        ('CachingEnabled', 'True')
        
        PRINT '✅ {title} configuration table created successfully'
    END
    ELSE
    BEGIN
        PRINT 'ℹ️  {title} configuration already exists'
    END
END
```

## Best Practices

### 1. Security & Governance
- Implement proper authentication using managed identities
- Follow principle of least privilege for access control
- Enable comprehensive auditing and monitoring
- Establish clear data governance policies

### 2. Performance Optimization
- Monitor resource utilization continuously
- Implement appropriate indexing strategies
- Use partitioning for large datasets
- Optimize query patterns and data access

### 3. Scalability Planning
- Design for horizontal scaling from the start
- Consider geographic distribution requirements
- Plan for data volume and user growth
- Implement auto-scaling where possible

### 4. Cost Management
- Monitor and optimize compute resource usage
- Implement scheduled scaling for predictable workloads
- Use appropriate storage tiers
- Leverage capacity reservations for consistent workloads

## Real-World Use Case

### Scenario: Enterprise Data Platform

**Business Requirements:**
- Process 10TB of data daily across multiple sources
- Support 500+ concurrent users
- Ensure 99.9% availability
- Meet regulatory compliance requirements

**Implementation Strategy:**

**Phase 1: Foundation Setup**
```python
# Phase 1: Core infrastructure setup
def setup_foundation():
    config = {{
        'data_sources': ['ERP', 'CRM', 'IoT', 'External APIs'],
        'processing_frequency': 'Real-time and Batch',
        'user_groups': ['Analysts', 'Data Scientists', 'Executives'],
        'compliance_requirements': ['GDPR', 'SOX', 'Industry specific']
    }}
    
    print("Setting up foundation architecture...")
    # Implementation details here
    return config
```

**Phase 2: {title} Implementation**
```python
# Phase 2: Specific {title.lower()} implementation
def implement_core_functionality():
    # Detailed implementation for {title.lower()}
    steps = [
        'Configure data ingestion pipelines',
        'Set up transformation logic',
        'Implement monitoring and alerting',
        'Test and validate functionality'
    ]
    
    for step in steps:
        print(f"Executing: {{step}}")
        # Detailed implementation for each step
    
    return "Implementation completed"
```

**Results Achieved:**
- ⚡ 40% improvement in data processing speed
- 💰 30% reduction in operational costs
- 📊 Real-time insights for decision making
- 🛡️ Enhanced security and compliance posture

## Common Challenges & Solutions

### Challenge 1: Performance Bottlenecks
**Problem:** Slow query performance during peak hours
**Solution:** 
- Implement proper indexing and partitioning
- Use materialized views for frequently accessed data
- Optimize data types and compression
- Consider query result caching

### Challenge 2: Data Quality Issues
**Problem:** Inconsistent or unreliable data affecting insights
**Solution:**
- Implement comprehensive data validation rules
- Set up automated data quality monitoring
- Establish data lineage and impact analysis
- Create data quality scorecards and alerts

### Challenge 3: Integration Complexity
**Problem:** Difficulty connecting disparate systems
**Solution:**
- Use standardized APIs and connectors
- Implement proper error handling and retry logic
- Establish clear data contracts between systems
- Monitor integration points continuously

## Monitoring & Troubleshooting

### Performance Monitoring

```python
# Monitoring framework for {title}
import logging
from datetime import datetime

class {title.replace(' ', '').replace('&', 'And')}Monitor:
    def __init__(self):
        self.metrics = {{}}
        self.alerts = []
    
    def collect_metrics(self):
        \"\"\"Collect performance metrics\"\"\"
        try:
            self.metrics = {{
                'timestamp': datetime.now().isoformat(),
                'component': '{title}',
                'status': 'healthy',
                'response_time_ms': 150,
                'throughput_per_second': 1000,
                'error_rate_percent': 0.1,
                'resource_utilization': {{
                    'cpu_percent': 65,
                    'memory_percent': 70,
                    'storage_percent': 45
                }}
            }}
            
            # Check thresholds and generate alerts
            self._check_thresholds()
            
            return self.metrics
            
        except Exception as e:
            logging.error(f"Error collecting metrics for {title}: {{e}}")
            return None
    
    def _check_thresholds(self):
        \"\"\"Check performance thresholds and generate alerts\"\"\"
        if self.metrics.get('response_time_ms', 0) > 1000:
            self.alerts.append({{
                'severity': 'warning',
                'message': f"{title} response time exceeded 1000ms"
            }})
        
        if self.metrics.get('error_rate_percent', 0) > 5:
            self.alerts.append({{
                'severity': 'critical',
                'message': f"{title} error rate exceeded 5%"
            }})

# Usage example
monitor = {title.replace(' ', '').replace('&', 'And')}Monitor()
metrics = monitor.collect_metrics()
if metrics:
    print(f"Current metrics: {{metrics}}")
    if monitor.alerts:
        print(f"Alerts: {{monitor.alerts}}")
```

## Advanced Configuration

### Enterprise-Grade Setup

```powershell
# PowerShell script for enterprise configuration
# Configure {title} for production environment

param(
    [string]$Environment = "Production",
    [string]$Region = "East US",
    [int]$CapacityUnits = 128
)

Write-Host "Configuring {title} for $Environment environment..." -ForegroundColor Green

# Resource configuration
$config = @{{
    Environment = $Environment
    Region = $Region
    CapacityUnits = $CapacityUnits
    ComponentName = "{title}"
    Settings = @{{
        AutoScale = $true
        BackupEnabled = $true
        MonitoringEnabled = $true
        SecurityLevel = "High"
    }}
}}

# Apply configuration
try {{
    Write-Host "Applying configuration..." -ForegroundColor Yellow
    # Configuration application logic here
    Write-Host "✅ {title} configured successfully" -ForegroundColor Green
    return $config
}}
catch {{
    Write-Error "❌ Failed to configure {title}: $($_.Exception.Message)"
    return $null
}}
```

## Integration Patterns

### Common Integration Scenarios

1. **Data Pipeline Integration**
   - Connect {title.lower()} with Azure Data Factory
   - Implement error handling and retry logic
   - Set up monitoring and alerting

2. **Real-time Analytics**
   - Stream processing with Azure Event Hubs
   - Real-time dashboard updates
   - Low-latency data processing

3. **Machine Learning Integration**
   - Feature engineering pipelines
   - Model training and deployment
   - MLOps best practices

## Next Steps & Learning Path

### Immediate Actions
1. **Hands-on Practice**: Set up a development environment
2. **Documentation Review**: Study Microsoft official documentation
3. **Community Engagement**: Join Microsoft Fabric community forums

### Advanced Topics to Explore
- Advanced security configurations
- Multi-region deployment strategies
- Disaster recovery and business continuity
- Custom connector development

### Recommended Learning Resources
- Microsoft Learn modules on Fabric
- Official Microsoft Fabric documentation
- Community blogs and case studies
- Certification paths (DP-600, DP-700)

## Conclusion

{title} represents a critical component in the Microsoft Fabric ecosystem. By understanding its core concepts, implementing best practices, and learning from real-world scenarios, you can leverage this technology to build robust, scalable data solutions that drive business value.

**Key Takeaways:**
- Understanding {title.lower()} is essential for Fabric success
- Implementation requires careful planning and best practices
- Monitoring and optimization are ongoing processes
- Community and continuous learning are valuable resources

As we continue our 100-day journey through Microsoft Fabric, each topic builds upon previous knowledge, creating a comprehensive foundation for data platform expertise.

**Tomorrow's Topic:** We'll explore another crucial aspect of Microsoft Fabric that will further enhance your understanding and practical capabilities.

---

*This article is part of the "Microsoft Fabric - 100 Days" series. Follow along for daily deep-dives into Microsoft Fabric concepts, implementations, and best practices.*

**Related Articles:**
- [Previous: Day {day-1 if day > 1 else day}](../day-{day-1 if day > 1 else day}.html)
- [Next: Day {day+1 if day < 100 else day}](../day-{day+1 if day < 100 else day}.html)
- [Series Index](../index.html)

**Connect & Share:**
- [Follow on LinkedIn](https://linkedin.com/in/mani-swaroop-sodadasi-1a165820a)
- [Share this article](https://linkedin.com/sharing/share-offsite/)
- [Join the discussion](https://linkedin.com/in/mani-swaroop-sodadasi-1a165820a)
"""

class FullAutomationSystem:
    """Main automation orchestrator"""
    
    def __init__(self):
        # Initialize APIs
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.linkedin_token = os.getenv('LINKEDIN_ACCESS_TOKEN') 
        self.person_id = os.getenv('LINKEDIN_PERSON_ID')
        
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable required")
        if not self.linkedin_token or not self.person_id:
            raise ValueError("LinkedIn credentials required: LINKEDIN_ACCESS_TOKEN, LINKEDIN_PERSON_ID")
        
        # Initialize components
        self._github_repo = _resolve_github_repo()
        self.github_api = GitHubAPI(self.github_token, self._github_repo)
        self.linkedin_api = LinkedInAPI(self.linkedin_token, self.person_id)
        self.content_generator = ContentGenerator()
        
        # State management
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.published_articles = self._load_published_articles()
        self.website_url = _resolve_website_url(self._github_repo)
        
        logger.info("Full automation system initialized")
    
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
        return self.github_api.create_or_update_file(
            'published_articles.json',
            content,
            f"Track Fabric 100 Days progress ({len(self.published_articles)} articles)",
        )
    
    def test_apis(self) -> Dict[str, bool]:
        """Test all API connections"""
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
    
    def create_article_html(self, day: int, title: str, content: str, category: str) -> str:
        """Generate complete HTML for article page"""
        
        # Convert markdown to basic HTML
        html_content = self._markdown_to_html(content)
        
        # Create slug for URL
        slug = f"day-{day}-{title.lower().replace(' ', '-').replace('&', 'and')}"
        
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
    
    def update_series_index(self):
        """Update the series index page with latest articles"""
        
        # Generate article cards
        article_cards = ""
        for article in self.published_articles[-12:]:  # Show latest 12
            published_date = datetime.datetime.fromisoformat(article['published_date'])
            
            article_cards += f"""
            <div class="article-card">
                <div class="day-badge">Day {article['day']}</div>
                <h3 class="article-title">{article['title']}</h3>
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
        <p>&copy; 2025 Mani Swaroop - Senior Data & AI Engineer<br>
           Microsoft Fabric 100 Days Learning Series</p>
    </footer>
</body>
</html>"""
        
        # Update series index
        success = self.github_api.create_or_update_file(
            "articles/fabric-100-days/index.html",
            index_html,
            f"Update series index - {len(self.published_articles)} articles published"
        )
        
        if success:
            logger.info(f"✅ Series index updated with {len(self.published_articles)} articles")
        else:
            logger.error("❌ Failed to update series index")
        
        return success
    
    def create_linkedin_post(self, day: int, day_content: Dict, article_url: str) -> str:
        """Create LinkedIn post content"""
        
        # Get pro tip based on the title
        pro_tips = {
            "What is Microsoft Fabric": "Think of Fabric as 'Azure Synapse + Power BI Premium + Data Factory' - but designed from the ground up for unified analytics.",
            "Fabric vs Synapse": "Fabric isn't replacing Synapse/Power BI - it's the next evolution with unified OneLake storage.",
            "Understanding Fabric Capacities": "Start with F64 (64 CU) for development. F2-F32 have limitations on Spark and advanced features.",
            "Fabric Workspaces": "Use security groups, not individual users for permissions. Create DEV/TEST/PROD workspaces for proper lifecycle management.",
            "OneLake": "OneLake shortcuts let you reference external data without copying - works with ADLS Gen2 accounts too!"
        }
        
        pro_tip = pro_tips.get(day_content['title'], f"Master {day_content['title'].lower()} to build more efficient and scalable data solutions in Microsoft Fabric.")
        
        # Extract the main content before the read more link
        main_content = day_content['linkedin_content'].split('📖')[0].strip()
        
        linkedin_post = f"""🧵 Microsoft Fabric - Day {day}/100

{main_content}

💡 Pro Tip: {pro_tip}

📖 Read the complete guide with step-by-step examples and best practices:
{article_url}

---
#MicrosoftFabric #DataEngineering #Azure #Analytics #100DaysChallenge #{day_content['category'].replace('_', '').title()}

👉 What's your experience with {day_content['title'].lower()}? Share your thoughts below!"""
        
        return linkedin_post
    
    def daily_automation_task(self):
        """Main daily automation task"""
        
        current_time = datetime.datetime.now(self.ist_timezone)
        day = len(self.published_articles) + 1
        
        logger.info(f"🚀 DAILY AUTOMATION - Day {day} - {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        if day > 100:
            logger.info("🎉 100 Days series completed!")
            return
        
        if day > len(self.content_generator.content_bank):
            logger.error(f"❌ No content available for Day {day}")
            return
        
        try:
            day_content = self.content_generator.content_bank[day - 1]
            
            logger.info(f"📝 Generating Day {day}: {day_content['title']}")
            
            # Generate detailed article content
            article_content = self.content_generator.generate_detailed_article(
                day, 
                day_content['title'], 
                day_content['category']
            )
            
            # Create article slug
            slug = f"day-{day}-{day_content['title'].lower().replace(' ', '-').replace('&', 'and')}"
            
            # Generate complete HTML
            article_html = self.create_article_html(
                day,
                day_content['title'],
                article_content,
                day_content['category']
            )
            
            # Upload article to GitHub
            file_path = f"articles/fabric-100-days/{slug}.html"
            github_success = self.github_api.create_or_update_file(
                file_path,
                article_html,
                f"Add Day {day}: {day_content['title']}"
            )
            
            if not github_success:
                logger.error(f"❌ Failed to create GitHub file for Day {day}")
                return
            
            # Article URL
            article_url = f"{self.website_url}/articles/fabric-100-days/{slug}.html"
            
            # Update published articles tracking
            article_info = {
                "day": day,
                "title": day_content['title'],
                "slug": slug,
                "url": article_url,
                "published_date": current_time.isoformat(),
                "category": day_content['category']
            }
            
            self.published_articles.append(article_info)
            self._save_published_articles()
            
            # Update series index
            index_success = self.update_series_index()
            
            # Create and post to LinkedIn
            linkedin_content = self.create_linkedin_post(day, day_content, article_url)
            linkedin_result = self.linkedin_api.post_to_linkedin(linkedin_content)
            
            if linkedin_result['success']:
                logger.info(f"✅ LinkedIn post successful - Post ID: {linkedin_result.get('post_id', 'Unknown')}")
            else:
                logger.error(f"❌ LinkedIn posting failed: {linkedin_result.get('error', 'Unknown error')}")
            
            # Log completion
            logger.info(f"✅ Day {day} automation completed!")
            logger.info(f"📄 Article: {article_url}")
            logger.info(f"📱 LinkedIn: {'✅ Posted' if linkedin_result['success'] else '❌ Failed'}")
            logger.info(f"🌐 Website: {'✅ Updated' if index_success else '❌ Failed'}")
            logger.info(f"📊 Progress: {day}/100 articles ({(day/100)*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"❌ Daily automation failed for Day {day}: {e}")
            raise
    
    def start_automation(self):
        """Start the full automation system"""
        
        logger.info("🚀 MICROSOFT FABRIC 100 DAYS - FULL AUTOMATION SYSTEM")
        logger.info("=" * 60)
        
        # Test API connections
        api_status = self.test_apis()
        
        if not api_status['github']:
            logger.error("❌ GitHub API connection failed")
            return
        
        if not api_status['linkedin']:
            logger.error("❌ LinkedIn API connection failed")
            return
        
        logger.info("✅ All API connections successful")
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
    args = parser.parse_args()

    if not args.once:
        print("🚀 MICROSOFT FABRIC 100 DAYS - FULL AUTOMATION SYSTEM")
        print("=" * 60)
        print()
        print("🎯 This system will automatically:")
        print("  ✅ Generate detailed articles daily at 9 AM IST")
        print("  ✅ Update your website with new content")
        print("  ✅ Post to LinkedIn with article links")
        print("  ✅ Update series index and tracking")
        print("  ✅ Run continuously for 100 days")
        print()
        print("🔑 Required Environment Variables:")
        print("  • GITHUB_TOKEN - GitHub Personal Access Token")
        print("  • LINKEDIN_ACCESS_TOKEN - LinkedIn API Access Token")
        print("  • LINKEDIN_PERSON_ID - Your LinkedIn Person ID")
        print("  • ANTHROPIC_API_KEY - Claude API Key (optional)")
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
            if not api_status.get('github'):
                logger.error("GitHub API connection failed")
                sys.exit(1)
            if not api_status.get('linkedin'):
                logger.error("LinkedIn API connection failed")
                sys.exit(1)
            automation_system.daily_automation_task()
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

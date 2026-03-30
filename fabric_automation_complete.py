#!/usr/bin/env python3
"""
Microsoft Fabric 100 Days - COMPLETE AUTOMATION
===============================================
Daily automation at 9:00 AM IST:
1. Generate article using Claude API
2. Publish to website 
3. Post to LinkedIn
4. Update progress tracking
"""

import os
import json
import requests
import datetime
import pytz
from typing import Dict
import base64
import schedule
import time

class FabricAutomation:
    def __init__(self):
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.linkedin_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.linkedin_person_id = os.getenv('LINKEDIN_PERSON_ID')
        self.anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
        
        self.repo_name = "maniswaroopsodadasi/maniswaroopsodadasi.github.io"
        self.website_url = "https://maniswaroopsodadasi.github.io"
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        self.github_headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        self.linkedin_headers = {
            "Authorization": f"Bearer {self.linkedin_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0"
        }
    
    def get_next_day_to_publish(self) -> int:
        """Get the next day number to publish"""
        try:
            # Load progress file
            response = requests.get(
                f"https://api.github.com/repos/{self.repo_name}/contents/published_articles.json",
                headers=self.github_headers
            )
            
            if response.status_code == 200:
                content = base64.b64decode(response.json()['content']).decode('utf-8')
                progress = json.loads(content)
                return len(progress.get('published_days', [])) + 1
            else:
                return 1
        except:
            return 1
    
    def daily_automation_task(self):
        """Run the daily automation task"""
        try:
            current_time = datetime.datetime.now(self.ist_timezone)
            day_number = self.get_next_day_to_publish()
            
            if day_number > 100:
                print(f"🎉 Series completed! All 100 days published.")
                return
            
            print(f"🚀 Running daily automation for Day {day_number}")
            
            # Generate article using Claude API
            article_content = self.generate_article_with_claude(day_number)
            
            if article_content:
                # Create article file
                filename = f"day-{day_number}-{article_content['slug']}.html"
                file_path = f"articles/fabric-100-days/{filename}"
                
                success = self.create_github_file(file_path, article_content['html'])
                
                if success:
                    # Post to LinkedIn
                    linkedin_success = self.post_to_linkedin(article_content['linkedin_content'])
                    
                    # Update progress
                    self.update_progress(day_number, filename)
                    
                    print(f"✅ Day {day_number} completed successfully!")
                else:
                    print(f"❌ Failed to publish Day {day_number}")
            else:
                print(f"❌ Failed to generate content for Day {day_number}")
                
        except Exception as e:
            print(f"❌ Daily automation error: {e}")
    
    def generate_article_with_claude(self, day_number: int) -> Dict:
        """Generate article using Claude API"""
        try:
            # Load content schedule
            response = requests.get(
                f"https://api.github.com/repos/{self.repo_name}/contents/enhanced_fabric_schedule.json",
                headers=self.github_headers
            )
            
            if response.status_code != 200:
                return None
            
            content = base64.b64decode(response.json()['content']).decode('utf-8')
            schedule_data = json.loads(content)
            
            day_info = None
            for day in schedule_data.get('days', []):
                if day['day'] == day_number:
                    day_info = day
                    break
            
            if not day_info:
                return None
            
            # Generate content with Claude API
            prompt = f"""
            Create a comprehensive technical article for Day {day_number} of Microsoft Fabric 100 Days series.
            
            Topic: {day_info['topic']}
            Category: {day_info['category']}
            Description: {day_info['description']}
            
            Requirements:
            1. 2500+ words comprehensive technical guide
            2. Include practical examples and code snippets
            3. Professional HTML formatting
            4. SEO optimized
            5. Include navigation and series branding
            
            Provide response in JSON format with:
            - html: Complete HTML article
            - linkedin_content: LinkedIn post content
            - slug: URL-friendly slug
            """
            
            claude_response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                    "x-api-key": self.anthropic_api_key
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4000,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            
            if claude_response.status_code == 200:
                claude_data = claude_response.json()
                content_text = claude_data['content'][0]['text']
                
                # Parse JSON from Claude response
                import re
                json_match = re.search(r'```json\n(.*?)\n```', content_text, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
            
            return None
            
        except Exception as e:
            print(f"Error generating article: {e}")
            return None
    
    def run_automation(self):
        """Run the automation scheduler"""
        # Schedule daily automation at 9:00 AM IST
        schedule.every().day.at("09:00").do(self.daily_automation_task)
        
        print("🤖 Microsoft Fabric 100 Days automation started!")
        print("📅 Daily publishing scheduled for 9:00 AM IST")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

if __name__ == "__main__":
    automation = FabricAutomation()
    automation.run_automation()

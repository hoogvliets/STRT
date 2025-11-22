import requests
from bs4 import BeautifulSoup
import yaml
import json
import os
import time
import random

def load_config():
    with open('config/linkedin-profiles.yaml', 'r') as file:
        return yaml.safe_load(file)

def fetch_profile_posts(username):
    # Note: Scraping LinkedIn is difficult without an API. 
    # This is a best-effort attempt using public profile pages.
    # It is highly likely to be blocked or require more complex handling (e.g. selenium/playwright)
    # which is out of scope for "simple python script" unless we use a proxy or lucky.
    # For this demo, we will try a simple request with a browser-like User-Agent.
    
    url = f"https://www.linkedin.com/in/{username}/recent-activity/all/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        # Add random delay to be polite
        time.sleep(random.uniform(1, 3))
        
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch {username}: Status {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        posts = []
        
        # Selectors are fragile and change often. This is a generic attempt.
        # We look for article/post containers.
        # In a real scenario, we might need to inspect the current DOM structure.
        # For now, let's assume we can find some content or return a placeholder if blocked.
        
        # Placeholder logic for demonstration if scraping fails (which it likely will on CI/CD IPs)
        # In a real production app, we'd use the LinkedIn API or a scraping service.
        
        # Let's try to find feed updates
        articles = soup.find_all('li', class_='profile-creator-shared-feed-update__container')
        
        if not articles:
             # Fallback selectors
            articles = soup.find_all('div', {'data-urn': True})

        for article in articles[:2]: # Limit to top 2
            # Extract data - this is highly dependent on current markup
            # Simplified extraction:
            text_div = article.find('span', class_='break-words')
            text = text_div.get_text(strip=True) if text_div else "No text content"
            
            # Link
            link_tag = article.find('a', class_='app-aware-link')
            link = link_tag['href'] if link_tag else f"https://www.linkedin.com/in/{username}"
            
            posts.append({
                'author': username,
                'summary': text[:200] + '...' if len(text) > 200 else text,
                'link': link,
                'source': 'LinkedIn',
                'published': datetime.now().isoformat() # Hard to parse relative dates reliably without JS
            })
            
        return posts

    except Exception as e:
        print(f"Error scraping {username}: {str(e)}")
        return []

def main():
    config = load_config()
    all_posts = []
    
    for profile in config.get('profiles', []):
        print(f"Fetching LinkedIn posts for {profile}...")
        posts = fetch_profile_posts(profile)
        all_posts.extend(posts)
    
    os.makedirs('data', exist_ok=True)
    with open('data/raw_linkedin.json', 'w') as f:
        json.dump(all_posts, f, indent=2)
        
    print(f"Fetched {len(all_posts)} LinkedIn posts.")

if __name__ == "__main__":
    from datetime import datetime
    main()

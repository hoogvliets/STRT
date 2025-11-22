import feedparser
import yaml
import json
import os
from datetime import datetime
from dateutil import parser
import time

def load_config():
    with open('config/substack-feeds.yaml', 'r') as file:
        return yaml.safe_load(file)

def fetch_feed(url):
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            print(f"Error parsing feed {url}: {feed.bozo_exception}")
            return []
        
        posts = []
        for entry in feed.entries:
            # Extract image if available
            image_url = None
            if 'media_content' in entry:
                image_url = entry.media_content[0]['url']
            elif 'links' in entry:
                for link in entry.links:
                    if link.rel == 'enclosure' and link.type.startswith('image/'):
                        image_url = link.href
                        break
            
            # Parse date
            published_date = None
            if hasattr(entry, 'published'):
                try:
                    dt = parser.parse(entry.published)
                    published_date = dt.isoformat()
                except:
                    published_date = datetime.now().isoformat()
            
            posts.append({
                'title': entry.title,
                'summary': entry.summary if hasattr(entry, 'summary') else '',
                'link': entry.link,
                'author': entry.author if hasattr(entry, 'author') else feed.feed.get('title', 'Unknown'),
                'source': feed.feed.get('title', 'Unknown Source'),
                'published': published_date,
                'image': image_url,
                'tags': [tag.term for tag in entry.tags] if hasattr(entry, 'tags') else [],
                'id': entry.id if hasattr(entry, 'id') else entry.link
            })
        return posts
    except Exception as e:
        print(f"Failed to fetch {url}: {str(e)}")
        return []

def main():
    config = load_config()
    all_posts = []
    
    for feed_url in config.get('feeds', []):
        print(f"Fetching {feed_url}...")
        posts = fetch_feed(feed_url)
        all_posts.extend(posts)
        
    # Sort by date (newest first)
    all_posts.sort(key=lambda x: x['published'] if x['published'] else '', reverse=True)
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Save raw feed data (processing will happen in process_data.py, but for now let's just save it)
    # Actually, per plan, process_data.py orchestrates this. 
    # So this script might just return data or save a temp file.
    # Let's make this script standalone capable but also importable.
    
    with open('data/raw_feeds.json', 'w') as f:
        json.dump(all_posts, f, indent=2)
    
    print(f"Fetched {len(all_posts)} posts from {len(config.get('feeds', []))} feeds.")

if __name__ == "__main__":
    main()

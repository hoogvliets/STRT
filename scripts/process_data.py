import json
import os
import sys
from datetime import datetime, timedelta
from dateutil import parser

# Import fetch functions
# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fetch_feeds import main as fetch_feeds_main, fetch_feed, load_config as load_rss_config
from fetch_linkedin import main as fetch_linkedin_main, fetch_profile_posts, load_config as load_linkedin_config

DATA_DIR = 'data'
FEED_FILE = os.path.join(DATA_DIR, 'feed.json')
LINKEDIN_FILE = os.path.join(DATA_DIR, 'linkedin.json')
ERROR_LOG = os.path.join(DATA_DIR, 'errors.log')

def load_existing_data(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_data(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def log_error(message):
    timestamp = datetime.now().isoformat()
    with open(ERROR_LOG, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")

def clean_old_posts(posts, days=60):
    cutoff = datetime.now() - timedelta(days=days)
    valid_posts = []
    for post in posts:
        try:
            pub_date = parser.parse(post['published'])
            # Make pub_date offset-naive if it's offset-aware, or vice versa to match cutoff
            # Simplest is to compare timestamps or ensure both are same type
            if pub_date.tzinfo is not None and cutoff.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=None)
            
            if pub_date > cutoff:
                valid_posts.append(post)
        except:
            # If date parsing fails, keep it safe or discard? 
            # Let's keep it but log warning? No, better to discard if invalid to keep clean.
            # Actually, let's default to keeping it if we just fetched it, but here we are cleaning old ones.
            # If we can't parse date, we can't determine age. Let's assume it's new if we can't parse?
            # Or just drop. Let's drop to be safe.
            pass
    return valid_posts

def deduplicate(new_posts, existing_posts):
    # Create a dict of existing posts by ID (or link)
    posts_map = {p.get('id', p.get('link')): p for p in existing_posts}
    
    # Update/Add new posts
    for post in new_posts:
        pid = post.get('id', post.get('link'))
        posts_map[pid] = post
        
    return list(posts_map.values())

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # --- Process RSS Feeds ---
    print("Processing RSS feeds...")
    try:
        rss_config = load_rss_config()
        new_rss_posts = []
        for url in rss_config.get('feeds', []):
            new_rss_posts.extend(fetch_feed(url))
            
        existing_rss = load_existing_data(FEED_FILE)
        merged_rss = deduplicate(new_rss_posts, existing_rss)
        cleaned_rss = clean_old_posts(merged_rss)
        
        # Sort
        cleaned_rss.sort(key=lambda x: x['published'] if x['published'] else '', reverse=True)
        
        save_data(FEED_FILE, cleaned_rss)
        print(f"Saved {len(cleaned_rss)} RSS posts.")
        
    except Exception as e:
        log_error(f"RSS Processing Error: {str(e)}")
        print(f"RSS Error: {str(e)}")

    # --- Process LinkedIn ---
    print("Processing LinkedIn profiles...")
    try:
        linkedin_config = load_linkedin_config()
        new_li_posts = []
        for profile in linkedin_config.get('profiles', []):
            new_li_posts.extend(fetch_profile_posts(profile))
            
        existing_li = load_existing_data(LINKEDIN_FILE)
        merged_li = deduplicate(new_li_posts, existing_li)
        cleaned_li = clean_old_posts(merged_li)
        
        # Sort
        cleaned_li.sort(key=lambda x: x['published'] if x['published'] else '', reverse=True)
        
        save_data(LINKEDIN_FILE, cleaned_li)
        print(f"Saved {len(cleaned_li)} LinkedIn posts.")
        
    except Exception as e:
        log_error(f"LinkedIn Processing Error: {str(e)}")
        print(f"LinkedIn Error: {str(e)}")

if __name__ == "__main__":
    main()

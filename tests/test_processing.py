import pytest
from datetime import datetime, timedelta
from scripts.process_data import clean_old_posts, deduplicate

def test_clean_old_posts():
    now = datetime.now()
    old_date = (now - timedelta(days=61)).isoformat()
    new_date = (now - timedelta(days=1)).isoformat()
    
    posts = [
        {'title': 'Old Post', 'published': old_date},
        {'title': 'New Post', 'published': new_date}
    ]
    
    cleaned = clean_old_posts(posts, days=60)
    assert len(cleaned) == 1
    assert cleaned[0]['title'] == 'New Post'

def test_deduplicate():
    existing = [{'id': '1', 'title': 'Post 1'}]
    new_posts = [
        {'id': '1', 'title': 'Post 1 Updated'},
        {'id': '2', 'title': 'Post 2'}
    ]
    
    deduped = deduplicate(new_posts, existing)
    assert len(deduped) == 2
    
    # Verify update happened (last write wins in current logic?)
    # Actually current logic:
    # posts_map = {p.get('id', p.get('link')): p for p in existing_posts}
    # for post in new_posts: posts_map[pid] = post
    # So new posts overwrite existing ones.
    
    post1 = next(p for p in deduped if p['id'] == '1')
    assert post1['title'] == 'Post 1 Updated'

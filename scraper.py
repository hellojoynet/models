import os
import pandas as pd
import numpy as np
import sqlite3
import logging
import requests
import re
import json
import argparse

logging.basicConfig(level=logging.INFO)

class SocialMediaDBScraper:
    def __init__(self):
        self.db_path = os.path.join(os.path.expanduser('~'), 'Desktop/JoyNet/norman/scraped_data/scraped_videos.db')
        self._create_tables()
        self.root = "https://ensembledata.com/apis"
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def _create_tables(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS scraped_data 
                        (
                        p_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        platform TEXT NOT NULL
                        )
                        
                        ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS tiktok 
                        (
                        v_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        p_id INTEGER,
                        search_query TEXT,
                        search_days INTEGER,
                        max_cursor INTEGER,
                        url TEXT UNIQUE,
                        username TEXT,
                        description TEXT UNIQUE,
                        hashtags TEXT,
                        likes INTEGER,
                        comments INTEGER,
                        views INTEGER,
                        collects INTEGER,
                        shares INTEGER,
                        video_mp3 TEXT,
                        FOREIGN KEY (p_id) REFERENCES scraped_data(p_id)
                        )
                        ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS instagram
                           (
                           v_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           p_id INTEGER,
                           search_query TEXT,
                           max_cursor INTEGER,
                           url TEXT UNIQUE,
                           username TEXT,
                           description TEXT,
                           likes INTEGER,
                           comments INTEGER,
                           code TEXT UNIQUE,
                           FOREIGN KEY (p_id) REFERENCES scraped_data(p_id)
                           )
                           ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS threads
                           (
                           v_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           p_id INTEGER,
                           search_query TEXT,
                           url TEXT,
                           username TEXT,
                           verified TEXT,
                           description TEXT,
                           likes INTEGER,
                           comments INTEGER,
                           shares INTEGER,
                           quotes INTEGER,
                           FOREIGN KEY (p_id) REFERENCES scraped_data(p_id),
                           UNIQUE(username, description, likes, comments, shares, quotes)
                           )
                           ''')
            conn.commit()

    def get_platform(self, platform_name):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT p_id FROM scraped_data WHERE platform = ?', (platform_name,))
            platform = cursor.fetchone()
            if platform:
                return platform[0]
            else:
                cursor.execute('INSERT INTO scraped_data (platform) VALUES (?)', (platform_name,))
                conn.commit()
                new_pid = cursor.lastrowid
                logging.info(f'Added: {platform_name} to database under (p_id={new_pid})')
                return new_pid
            
    ### FETCHING SOCIAL MEDIA PLATFORM DATA METHODS ###
    def fetch_instagram_data(self, platform, hashtag, token, max_cursor):
        params = {
            "name": hashtag,
            "cursor": "",
            "chunk_size": f'{max_cursor}',
            "get_author_info": True,
            "token": token
        }
        res = requests.get(self.root + '/instagram/hashtag/posts', params=params)
        data = res.json()
        scrape_features = self.add_platform_database(platform, data, hashtag, None, max_cursor)
        return scrape_features

    def fetch_tiktok_data(self, platform, hashtag, token, days, max_cursor):
        params = {
            'name': hashtag,
            'days': days,
            'remap_output': True,
            'max_cursor': max_cursor,
            'token': token
            }
        res = requests.get(self.root + '/tt/hashtag/recent-posts', params=params)
        data = res.json()
        scrape_features = self.add_platform_database(platform, data, hashtag, days, max_cursor)
        return scrape_features
    
    def fetch_threads_data(self, platform, hashtag, token):
        params = {
        "name": hashtag,
        "sorting": 0,
        "token": token
        }
        res = requests.get(self.root + '/threads/keyword/search', params=params)
        data = res.json()
        scrape_features = self.add_platform_database(platform, data, hashtag, None, None)
        return scrape_features

    def add_platform_database(self, platform, video_metadata, hashtag, days, max_cursor):
        platform = platform.lower()
        pid = self.get_platform(platform)

        if platform == 'tiktok':
            video_metadata = video_metadata.get('data', {}).get('posts', {})
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    for i in range(len(video_metadata)):
                        video_info = video_metadata[i].get('itemInfos', {})
                        user_info = video_metadata[i].get('authorInfos', {}).get('uniqueId', {})
                        description = video_info.get('text', {})
                        play_urls = video_metadata[i].get('musicInfos', {}).get('playUrl', [])
                        video_mp3 = play_urls[0] if isinstance(play_urls, list) and len(play_urls) > 0 else None
                        features = {
                            'url_link': video_info.get('video', {}).get('urls', {})[0],
                            'username': user_info,
                            'description': description,
                            'hashtags': re.findall(r'#\w+', description),
                            'likes': video_info.get('diggCount', 0),
                            'comments': video_info.get('commentCount', 0),
                            'views': video_info.get('playCount', 0),
                            'collects': video_info.get('collectCount', 0),
                            'shares': video_info.get('shareCount', 0),
                            'video_mp3': video_mp3
                        }
                        try:
                            cursor.execute('SELECT 1 FROM tiktok WHERE url = ?', (features['url_link'],))
                            if cursor.fetchone():
                                logging.warning(f"URL already exists: {features['url_link']}")
                                continue
                            hashtags = ', '.join(features['hashtags']) if isinstance(features['hashtags'], list) else ''
                            description_clean = features['description'].encode('utf-8', 'ignore').decode('utf-8')
                            cursor.execute('''
                                INSERT INTO tiktok (p_id, search_query, search_days, max_cursor, url, username, description, hashtags, likes, comments, views, collects, shares, video_mp3)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (pid, hashtag, days, max_cursor, features['url_link'], features['username'], description_clean, hashtags,
                                features['likes'], features['comments'], features['views'],
                                features['collects'], features['shares'], features['video_mp3']))
                        except sqlite3.IntegrityError as e:
                            logging.warning(f"Failed to insert video with URL {features['url_link']}: {e}")
                    conn.commit()
                    logging.info(f'Added {len(video_metadata)} videos with {hashtag} to {platform} database.')
            
            except sqlite3.IntegrityError:
                logging.warning(f'URL already in database.')
        
        elif platform == 'instagram':

            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    video_info = video_metadata.get('data', {}).get('recent_posts', {})
                    for i in range(len(video_info)):
                        n_data = video_info[i].get('node', {})
                        features = {
                                    'url_link': n_data.get('display_url', {}),
                                    'user': n_data.get('owner', {}).get('username',{}),
                                    'description': n_data.get('edge_media_to_caption', {}).get('edges', {})[0].get('node', {}).get('text', {}),
                                    'likes': n_data.get('edge_liked_by', {}).get('count', {}),
                                    'comments': n_data.get('edge_media_to_comment', {}).get('count', {}),
                                    'code': n_data.get('shortcode', {})
                        }
                        try:
                            cursor.execute('SELECT 1 FROM instagram WHERE url = ?', (features['url_link'],))
                            if cursor.fetchone():
                                logging.warning(f"URL already exists: {features['url_link']}")
                                continue
                            description_clean = features['description'].encode('utf-8', 'ignore').decode('utf-8')
                            cursor.execute('''
                                           INSERT INTO instagram (p_id, search_query, max_cursor, url, username, description, likes, comments, code)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                           ''', (pid, hashtag, max_cursor, features['url_link'], features['user'], description_clean,
                                                 features['likes'], features['comments'], features['code']))
                        except sqlite3.IntegrityError as e:
                            logging.warning(f"Failed to insert video with URL {features['url_link']}: {e}")
                    conn.commit()
                    logging.info(f'Added {len(video_info)} videos with {hashtag} to {platform} database.')

            except sqlite3.IntegrityError:
                logging.warning(f'URL already in database.')
        
        elif platform == 'threads':
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    video_info = video_metadata.get('data', {})
                    idx = 0
                    for i in range(len(video_info)):
                        n_data = video_info[i].get('node', {})
                        post_items = n_data.get('thread', {}).get('thread_items', [])[0].get('post', {})
                        if post_items:
                            video_items = post_items.get('video_versions', [])
                            url_link = video_items[0].get('url') if video_items else None
                            features = {
                                    'url_link': url_link,
                                    'user': post_items.get('user', {}).get('username', {}),
                                    'verified': post_items.get('user', {}).get('is_verified', {}),
                                    'description': post_items.get('text_post_app_info', {}).get('text_fragments', {}).get('fragments', {})[0].get('plaintext', {}),
                                    'likes': post_items.get('like_count') or 0,
                                    'comments': post_items.get('text_post_app_info', {}).get('direct_reply_count') or 0,
                                    'shares': post_items.get('text_post_app_info', {}).get('repost_count') or 0,
                                    'quotes': post_items.get('text_post_app_info', {}).get('quote_count') or 0
                                }
                            cursor.execute('''
                                SELECT 1 FROM threads 
                                WHERE url = ? AND username = ? AND description = ? AND likes = ? 
                                AND comments = ? AND shares = ? AND quotes = ?
                            ''', (features['url_link'], features['user'], features['description'], features['likes'],
                                features['comments'], features['shares'], features['quotes']))

                            if cursor.fetchone():
                                logging.warning(f"Duplicate row found, skipping insertion for URL: {features['url_link']}")
                                continue
                            try:
                                description_clean = features['description'].encode('utf-8', 'ignore').decode('utf-8')
                                cursor.execute('''
                                    INSERT INTO threads (p_id, search_query, url, username, verified, description, likes, comments, shares, quotes)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (pid, hashtag, features['url_link'], features['user'], features['verified'], description_clean,
                                    features['likes'], features['comments'], features['shares'], features['quotes']))
                                idx += 1
                            except sqlite3.IntegrityError as e:
                                logging.warning(f"Failed to insert video with URL {features['url_link']}: {e}")

                    conn.commit()
                logging.info(f'Added {idx} videos with {hashtag} to {platform} database.')

            except sqlite3.IntegrityError:
                logging.warning(f'URL already in database.')
            
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TikTok Scraper')
    parser.add_argument('--platform', type=str, default='TikTok', help='Selected platform to scrape from')
    parser.add_argument('--hashtag', type=str, default='blackjoy', help='Selected hashtag to scrape from')
    parser.add_argument('--days', type=int, default=100000, help='Selected days to filter beyond video scraping')
    parser.add_argument('--max_cursor', type=int, default=10000, help='Choose maximum number of videos to scrape total')
    parser.add_argument('--token', type=str, default='yoUrbeamvcYVcG5w', help='Specific account token from EnsembleData API')
    args = parser.parse_args()

    smdb_manager = SocialMediaDBScraper()
    print(f'Scraping {args.platform} with hashtag {args.hashtag} and adding to database . . .')

    if args.platform.lower() == 'tiktok':
        smdb_manager.fetch_tiktok_data(
            platform=args.platform,
            hashtag=args.hashtag,
            token=args.token,
            days = args.days,
            max_cursor= args.max_cursor
        )
    elif args.platform.lower() == 'instagram':
        smdb_manager.fetch_instagram_data(
            platform=args.platform,
            hashtag=args.hashtag,
            token=args.token,
            max_cursor=args.max_cursor
        )
    elif args.platform.lower() == 'threads':
        smdb_manager.fetch_threads_data(
            platform=args.platform,
            hashtag=args.hashtag,
            token=args.token
        )

    else:
        None
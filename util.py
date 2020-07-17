from datetime import datetime, timedelta
from html import unescape
from os import environ

import praw
import pymongo
import requests
from unidecode import unidecode

MAX_SIZE = 500
REDDIT = praw.Reddit(
  client_id=environ['REDDIT_CLIENT_ID'],
  client_secret=environ['REDDIT_CLIENT_SECRET'],
  user_agent=environ['REDDIT_USER_AGENT'])

class Database():
  def __init__(self):
    self.client = pymongo.MongoClient(environ['MONGODB_URI'])

  def insert(self, posts):
    return self.client.hhh.posts.insert_many(posts)

  def update_score(self, post_id):
    return self.client.hhh.posts.update_one(
      {'_id': post_id},
      { '$set': { 'score': REDDIT.submission(post_id).score, 'updated': datetime.utcnow() } })

  def get_scoreless_posts(self):
    return self.client.hhh.posts.find({'score': None})

  def get_outdated_posts(self):
    # One month in seconds = 2592000; ms = 2592000000
    return self.client.hhh.posts.find({
      'updated': { '$lt': datetime.utcnow() - timedelta(hours=2) },
      '$expr': { '$lt': ['$updated', {'$add': ['$created', 2592000000]}] }})

  def get_max_timestamp(self):
    return self.client.hhh.posts.find_one(sort=[('created', pymongo.DESCENDING)])['created']

def get_pushshift_posts(after, before):
  params = {
    'subreddit': 'hiphopheads',
    'title': '[FRESH',
    'fields': ['created_utc', 'id', 'score', 'title', 'url'],
    'size': MAX_SIZE,
    'after': int(after.timestamp()),
    'before': int(before.timestamp()),
  }
  r = requests.get('https://api.pushshift.io/reddit/search/submission/', params=params)
  if not r.ok:
    raise RuntimeError(f'Pushshift error: {r.text}')
  posts = [_create_post(post) for post in r.json()['data']]
  if len(posts) == MAX_SIZE:
    raise RuntimeError('Number of posts has reached MAX_SIZE - adjust chunk size.')
  return posts

def _create_post(post):
  return {
    '_id': post['id'],
    'title': unescape(post['title']),
    'searchable': unidecode(unescape(post['title'])),
    'url': post['url'],
    'created': datetime.utcfromtimestamp(post['created_utc']),
  }

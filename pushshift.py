from datetime import datetime
from typing import TypedDict

import requests
from unidecode import unidecode


class PushshiftPost(TypedDict):
  id: str
  title: str
  url: str
  created_utc: int


def get_pushshift_posts(after: datetime, before: datetime) -> list[PushshiftPost]:
  params = {
    'subreddit': 'hiphopheads',
    'title': '[FRESH',
    'fields': ['created_utc', 'id', 'score', 'title', 'url'],
    'size': 500,  # Through manual testing, this actually seems to be 100
    'after': int(after.timestamp()),
    'before': int(before.timestamp()),
  }
  r = requests.get('https://api.pushshift.io/reddit/search/submission/', params=params)
  if not r.ok:
    raise RuntimeError(f'Pushshift error: {r.text}')
  return r.json()['data']

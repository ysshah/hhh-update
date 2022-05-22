import os

import praw

REDDIT = praw.Reddit(client_id=os.environ['REDDIT_CLIENT_ID'],
                     client_secret=os.environ['REDDIT_CLIENT_SECRET'],
                     user_agent=os.environ['REDDIT_USER_AGENT'])


def get_score(post_id: str) -> int:
  return REDDIT.submission(post_id).score

from datetime import datetime, timedelta, timezone
from time import sleep

from tqdm import tqdm

import db
import reddit
import pushshift

DELTA = timedelta(days=1)


def insert_posts(after: datetime, before: datetime):
  posts = pushshift.get_pushshift_posts(after, before)
  print(f'Retrieved {len(posts)} posts from {after} to {before}')
  if len(posts) < 100:
    db.insert(posts)
    # inserted = sum(row[0] for row in db.insert_with_duplicates(posts))
    # print(f'Inserted {inserted} posts')
    return
  print('Splitting...')
  delta = before - after
  middle = after + delta / 2
  sleep(1)
  insert_posts(after, middle)
  sleep(1)
  insert_posts(middle, before)


def insert_old_posts():
  before = db.get_timestamp_of_oldest_post()
  after = before - DELTA
  while before > datetime(2010, 6, 19, tzinfo=timezone.utc):
    insert_posts(after, before)
    after -= DELTA
    before -= DELTA
    sleep(1)  # Set rate limit to 1 QPS: https://pypi.org/project/pushshift.py


def insert_new_posts():
  after = db.get_timestamp_of_latest_post()
  before = after + DELTA
  limit = datetime.utcnow()
  print(f'Retrieving posts from {after} to {limit}')
  while after < limit:
    insert_posts(after, before)
    after += DELTA
    before += DELTA
    sleep(1)  # Set rate limit to 1 QPS: https://pypi.org/project/pushshift.py


def update_scores(post_ids: list[str]):
  print(f'Updating the scores of {len(post_ids)} posts')
  for post_id in tqdm(post_ids):
    db.set_score(post_id, reddit.get_score(post_id))


def main():
  update_scores(db.get_scoreless_posts())
  # update_scores(db, db.get_outdated_posts())


if __name__ == "__main__":
  main()

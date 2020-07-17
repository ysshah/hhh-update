from datetime import datetime, timedelta
import util

DELTA = timedelta(days=1)

def insert(db):
  after = db.get_max_timestamp()
  before = after + DELTA
  limit = datetime.utcnow()
  print(f'Retrieving posts from {after} to {limit}')
  while after < limit:
    posts = util.get_pushshift_posts(after, before)
    print(f'Retrieved {len(posts)} posts from {after} to {before}')
    if len(posts) == 0: return
    db.insert(posts)
    after += DELTA
    before += DELTA

def update_scores(db, cursor):
  posts = list(cursor)
  print(f'Updating the scores of {len(posts)} posts')
  for post in posts:
    db.update_score(post['_id'])

def main():
  db = util.Database()
  insert(db)
  update_scores(db, db.get_scoreless_posts())
  update_scores(db, db.get_outdated_posts())

if __name__ == "__main__":
  main()

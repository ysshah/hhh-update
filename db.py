'''
To get the schema:

SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_name = 'post'

Update score if:
- Post is less than 1 day old and score is at least 1 hour old
- Post is less than 1 week old and score is at least 1 day old
- Post is less than 1 month old and score is at least 1 week old
- Post is more than 1 month old and score is less than 1 month older than the post
'''
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import psycopg

from pushshift import PushshiftPost


@dataclass
class Post:
  id: str
  title: str
  url: str
  score: Optional[int]
  created: datetime
  updated: datetime


def create_table():
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    connection.execute('''
      CREATE TABLE post (
        id VARCHAR(10) PRIMARY KEY NOT NULL,
        title VARCHAR(300) NOT NULL,
        url TEXT NOT NULL,
        score INTEGER,
        created TIMESTAMP (0) WITH TIME ZONE NOT NULL,
        updated TIMESTAMP (0) WITH TIME ZONE NOT NULL)
      ''')


def insert(posts: list[PushshiftPost]):
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    with connection.cursor() as cursor:
      with cursor.copy('COPY post (id, title, url, created, updated) FROM STDIN') as copy:
        for post in posts:
          copy.write_row([
            post['id'], post['title'], post['url'],
            datetime.utcfromtimestamp(post['created_utc']),
            datetime.utcnow()
          ])


def insert_with_duplicates(posts: list[PushshiftPost]):
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    with connection.cursor() as cursor:
      cursor.execute('''
        CREATE TEMP TABLE tmp
        ON COMMIT DROP
        AS
        SELECT * FROM post
        WITH NO DATA''')
      with cursor.copy('COPY tmp (id, title, url, created, updated) FROM STDIN') as copy:
        for post in posts:
          copy.write_row([
            post['id'], post['title'], post['url'],
            datetime.utcfromtimestamp(post['created_utc']),
            datetime.utcnow()
          ])
      return cursor.execute('''
        INSERT INTO post
        SELECT * FROM tmp
        ON CONFLICT DO NOTHING
        RETURNING 1''').fetchall()


def set_score(id: str, score: int):
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    connection.execute('UPDATE post SET score = %s, updated = %s WHERE id = %s',
                       [score, datetime.utcnow(), id])


def get_scoreless_posts() -> list[str]:
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    return [
      row[0] for row in connection.execute('SELECT id FROM post WHERE score IS NULL').fetchall()
    ]


def get_timestamp_of_latest_post() -> datetime:
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    if not (timestamp := connection.execute('SELECT MAX(created) FROM post').fetchone()):
      raise RuntimeError('Unable to fetch timestamp of latest post')
    return timestamp[0]


def get_timestamp_of_oldest_post() -> datetime:
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    if not (timestamp := connection.execute('SELECT MIN(created) FROM post').fetchone()):
      raise RuntimeError('Unable to fetch timestamp of latest post')
    return timestamp[0]


def get_outdated_posts():
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    rows = connection.execute(
      'SELECT id FROM post WHERE NOW() - created < %s AND updated - created > %s',
      [timedelta(days=30), timedelta(days=7)]).fetchall()
    # [timedelta(days=1), timedelta(hours=1)]).fetchall()
    print(len(rows))


def mark_duplicates():
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    pass


if __name__ == '__main__':
  get_outdated_posts()

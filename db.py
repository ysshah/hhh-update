import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg
from psycopg.rows import class_row

from util import PushshiftPost
import reddit


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
        id CHARACTER(6) PRIMARY KEY NOT NULL,
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
  print(f'Setting post {id} = {score}')
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
    return connection.execute('SELECT MAX(created) FROM post').fetchone()[0]


def get_timestamp_of_oldest_post() -> datetime:
  with psycopg.connect(os.environ['DATABASE_URL']) as connection:
    return connection.execute('SELECT MIN(created) FROM post').fetchone()[0]


if __name__ == '__main__':
  print(
    insert_with_duplicates([{
      'id': 'u7ykkv',
      'title': '[FRESH] Peaceful Piranha - My Funky Buddha',
      'url': 'https://youtu.be/bBLBdH3GLFM',
      'created_utc': 0
    }]))

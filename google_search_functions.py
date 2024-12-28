import typing as tp
import aiohttp
import os

from kp_search_functions import get_sql_kp_post


API_KEY = os.environ['GOOGLE_SEARCH_TOKEN']


def create_sql_google_search(conn: tp.Any) -> tp.Any:
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS google_search;
    ''')
    cursor.execute('''
        CREATE TABLE google_search (
            kinopoisk_id TEXT,
            link TEXT,
            title TEXT,
            source TEXT,
            user_id TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    return cursor


async def append_sql_google_search(
        conn: tp.Any,
        kinopoisk_id: str,
        movie_data: list[dict[tp.Any, tp.Any]],
        user_id: str,
        timestamp: str) -> None:
    cursor = conn.cursor()
    for movie in movie_data:
        cursor.execute('''
            INSERT OR IGNORE INTO google_search (
                kinopoisk_id, link, title, source,
                user_id, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            kinopoisk_id,
            movie['link'],
            movie['title'],
            movie['source'],
            user_id,
            timestamp
        ))
        conn.commit()

        cursor.execute('''
            DELETE FROM google_search
            WHERE timestamp < (
                SELECT MAX(timestamp)
                FROM google_search AS sub
                WHERE google_search.user_id = sub.user_id
                AND google_search.kinopoisk_id = sub.kinopoisk_id
            )
        ''')
        conn.commit()


async def get_sql_google_search(conn: tp.Any, kinopoisk_id: str, user_id: str) -> list[dict[tp.Any, tp.Any]]:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM google_search WHERE kinopoisk_id = ? AND user_id = ?", (kinopoisk_id, user_id))
    search_rows = cursor.fetchall()
    result = [
        {
            'id': row[0],
            'link': row[1],
            'title': row[2],
            'source': row[3],
            'user_id': row[4],
            'timestamp': row[5]
        }
        for row in search_rows
    ]
    return result


async def parse_google_json(data: list[dict[tp.Any, tp.Any]]) -> list[dict[tp.Any, tp.Any]]:
    urls = [
        {
            "title": res.get("title", "No Title"),
            "source": res.get("source", "").split('.')[0],
            "link": res.get("link")
        }
        for res in data
        if "link" in res.keys()
    ]
    return urls


async def get_request_google_api(conn: tp.Any, id: str, user_id: str, timestamp: str) -> list[dict[tp.Any, tp.Any]]:
    search_rows = await get_sql_google_search(conn, id, user_id)
    if not search_rows:
        movie_row = await get_sql_kp_post(conn, id, user_id)
        movie = movie_row[0]
        url = "https://www.searchapi.io/api/v1/search"
        params = {
            "engine": "google",
            "q": f"{movie['name']} ({movie['alternativeName']}, {movie['year']})\
смотреть онлайн бесплатно без смс и регистрации на русском",
            "api_key": API_KEY
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                response_text = await response.json()
                search_rows_from_api = await parse_google_json(response_text.get("organic_results", []))
                await append_sql_google_search(conn, id, search_rows_from_api, user_id, timestamp)
                return search_rows_from_api

    await append_sql_google_search(conn, id, search_rows, user_id, timestamp)
    return search_rows


async def google_search_message(search_data: list[dict[tp.Any, tp.Any]]) -> str:
    if not search_data:
        return 'Ничего не нашлось :('
    result = '\n'.join(
        [
            f"{idx}. *{u['source']}*: [{u['title']}]({u['link']})\n" for idx, u in enumerate(search_data, 1)
        ]
    )
    return result

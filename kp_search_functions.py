import typing as tp
import aiohttp
import os

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

API_KEY = os.environ['KINOPOISK_TOKEN']


def create_sql_kp_post(conn: tp.Any) -> None:
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS kp_post;
    ''')
    cursor.execute('''
        CREATE TABLE kp_post (
            kinopoisk_id TEXT,
            name TEXT,
            alternative_name TEXT,
            year INTEGER,
            country TEXT,
            imdb_rating REAL,
            kp_rating REAL,
            runtime INTEGER,
            description TEXT,
            genres TEXT,
            poster_url TEXT,
            kp_url TEXT,
            imdb_url TEXT,
            user_id TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()


def create_sql_kp_search(conn: tp.Any) -> None:
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS kp_search;
    ''')
    cursor.execute('''
        CREATE TABLE kp_search (
            search_text TEXT,
            kinopoisk_id TEXT,
            name TEXT,
            year INTEGER,
            user_id TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    return cursor


def create_sql_kp_count(conn: tp.Any) -> None:
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS kp_count;
    ''')
    cursor.execute('''
        CREATE TABLE kp_count (
            search_text TEXT,
            user_id TEXT
        )
    ''')
    conn.commit()
    return cursor


async def append_sql_kp_post(
        conn: tp.Any,
        movie_data: list[dict[tp.Any, tp.Any]],
        user_id: str,
        timestamp: str) -> None:
    cursor = conn.cursor()
    for movie in movie_data:
        cursor.execute('''
            INSERT OR IGNORE INTO kp_post (
                kinopoisk_id, name, alternative_name, year, country, imdb_rating,
                kp_rating, runtime, description, genres, poster_url,
                kp_url, imdb_url,
                user_id, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            movie['id'],
            movie['name'],
            movie['alternativeName'],
            movie['year'],
            movie['country'],
            movie['imdb_rating'],
            movie['kp_rating'],
            movie['runtime'],
            movie['description'],
            movie['genres'],
            movie['poster_url'],
            movie['kp_url'],
            movie['imdb_url'],
            user_id,
            timestamp
        ))
        conn.commit()
        cursor.execute('''
            DELETE FROM kp_post
            WHERE timestamp < (
            SELECT MAX(timestamp)
                FROM kp_post AS sub
                WHERE kp_post.user_id = sub.user_id
                AND kp_post.kinopoisk_id = sub.kinopoisk_id
            )
        ''')
        conn.commit()


async def append_sql_kp_search(
        conn: tp.Any,
        search_text: str,
        movie_data: list[dict[tp.Any, tp.Any]],
        user_id: str,
        timestamp: str) -> None:
    cursor = conn.cursor()
    for movie in movie_data:
        cursor.execute('''
            INSERT OR IGNORE INTO kp_search (
                search_text, kinopoisk_id, name, year,
                user_id, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            search_text,
            movie['id'],
            movie['name'],
            movie['year'],
            user_id,
            timestamp
        ))
        conn.commit()
        cursor.execute('''
            DELETE FROM kp_search
            WHERE timestamp < (
                SELECT MAX(timestamp)
                FROM kp_search AS sub
                WHERE kp_search.user_id = sub.user_id
                AND kp_search.search_text = sub.search_text
            )
        ''')
        conn.commit()


async def append_sql_kp_count(
        conn: tp.Any,
        search_text: str,
        user_id: str) -> None:
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO kp_count (
            search_text, user_id
        ) VALUES (?, ?)
    ''', (
        search_text,
        user_id
    ))
    conn.commit()


async def get_sql_kp_post(conn: tp.Any, kinopoisk_id: str, user_id: str) -> list[dict[tp.Any, tp.Any]]:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM kp_post WHERE kinopoisk_id = ? AND user_id = ?", (kinopoisk_id, user_id))
    search_rows = cursor.fetchall()
    result = [
        {
            'id': row[0],
            'name': row[1],
            'alternativeName': row[2],
            'year': row[3],
            'country': row[4],
            'imdb_rating': row[5],
            'kp_rating': row[6],
            'runtime': row[7],
            'description': row[8],
            'genres': row[9],
            'poster_url': row[10],
            'kp_url': row[11],
            'imdb_url': row[12],
            'user_id': row[13],
            'timestamp': row[14]
         }
        for row in search_rows
    ]
    return result


async def get_sql_kp_search(conn: tp.Any, search_text: str, user_id: str) -> list[dict[tp.Any, tp.Any]]:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM kp_search WHERE search_text = ? AND user_id = ?", (search_text, user_id))
    search_rows = cursor.fetchall()
    result = [
        {
            'search_text': row[0],
            'id': row[1],
            'name': row[2],
            'year': row[3],
            'user_id': row[4],
            'timestamp': row[5]
        }
        for row in search_rows
    ]
    return result


async def parse_kp_requests(kp_requests: list[dict[tp.Any, tp.Any]]) -> list[dict[tp.Any, tp.Any]]:
    result = [
        {
            'id': kp_post_request.get('id'),
            'name': kp_post_request.get('name', 'N/A'),
            'alternativeName': kp_post_request.get('alternativeName') or 'N/A',
            'year': kp_post_request.get('year', 'N/A'),
            'country': kp_post_request.get('countries', [{'name': 'N/A'}])[0]['name'],
            'imdb_rating': kp_post_request.get('rating', {}).get('imdb', 'N/A'),
            'kp_rating': kp_post_request.get('rating', {}).get('kp', 'N/A'),
            'runtime': kp_post_request.get('movieLength', 'N/A'),
            'description': kp_post_request.get('description', 'N/A'),
            'genres': ', '.join([genre['name'] for genre in kp_post_request.get('genres', [])]),
            'poster_url': kp_post_request.get('poster', {}).get('url'),
            'kp_url': f"https://www.kinopoisk.ru/film/{kp_post_request['id']}/" if kp_post_request.get('id') else 'N/A',
            'imdb_url': f"https://www.imdb.com/title/{kp_post_request['externalId'].get('imdb')}/" or 'N/A'
        }
        for kp_post_request in kp_requests
    ]
    return result


async def get_request_kp_api(conn: tp.Any, query: str, user_id: str, timestamp: str) -> list[dict[tp.Any, tp.Any]]:
    headers = {
        "X-API-KEY": API_KEY,
        "accept": "application/json"
    }
    await append_sql_kp_count(conn, query, user_id)
    search_rows = await get_sql_kp_search(conn, query, user_id)
    if not search_rows:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://api.kinopoisk.dev/v1.4/movie/search?query={query}&page=1&limit=5',
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    movies = await parse_kp_requests(data.get('docs', []))
                    await append_sql_kp_search(conn, query, movies, user_id, timestamp)
                    await append_sql_kp_post(conn, movies, user_id, timestamp)

                    return movies
                else:
                    return []
    else:
        await append_sql_kp_search(conn, query, search_rows, user_id, timestamp)
        return search_rows


async def kp_search_message(searches: list[dict[tp.Any, tp.Any]]) -> tp.Any:
    if not searches:
        return 'Ничего не найдено :(', None
    buttons = []
    for movie in searches:
        button_text = f"{movie['name']} ({movie['year']})"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f'movie_{movie['id']}')])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return 'Нашлись эти фильмы, кликни на тот, что ты искал :)', keyboard


async def kp_post_message(movie: dict[str, str]) -> tp.Any:
    if not movie:
        return 'Информация не найдена :(', None, None
    response_text = (
        f"*Title*: {movie['name']} ({movie['alternativeName']})\n"
        f"*Genres*: {movie['genres']}\n"
        f"*Year*: {movie['year']}\n"
        f"*Country*: {movie['country']}\n"
        f"*IMDB Rating*: {movie['imdb_rating']}\n"
        f"*Kinopoisk Rating*: {movie['kp_rating']}\n"
        f"*Runtime*: {movie['runtime']} min\n"
        f"*Description*: {movie['description']}\n"
        f"*IMDB*: {movie['imdb_url']}\n"
        f"*Kinopoisk*: {movie['kp_url']}"
    )
    poster_url = movie['poster_url'] or None
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text='Показать ссылки для бесплатного просмотра :)',
                callback_data=f'google_{movie['id']}'
            )
        ]]
    )
    return response_text, keyboard, poster_url

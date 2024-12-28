import hashlib
import os
import sqlite3
import asyncio
import logging
import typing as tp

from kp_search_functions import create_sql_kp_search, create_sql_kp_post, get_sql_kp_post, \
    get_request_kp_api, kp_search_message, kp_post_message, create_sql_kp_count
from google_search_functions import create_sql_google_search, get_request_google_api, google_search_message

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from aiogram.types import CallbackQuery

logging.basicConfig(level=logging.INFO)
bot = Bot(token=os.environ['TELEGRAM_TOKEN'])
API_KEY = 'NZX33MA-VHB4DAE-HZWYBGY-E4RHH8B'
dp = Dispatcher()

conn = sqlite3.connect("movies.db")
create_sql_kp_search(conn)
create_sql_kp_post(conn)
create_sql_google_search(conn)

create_sql_kp_count(conn)


@dp.message(Command('start'))
async def start(message: types.Message) -> None:
    await message.reply("Стартуем!")


@dp.message(Command('help'))
async def help(message: types.Message) -> None:
    await message.reply(
        "Это бот для поиска бесплатных фильмов.\n\
Просто напиши название фильма, выбери нужный вариант из предложенных.\n\
Изучи информацию о нем и при необходимости перейди к поиску бесплатных ссылок для просмотра.\n\
Команда /history поможет вспомнить тебе твои ставые запросы в бота\n\
Команда /stats покажет количество сделанных тобой зарпосов в бота.\n\n\
Автор @rey_arzhan, сделано в рамках БДЗ3 по курсу питона в ШАДе"
    )


@dp.message(Command('history'))
async def history(message: types.Message) -> None:
    if message.from_user:
        user_id = hashlib.sha256(str(message.from_user.id).encode()).hexdigest()
    else:
        user_id = 'undef'
    cursor = conn.cursor()
    cursor.execute('''
        SELECT search_text, MAX(timestamp) as last_searched
        FROM kp_search
        WHERE user_id = ?
        GROUP BY search_text
        ORDER BY last_searched DESC
        LIMIT 10
    ''', (user_id,))
    results = cursor.fetchall()

    if results:
        history_message = "Последние заданные в бот запросы:\n"
        for i, (search_text, timestamp) in enumerate(results, 1):
            history_message += f"*{i}. {search_text}* (Время: {timestamp})\n"
    else:
        history_message = "Пока нет запросов в бота"
    await message.reply(history_message, parse_mode="Markdown")


@dp.message(Command('stats'))
async def stats(message: types.Message) -> None:
    if message.from_user:
        user_id = hashlib.sha256(str(message.from_user.id).encode()).hexdigest()
    else:
        user_id = 'undef'
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(DISTINCT(search_text)) as cnt
        FROM kp_search
        WHERE user_id = ?
    ''', (user_id,))
    bot_cnt = cursor.fetchall()[0][0]

    cursor.execute('''
        SELECT COUNT(DISTINCT(kinopoisk_id)) as cnt
        FROM google_search
        WHERE user_id = ?
    ''', (user_id,))
    google_cnt = cursor.fetchall()[0][0]

    cursor.execute('''
        SELECT search_text, count(*) as cnt
        FROM kp_count
        WHERE user_id = ?
        GROUP BY search_text
        ORDER BY cnt DESC
        LIMIT 10
    ''', (user_id,))
    movie_cnt = cursor.fetchall()
    if movie_cnt and movie_cnt[0][0]:
        stat_message = 'Ваши наиболее частые запросы в бота:\n'
        for i, (search_text, cnt) in enumerate(movie_cnt, 1):
            stat_message += f"*{i}. {search_text}* (Кол-во запросов: {cnt})\n"
        stat_message += f'\n\n\
За все время вы сделали *{bot_cnt}* зарпосов поиска фильма \
и *{google_cnt}* запросов для поиска бесплатных ссылок\n\n\
На всех пользователей есть огранчение на *200* зарпосов поиска фильма \
и *100* запросов для поиска бесплатных ссылок в день.\
\nНе спамьте плиз :/'
    else:
        stat_message = 'Пока не было запросов в бота'
    await message.reply(stat_message, parse_mode="Markdown")


@dp.message()
async def search_and_show_results(message: tp.Any) -> None:
    if message.from_user:
        user_id = hashlib.sha256(str(message.from_user.id).encode()).hexdigest()
    else:
        user_id = 'undef'
    if message:
        timestamp = str(message.date)
    else:
        timestamp = ''
    movies_data = await get_request_kp_api(conn, message.text.strip(), user_id, timestamp)
    text, keyboard = await kp_search_message(movies_data)
    if keyboard is None:
        await message.reply(text)
    else:
        await message.reply(text, reply_markup=keyboard)


@dp.callback_query()
async def handle_buttons(callback_query: CallbackQuery) -> None:
    if callback_query.from_user:
        user_id = hashlib.sha256(str(callback_query.from_user.id).encode()).hexdigest()
    else:
        user_id = 'undef'
    if callback_query.message:
        timestamp = str(callback_query.message.date)
    else:
        timestamp = ''
    if callback_query.data:
        type, message = callback_query.data.split('_')
    else:
        type, message = '', ''
    if type == 'movie':
        movie_rows = await get_sql_kp_post(conn, message, user_id)
        movie = movie_rows[0] if movie_rows else {}
        text, keyboard, poster_url = await kp_post_message(movie)
        if keyboard:
            if poster_url:
                await bot.send_photo(
                    chat_id=callback_query.message.chat.id if callback_query.message else 0,
                    photo=poster_url,
                    caption=text,
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )
            else:
                await bot.send_message(
                    chat_id=callback_query.message.chat.id if callback_query.message else 0,
                    text=text,
                    disable_web_page_preview=True,
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )
            await callback_query.answer()
        else:
            await callback_query.answer(text)

    if type == 'google':
        search_data = await get_request_google_api(conn, message, user_id, timestamp)
        text = await google_search_message(search_data)
        await bot.send_message(
            chat_id=callback_query.message.chat.id if callback_query.message else 0,
            text=text,
            disable_web_page_preview=True,
            parse_mode="Markdown"
        )


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())

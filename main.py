import logging
import os

import psycopg2

from aiogram.utils.executor import start_webhook
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, ContentType

import threading
import asyncio

import time


from psycopg2 import sql

from name import *

TOKEN = '969792461:AAG9ctwOj9ONUK5enPRFOPCwXOU4m1l913M'

WEBHOOK_HOST = 'https://random-friend-bot.herokuapp.com'  # name your app
WEBHOOK_PATH = '/webhook/'
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

WEBAPP_HOST = '0.0.0.0'
WEBAPP_PORT = os.environ.get('PORT')

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

heroku_start = True

conn = psycopg2.connect(dbname='mydb', user='myuser',
                        password='12345678', host='mydb.clmg1sgw6zpf.eu-west-3.rds.amazonaws.com')


async def timer_logic():
    # t0 = time.time()
    data=await get_all_data()
    for user in data:
        # print(user[0])
        find_data=await search_data(user[0],user[1])
        if find_data is not None:

            await update_data(user[0], 'STATE', 0)
            await update_data(user[0], 'ID_FRIEND', find_data[0][0])

            button = KeyboardButton(B_CLOSE)
            kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button)
            if user[1] == 1:
                await bot.send_message(user[0], T_FIND_BOY, reply_markup=kb)
                await bot.send_message(user[0], 'Описание: {}'.format(find_data[0][1]))
            else:
                await bot.send_message(user[0], T_FIND_GIRL, reply_markup=kb)
                await bot.send_message(user[0], 'Описание: {}'.format(find_data[0][1]))


            await update_data(find_data[0][0], 'STATE', 0)
            await update_data(find_data[0][0], 'ID_FRIEND', user[0])

            if user[1] == 1:
                await bot.send_message(find_data[0][0], T_FIND_GIRL, reply_markup=kb)
                await bot.send_message(find_data[0][0], 'Описание: {}'.format(user[2]))
            else:
                await bot.send_message(user[0], T_FIND_BOY, reply_markup=kb)
                await bot.send_message(user[0], 'Описание: {}'.format(find_data[0][1]))

    #     print(find_data)
    # print(len(data))
    #
    #
    # print(time.time()-t0)

def timer_start():
    threading.Timer(300.0, timer_start).start()

    try:
        asyncio.run_coroutine_threadsafe(timer_logic(), bot.loop)
    except Exception as exc:
        print(f'The coroutine raised an exception: {exc!r}')


async def get_data(message):
    try:

        with conn.cursor() as cursor:
            conn.autocommit = True

            data = sql.SQL('Select *  from users where id_user={}'
                           .format(message.chat.id))

            cursor.execute(data)
            data_user = cursor.fetchall()
            # print(data_user[0][6])
        if len(data_user) > 0:
            return data_user
        else:
            return None
    except Exception as ex:
        # print(ex)
        # await bot.send_message(message.chat.id, 'Что-то пошло не так. Мы работаем над данной проблемой.')
        return None

async def get_all_data():
    try:

        with conn.cursor() as cursor:
            conn.autocommit = True

            data = sql.SQL('Select id_user,sex,description from users where id_friend=1')

            cursor.execute(data)
            data_user = cursor.fetchall()

        if len(data_user) > 0:
            return data_user
        else:
            return None
    except Exception as ex:
        print(ex)

        return None


async def search_data(id,sex):
    try:

        with conn.cursor() as cursor:
            conn.autocommit = True

            data = sql.SQL('Select S.id_user, S.description from users S where distance3(S.latitude,S.longitude,(Select latitude from users where id_user={} ),(Select longitude from users where id_user={} ))<100 AND S.sex!={} AND S.id_friend=1 Order by RANDOM() LIMIT 1'
                           .format(id,id,sex))

            cursor.execute(data)
            data_user = cursor.fetchall()
            # print(data_user)
        if len(data_user)>0:
            return data_user
        else:
            return None
    except Exception as ex:
        print(ex)
        return None

async def update_data(id,set,item):
    try:

        with conn.cursor() as cursor:
            conn.autocommit = True

            update = sql.SQL('UPDATE users SET {} = {} WHERE id_user={}'
                             .format(set,item,id))

            cursor.execute(update)

    except Exception as ex:
        print(ex)
        await bot.send_sticker(id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(id, 'Что-то пошло не так. ')

@dp.message_handler(commands='start')
async def welcome(message: types.Message):
    button = KeyboardButton(B_REGISTRER)
    button.request_location = True
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button)

    await bot.send_sticker(message.chat.id, 'CAADAgADyCIAAulVBRgZbm0FEwO_9hYE')
    await bot.send_message(
        message.chat.id,
        T_HELLO.format(message.chat.first_name),
        reply_markup=kb)

@dp.message_handler(content_types=ContentType.LOCATION)
async def registration(message: types.Message):
    # t0 = time.time()
    data = await get_data(message)
    # print(data)
    if data is not None:
        state = data[0][7]
        if state==8:
            await update_data(message.chat.id, 'latitude', message.location.latitude)
            await update_data(message.chat.id, 'longitude', message.location.longitude)
            await bot.send_sticker(message.chat.id, 'CAADAgADvSIAAulVBRjK14yseIAdlRYE')
            await bot.send_message(message.chat.id,T_GEO_SUCC)

    else:
        try:

            with conn.cursor() as cursor:
                conn.autocommit = True

                insert = sql.SQL('INSERT INTO users (id_user,latitude, longitude,state ) VALUES ({},{},{},{})'
                                 .format(message.chat.id, message.location.latitude, message.location.longitude, 1))

                cursor.execute(insert)
            button = KeyboardButton(B_MAN)
            button2=KeyboardButton(B_WOMAN)
            kb = ReplyKeyboardMarkup(resize_keyboard=True).row(button,button2)
            await bot.send_message(message.chat.id, T_REG ,reply_markup=kb)
            await bot.send_sticker(message.chat.id, 'CAADAgADFgIAAjbsGwUrH3k-y6Vv9hYE')
        except Exception as ex:
            # print('err'+ex)
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы уже зарегистрированы.')

    # t1 = time.time()
    # print(t1 - t0)


@dp.message_handler()
async def main_logic(message: types.Message):
    # t0 = time.time()

    data = await get_data(message)
    if data is not None:
        state=data[0][7]
        id_friend=data[0][6]
        description_you=data[0][2]
        sex=data[0][5]
        # print(data)
        if state == 1:

            if message.text==B_MAN:
                await update_data(message.chat.id,'SEX',1)
                await update_data(message.chat.id, 'STATE', 2)
                button = KeyboardButton(B_HELP)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button)
                await bot.send_message(message.chat.id, T_DESCR, reply_markup=kb)
                await bot.send_sticker(message.chat.id, 'CAADAgADEwEAAjbsGwXj2-2f4C8x4BYE')

            if message.text == B_WOMAN:
                await update_data(message.chat.id,'SEX', 0)
                await update_data(message.chat.id, 'STATE', 2)
                button = KeyboardButton(B_HELP)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button)
                await bot.send_message(message.chat.id, T_DESCR, reply_markup=kb)
                await bot.send_sticker(message.chat.id, 'CAADAgADEwEAAjbsGwXj2-2f4C8x4BYE')
        if state == 2:
            if message.text == B_HELP:
                await bot.send_sticker(message.chat.id, 'CAADAgADdQEAAjbsGwWMu47DIo7j_RYE')
                await bot.send_message(message.chat.id, T_HELP_DESC)
            else:
                await update_data(message.chat.id,'DESCRIPTION','\''+message.text+'\'')
                await update_data(message.chat.id, 'STATE', 9)
                button = KeyboardButton(B_SEARCH)
                button2 = KeyboardButton(B_SETTING)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                await bot.send_sticker(message.chat.id, 'CAADAgADwCIAAulVBRgzpeElWvFn2RYE')
                await bot.send_message(message.chat.id, T_SUCCESS, reply_markup=kb)
        if state == 0:
            if message.text == B_CLOSE:
                await update_data(message.chat.id, 'STATE', 9)
                await update_data(message.chat.id, 'ID_FRIEND', 0)
                button = KeyboardButton(B_SEARCH)
                button2 = KeyboardButton(B_SETTING)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                await bot.send_sticker(message.chat.id, 'CAADAgADYwEAAjbsGwXkTe2zgRvwWBYE')
                await bot.send_message(message.chat.id, T_CLOSE_CHAT, reply_markup=kb)


                await update_data(id_friend, 'STATE', 9)
                await update_data(id_friend, 'ID_FRIEND', 0)
                await bot.send_sticker(id_friend, 'CAADAgADYwEAAjbsGwXkTe2zgRvwWBYE')
                await bot.send_message(id_friend, T_CLOSE_CHAT, reply_markup=kb)
            else:
                await bot.send_message(id_friend, message.text)
        if state == 9:
            if message.text == B_SEARCH:
                data_find=await search_data(message.chat.id,sex)

                if data_find is not None:
                    id_find = data_find[0][0]
                    description = data_find[0][1]
                    await update_data(message.chat.id, 'STATE', 0)
                    await update_data(message.chat.id, 'ID_FRIEND', id_find)

                    button = KeyboardButton(B_CLOSE)
                    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button)
                    if sex==1:
                        await bot.send_message(message.chat.id, T_FIND_GIRL, reply_markup=kb)
                        await bot.send_message(message.chat.id, 'Описание: {}'.format(description))
                    else:
                        await bot.send_message(message.chat.id, T_FIND_BOY, reply_markup=kb)
                        await bot.send_message(message.chat.id, 'Описание: {}'.format(description))

                    await update_data(id_find, 'STATE', 0)
                    await update_data(id_find, 'ID_FRIEND', message.chat.id)
                    if sex == 1:
                        await bot.send_message(id_find, T_FIND_BOY, reply_markup=kb)
                        await bot.send_message(id_find, 'Описание: {}'.format(description_you))
                    else:
                        await bot.send_message(id_find, T_FIND_GIRL, reply_markup=kb)
                        await bot.send_message(id_find, 'Описание: {}'.format(description_you))
                else:
                    await update_data(message.chat.id,'ID_FRIEND',1)
                    await update_data(message.chat.id, 'STATE', 6)
                    button = KeyboardButton(B_STOP)
                    button2 = KeyboardButton(B_SETTING)
                    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                    await bot.send_message(message.chat.id, T_STAY_FIND, reply_markup=kb)
            if message.text == B_SETTING:
                await update_data(message.chat.id, 'STATE', 8)
                button = KeyboardButton(B_GEO)
                button.request_location = True
                button2 = KeyboardButton(B_DESCRIPTION)
                button3 = KeyboardButton(B_INFO)
                button4 = KeyboardButton(B_BACK)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2).add(button3).add(button4)
                await bot.send_sticker(message.chat.id, 'CAADAgADuSIAAulVBRioU39rZen8LRYE')
                await bot.send_message(message.chat.id, T_SETTING, reply_markup=kb)
        if state == 6:
            if message.text == B_STOP:
                await update_data(message.chat.id, 'STATE', 9)
                await update_data(message.chat.id, 'ID_FRIEND', 0)
                button = KeyboardButton(B_SEARCH)
                button2 = KeyboardButton(B_SETTING)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                await bot.send_message(message.chat.id, T_STOP, reply_markup=kb)
            if message.text == B_SETTING:
                await update_data(message.chat.id, 'STATE', 8)
                button = KeyboardButton(B_GEO)
                button.request_location = True
                button2 = KeyboardButton(B_DESCRIPTION)
                button3 = KeyboardButton(B_INFO)
                button4 = KeyboardButton(B_BACK)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2).add(button3).add(button4)
                await bot.send_sticker(message.chat.id, 'CAADAgADuSIAAulVBRioU39rZen8LRYE')
                await bot.send_message(message.chat.id, T_SETTING, reply_markup=kb)

        if state == 8:
            if message.text == B_GEO:
                pass
            if message.text == B_DESCRIPTION:
                await update_data(message.chat.id, 'STATE', 7)
                button = KeyboardButton(B_HELP)
                button2 = KeyboardButton(B_BACK)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                await bot.send_message(message.chat.id, T_DESCR_CHANGE, reply_markup=kb)
            if message.text == B_INFO:
                await bot.send_sticker(message.chat.id, 'CAADAgADwiIAAulVBRhBZgUAAYnNXoUWBA')
                await bot.send_message(message.chat.id, T_INFO)
            if message.text == B_BACK:
                if id_friend==1:
                    await update_data(message.chat.id, 'STATE', 6)
                    button = KeyboardButton(B_STOP)
                    button2 = KeyboardButton(B_SETTING)
                    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                    await bot.send_message(message.chat.id, T_BACK_MAIN, reply_markup=kb)
                else:
                    await update_data(message.chat.id, 'STATE', 9)
                    button = KeyboardButton(B_SEARCH)
                    button2 = KeyboardButton(B_SETTING)
                    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2)
                    await bot.send_message(message.chat.id, T_BACK_MAIN, reply_markup=kb)
        if state == 7:
            if message.text == B_BACK:
                await update_data(message.chat.id, 'STATE', 8)
                button = KeyboardButton(B_GEO)
                button.request_location = True
                button2 = KeyboardButton(B_DESCRIPTION)
                button3 = KeyboardButton(B_INFO)
                button4 = KeyboardButton(B_BACK)
                kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2).add(button3).add(button4)
                await bot.send_message(message.chat.id,T_BACK_DESC, reply_markup=kb)

            else:
                if message.text == B_HELP:
                    await bot.send_sticker(message.chat.id, 'CAADAgADdQEAAjbsGwWMu47DIo7j_RYE')
                    await bot.send_message(message.chat.id, T_HELP_DESC)
                else:
                    await update_data(message.chat.id, 'STATE', 8)
                    await update_data(message.chat.id, 'DESCRIPTION','\''+message.text+'\'')
                    await bot.send_sticker(message.chat.id, 'CAADAgADvSIAAulVBRjK14yseIAdlRYE')
                    button = KeyboardButton(B_GEO)
                    button.request_location = True
                    button2 = KeyboardButton(B_DESCRIPTION)
                    button3 = KeyboardButton(B_INFO)
                    button4 = KeyboardButton(B_BACK)
                    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(button).add(button2).add(button3).add(button4)
                    await bot.send_message(message.chat.id, T_DESCR_SUCC, reply_markup=kb)



    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Скорее всего вы не прошли регистрацию.')

    # t1 = time.time()
    # print(t1 - t0)


async def on_startup(dp):
    await bot.set_webhook(WEBHOOK_URL)
    conn = psycopg2.connect(dbname='mydb', user='myuser',
                            password='12345678', host='mydb.clmg1sgw6zpf.eu-west-3.rds.amazonaws.com')


async def on_shutdown(dp):
    # insert code here to run it before shutdown
    pass

@dp.message_handler(content_types=ContentType.STICKER)
async def sticker(message: types.sticker):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state == 0:
            await bot.send_sticker(id_friend, message.sticker.file_id)
        else:
            # print(message.sticker.file_id)
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        # print(message.sticker.file_id)
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')


@dp.message_handler(content_types=ContentType.PHOTO)
async def photo(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_photo(id_friend, message.photo[-1].file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')

@dp.message_handler(content_types=ContentType.VIDEO)
async def video(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_video(id_friend, message.video.file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')

@dp.message_handler(content_types=ContentType.DOCUMENT)
async def document(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_document(id_friend, message.document.file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')

@dp.message_handler(content_types=ContentType.AUDIO)
async def audio(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_audio(id_friend, message.audio.file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')

@dp.message_handler(content_types=ContentType.VOICE)
async def voice(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_voice(id_friend, message.voice.file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')

@dp.message_handler(content_types=ContentType.ANIMATION)
async def gif(message: types.Message):

    data = await get_data(message)
    if data is not None:
        id_friend = data[0][6]
        state = data[0][7]
        if state==0:
            await bot.send_animation(id_friend, message.animation.file_id)
        else:
            await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
            await bot.send_message(message.chat.id, 'Что-то пошло не так.')
    else:
        await bot.send_sticker(message.chat.id, 'CAADAgADsSIAAulVBRjeeCmOKBdM4RYE')
        await bot.send_message(message.chat.id, 'Что-то пошло не так. Возможно вы не зарегистрированы.')




if __name__ == '__main__':
    timer_start()

    if heroku_start:
        start_webhook(dispatcher=dp, webhook_path=WEBHOOK_PATH,
                      on_startup=on_startup, on_shutdown=on_shutdown,
                      host=WEBAPP_HOST, port=WEBAPP_PORT)
    else:
        executor.start_polling(dp, skip_updates=True)

from aiogram import Bot, Dispatcher
import asyncio
import logging
import sys
import re
import ydb
import uuid
from aiogram.fsm.state import StatesGroup, State
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram import types
from aiogram.fsm.context import FSMContext
from datetime import datetime
bot = Bot(token='7321982938:AAHrIf2Cuj0s0_6YxFCLcNeSJdvXYQ63MyU')
dp = Dispatcher()
class UserStates(StatesGroup):
    creating_tranaction = State()
    standart_state = State()
    searching_tranactions = State()

driver = ydb.Driver(
  endpoint='grpcs://ydb.serverless.yandexcloud.net:2135',
  database='/ru-central1/b1godpkhv4bhrc2ev0pn/etnu19lcc89d3ftuvqt8',
  credentials=ydb.AuthTokenCredentials('t1.9euelZqQk5qWjZ6amJOSjZGLnJuXmO3rnpWay8mWmZSYzpuJjciJkp2TjIzl8_dwEGBM-e8XGBU9_N3z9zA_XUz57xcYFT38zef1656Vmp6NzYqJi5aMnM2Lyc6OlcjL7_zF656Vmp6NzYqJi5aMnM2Lyc6OlcjL.YL4_b9k8l7ht8GsxZuKAoo8rodItRoSG3WHKrX2widnMacarkIy-1AOF4OYu6t7hE9fEHxl9zNisjJp8OHoaBg'),
)

driver.wait(fail_fast=True, timeout=5)

pool = ydb.SessionPool(driver)
def upsert_transaction(session, path, unique_id, category, value, commentary, user_id, month, year):
    session.transaction().execute(
        """
        PRAGMA TablePathPrefix("{}");
        UPSERT INTO transactions (id, category, value, commentary, user_id, month, year) VALUES
            ({}, '{}', {}, '{}', {}, {}, {});
        """.format(path, unique_id, category, value, commentary, user_id, month, year),
        commit_tx=True,
    )

def execute_upsert(path, unique_id, category, value, commentary, user_id, month, year):
    def run(session):
        upsert_transaction(session, path, unique_id, category, value, commentary, user_id, month, year)
    pool.retry_operation_sync(run)






def select_transactions(session, path, arr, user_id):


    if arr[1] != "_":
        arr[1] = int(arr[1])
    if arr[2] != "_":
        arr[2] = int(arr[2])
    for i in range(len(arr)):
        if isinstance(arr[i], str):
            arr[i] = f"'{arr[i]}'"
    keys = ['category', 'month', 'year']
    query = """
        PRAGMA TablePathPrefix("{}");
        SELECT * FROM transactions WHERE user_id = {}
        """.format(path,user_id)
    for i in range(len(keys)):
        if arr[i] != "'_'":
            query = query + ' AND ' + str(keys[i]) + ' = ' + str(arr[i])
    query += ';'


    result = session.transaction().execute(query, commit_tx=True)
    return result, query

def execute_select(path, arr, user_id):
    def run(session):
        result = select_transactions(session, path, arr, user_id)
        return result

    return pool.retry_operation_sync(run)






@dp.message(CommandStart())
async def command_start_handler(message: types.Message) -> None:
    await message.answer("Теперь вы можете добавлять и искать транзакции\n Используйте команду /help чтобы помощь")

@dp.message(Command('help'))
async def command_help_handler(message: types.Message) -> None:
    await message.answer('Чтобы создать транзакцию используйте команду "/create", и после ответа от бота отправьте описание транзакции в формате:\n*имя_катеории*.*цена_транзакции*.*комментарий*.*месяц совершения транзакции*.*год совершения транзакции*\nПример: Еда.2500.сходил в ресторан.6.2024\nМесяц и год можно не писать, тогда будет использована текущая дата\nПример: Еда.2500.сходил в ресторан\nЧтобы найти транзакции используйте команду "/search", и после ответа от бота отправьте описание транзакций в формате: *имя_катеории*.*месяц совершения*.*год совершения*.*тип вывода - список или сумма*\nПример: еда.5.2024.сумма')

@dp.message(Command('create'))
async def initiate_creation(message: types.Message, state: FSMContext) -> None:
    await state.set_state(UserStates.creating_tranaction)
    await message.answer('Отправьте описание транзакции')

@dp.message(UserStates.creating_tranaction)
async def create_transaction(message: types.Message, state: FSMContext) -> None:
    pattern = r'^.{1,30}\.\d{1,9}\..{0,140}\.\d{1,2}\.\d{1,9}$'
    dateless_pattern = r'^.{1,30}\.\d{1,9}\..{0,140}$'
    if message.text == '/cancel':
        await state.set_state(UserStates.standart_state)
        return
    if re.match(pattern, message.text):
        pass
    elif re.match(dateless_pattern, message.text) and not re.search(r'\.\w+$', message.text):
        pass
    else:
        await message.answer('Вы неправильно описали транзакцию, бот умер от кринжа, попытайтесь снова')
        await state.set_state(UserStates.standart_state)
        return
    arr = message.text.lower().split('.')
    if len(arr) < 5:
        arr.append(datetime.now().date().month)
        arr.append(datetime.now().date().year)
    execute_upsert('/ru-central1/b1godpkhv4bhrc2ev0pn/etnu19lcc89d3ftuvqt8', uuid.uuid4().int & (1 << 64) - 1, arr[0], arr[1], arr[2], message.from_user.id, int(arr[3]), int(arr[4]))
    await message.answer('Транзакция добавлена')
    await state.set_state(UserStates.standart_state)







@dp.message(Command('search'))
async def initiate_search(message: types.Message, state: FSMContext) -> None:
    await state.set_state(UserStates.searching_tranactions)
    await message.answer('Отправьте описание транзакции')


@dp.message(UserStates.searching_tranactions)
async def find_tranaction(message: types.Message, state: FSMContext) -> None:
    pattern = r'^[^.\n]{1,30}\.(?:\d{1,2}|нет)\.(?:\d{1,6}|нет)\.(список|сумма)$'
    if message.text == '/cancel':
        await state.set_state(UserStates.standart_state)
        return

    if re.match(pattern, message.text):
        pass
    else:
        await message.answer('Вы неправильно описали шаблон поиска, бот умер от кринжа, попытайтесь снова')
        await state.set_state(UserStates.standart_state)
        return
    arr = message.text.lower().replace('нет', '_').split('.')
    ans_type = arr[-1]
    user_id = message.from_user.id
    listik, query = execute_select('/ru-central1/b1godpkhv4bhrc2ev0pn/etnu19lcc89d3ftuvqt8', arr, user_id)
    listik = listik[0].rows

    if arr[3] == "'список'":
        for i in listik:
            await message.answer("Категория - {}\nМесяц - {}\nСумма - {}.\nКомментарий - {}".format(
            i['category'],
                str(i['month']) + '.' + str(i['year']),
                i['value'],
                i['commentary']
            ))

    elif  arr[3] == "'сумма'":
        summ = 0

        for i in listik:
            summ += int(i['value'])

        ans = 'Сумарные расходы'
        if arr[0] != "'_'":
            ans = ans + ' в категории ' + arr[0]
        if arr[1] != "'_'":
            ans = ans + ' за ' + arr[1]
        if arr[2] != "'_'":
            ans = ans + '.' + arr[2]
        ans = ans + ' составляют ' + str(summ)
        await message.answer(ans)
    await state.set_state(UserStates.standart_state)






async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
from pymongo import MongoClient
from aiogram import Bot, Dispatcher
import asyncio
import logging
import sys
from os import getenv
from aiogram.fsm.state import StatesGroup, State
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram import types
from aiogram.fsm.context import FSMContext
from datetime import datetime
client = MongoClient('mongodb://localhost:27017/')
TOKEN = "7494194851:AAG2rD_yQL03ytHTUxwUzIU80gS_V-suaPk"
dp = Dispatcher()

db = client['mydatabase']

collection = db['transactions']
class UserStates(StatesGroup):
    creating_tranaction = State()
    standart_state = State()
    searching_tranactions = State()





@dp.message(CommandStart())
async def command_start_handler(message: types.Message) -> None:
    await message.answer("Теперь вы можете добавлять транзакции")


@dp.message(Command('create'))
async def initiate_creation(message: types.Message, state: FSMContext) -> None:
    await state.set_state(UserStates.creating_tranaction)
    await message.answer('Отправьте описание транзакции в формате: *имя_катеории*.*цена_транзакции*.*комментарий* Пример: Медицина.2500.сдавал кал на анализ')

@dp.message(UserStates.creating_tranaction)
async def create_transaction(message: types.Message, state: FSMContext) -> None:
    if message.text == '/cancel':
        await state.set_state(UserStates.standart_state)
        return
    arr = message.text.lower().split('.')
    collection.insert_one({
        'month': datetime.now().date().month,
        'year': datetime.now().date().year,
        'user_id': message.from_user.id,
        'category': arr[0],
        'value': arr[1],
        'commentary': arr[2]
    })
    await state.set_state(UserStates.standart_state)







@dp.message(Command('search'))
async def initiate_search(message: types.Message, state: FSMContext) -> None:
    await state.set_state(UserStates.searching_tranactions)
    await message.answer('Отправьте описание транзакций в формате: *имя_катеории*.*месяц совершения*.*год совершения*.*тип вывода - список или сумма* Пример: Медицина.5.2024.сумма')


@dp.message(UserStates.searching_tranactions)
async def find_tranaction(message: types.Message, state: FSMContext) -> None:
    text = message.text.lower().replace('нет', '_')
    arr = text.split('.')
    if arr[1] != "_":
        arr[1] = int(arr[1])
    if arr[2] != "_":
        arr[2] = int(arr[2])
    keys = ['category', 'month', 'year']
    query = {'user_id':  message.from_user.id}
    for i in range(len(keys)):
        if arr[i] != "_":
            query[keys[i]] = arr[i]
    print('query',query)
    listik = collection.find(query)
    if arr[3] == 'список':
        for i in listik:
            await message.answer("Категория - {}\nМесяц - {}\nСумма - {}.\nКомментарий - {}".format(
            i['category'],
                str(i['month']) + '.' + str(i['year']),
                i['value'],
                i['commentary']
            ))

    elif  arr[3] == 'сумма':
        summ = 0

        for i in listik:
            summ += int(i['value'])

        ans = 'Сумарные расходы'
        if 'category' in query:
            ans = ans + ' в категории ' + query['category']
        if 'month' in query:
            ans = ans + ' за ' + str(query['month'])
        if 'year' in query:
            ans = ans + '.' + str(query['year'])
        ans = ans + ' составляют ' + str(summ)
        await message.answer(ans)
    await state.set_state(UserStates.standart_state)

















async def main() -> None:
    # Initialize Bot instance with default bot properties which will be passed to all API calls
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
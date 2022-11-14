from aiohttp import web
import json
import asyncio
from sqlalchemy import create_engine, insert, select, update, delete, and_, Table, Column, String, MetaData, Integer, Date, ForeignKey
import psycopg2
from aiohttp_basicauth import BasicAuthMiddleware


async def handle(request):
    response_obj = {'status': 'success'}
    return web.Response(text=json.dumps(response_obj), status=200)


async def insert_db(name, surname, login, password, bday, permission):
    engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
    ins = insert(users)
    conn = engine.connect()
    r = conn.execute(ins,
                     name=name,
                     surname=surname,
                     login=login,
                     password=password,
                     birthday=bday,
                     permission=permission)
    return r


async def insert_func(request):
    name = request.query['name']
    surname = request.query['surname']
    login = request.query['login']
    password = request.query['password']
    bday = request.query['bday']
    permission = request.query['permission']
    r = asyncio.get_event_loop().create_task(insert_db(name, surname, login, password, bday, permission))
    return web.Response(text=str(r), status=200)


async def select_func(request):# пробовал сделать select по-другому, все ровно ошибка с ключом
    async with request.app['Users'].acquire() as conn:
        query = select([users.c.name])
        result = await conn.fetch(query)
    return web.Response(text=str(result), status=200)


async def select_db():#использовалось для функции
    engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
    conn = engine.connect()
    s = select(users)
    r = conn.execute(s)
    return r


async def update_db(name, surname, login, password, bday, permission, id):
    engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
    conn = engine.connect()
    s = update(users).where(
        users.c.id == id
    ).values(
        name=name,
        surname=surname,
        login=login,
        password=password,
        birthday=bday,
        permission=permission
    )
    r = conn.execute(s)
    return r


async def update_func(request):
    name = request.query['name']
    surname = request.query['surname']
    login = request.query['login']
    password = request.query['password']
    bday = request.query['bday']
    permission = request.query['permission']
    id = request.query['id']
    r = asyncio.get_event_loop().create_task(update_db(name, surname, login, password, bday, permission, id))
    return web.Response(text=str(r), status=200)


async def delete_db(id):
    engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
    conn = engine.connect()
    s = delete(users).where(
        users.c.id == id
    )
    r = conn.execute(s)
    return r


async def user_check(login, password):
    engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
    conn = engine.connect()
    s = select(users.c.login,
               users.c.password,
               users.c.id).select_from(users).where(and_(
        users.c.login == login,
        users.c.password == password
    ))
    r = conn.execute(s)
    return r


async def delete_func(request):
    id = request.query['id']
    r = asyncio.get_event_loop().create_task(delete_db(id))
    return web.Response(text=str(r), status=200)


class CustomBasicAuth(BasicAuthMiddleware):
    async def check_credentials(self, login, password, request):
        engine = create_engine("postgresql+psycopg2://postgres:1919@localhost/Sima")
        conn = engine.connect()
        s = select(users.c.login,
                   users.c.password,
                   users.c.permission).select_from(users).where(and_(
            users.c.login == login,
            users.c.password == password
        ))
        r = conn.execute(s)
        return login == 'admin' and password == 'admin' # тут должны возвращаться значения из r, но т.к. БД не работает оставил так


auth = CustomBasicAuth()
app = web.Application(middlewares=[auth])
db_string = "postgresql+psycopg2://postgres:1919@localhost/Sima"
db = create_engine(db_string)
meta = MetaData(db)# БД делал в pgadmin,но алхимия не видит таблицу users, пришлось прописывать еще раз ниже, так и не понял так нужно всегда делать или нет и нужно ли было создавать таблицы в pgadmin
users = Table('users', meta,
              Column('name', String),
              Column('surname', String),
              Column('login', String),
              Column('password', String),
              Column('birthday', Date),
              Column('permission', Integer,),
              Column('user_id', Integer, primary_key=True),
              )
permissions = Table('permissions', meta,
                    Column('permission_id', Integer, primary_key=True),
                    Column('permission', String,)
                    )

app.router.add_get('/', handle)
app.router.add_post('/insert', insert_func)
app.router.add_post('/update', update_func)
app.router.add_delete('/delete', delete_func)
app.router.add_get('/select', select_func)
web.run_app(app)

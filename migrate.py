import asyncio
from databases import Database

DATABASE_URL = "sqlite+aiosqlite:///./starwars.db"

async def migrate():
    async with Database(DATABASE_URL) as database:
        query = """
        CREATE TABLE IF NOT EXISTS characters (
            id INTEGER PRIMARY KEY,
            birth_year TEXT,
            eye_color TEXT,
            gender TEXT,
            hair_color TEXT,
            height TEXT,
            mass TEXT,
            name TEXT,
            skin_color TEXT,
            homeworld TEXT,
            films TEXT,
            species TEXT,
            starships TEXT,
            vehicles TEXT
        )
        """
        await database.execute(query)
        print("Миграция выполнена: таблица 'characters' готова к работе.")

if __name__ == "__main__":
    asyncio.run(migrate())

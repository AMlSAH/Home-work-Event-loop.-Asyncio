import asyncio
import aiohttp
from databases import Database
from tqdm.asyncio import tqdm_asyncio

BASE_URL = "https://www.swapi.tech/api/people/"
DATABASE_URL = "sqlite+aiosqlite:///./starwars.db"

SEMAPHORE = asyncio.Semaphore(10)
TIMEOUT = aiohttp.ClientTimeout(total=20, connect=5, sock_read=15)

async def fetch_person(session, person_id):
    url = f"{BASE_URL}{person_id}/"
    async with SEMAPHORE:
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                elif response.status == 404:
                    return None
                else:
                    print(f"Предупреждение: Не удалось загрузить персонажа с ID {person_id}, статус {response.status}")
                    return None
        except asyncio.TimeoutError:
            print(f"Таймаут при загрузке персонажа с ID {person_id}")
            return None
        except Exception as e:
            print(f"Ошибка при загрузке персонажа с ID {person_id}: {e}")
            return None

async def load_data_to_db():
    async with Database(DATABASE_URL) as database:
        async with aiohttp.ClientSession() as session:
            person_ids = range(1, 101)
            print("Начинаем загрузку данных о персонажах (ID с 1 по 100)...")
            tasks = [fetch_person(session, pid) for pid in person_ids]
            results = await tqdm_asyncio.gather(*tasks, desc="Загрузка из API", unit="запр")
            valid_results = [res for res in results if res is not None]
            print(f"Успешно загружено данных о {len(valid_results)} персонажах.")
            insert_query = """
            INSERT INTO characters(id, birth_year, eye_color, gender, hair_color, homeworld, mass, name, skin_color)
            VALUES (:id, :birth_year, :eye_color, :gender, :hair_color, :homeworld, :mass, :name, :skin_color)
            ON CONFLICT(id) DO UPDATE SET
                birth_year=excluded.birth_year,
                eye_color=excluded.eye_color,
                gender=excluded.gender,
                hair_color=excluded.hair_color,
                homeworld=excluded.homeworld,
                mass=excluded.mass,
                name=excluded.name,
                skin_color=excluded.skin_color;
            """
            values_list = []
            for data in valid_results:
                props = data.get('result', {}).get('properties', {})
                char_id = data.get('result', {}).get('uid')
                if not char_id:
                    continue
                values = {
                    "id": int(char_id),
                    "birth_year": props.get('birth_year'),
                    "eye_color": props.get('eye_color'),
                    "gender": props.get('gender'),
                    "hair_color": props.get('hair_color'),
                    "homeworld": props.get('homeworld'),
                    "mass": props.get('mass'),
                    "name": props.get('name'),
                    "skin_color": props.get('skin_color'),
                }
                values_list.append(values)
            if values_list:
                await database.execute_many(query=insert_query, values=values_list)
                print(f"В базу данных добавлено {len(values_list)} персонажей.")
            else:
                print("Нет данных для вставки в базу данных.")

if __name__ == "__main__":
    asyncio.run(load_data_to_db())

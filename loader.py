import asyncio
import aiohttp
from databases import Database
from tqdm.asyncio import tqdm_asyncio
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.swapi.tech/api"
DATABASE_URL = "sqlite+aiosqlite:///./starwars.db"

SEMAPHORE = asyncio.Semaphore(10)
TIMEOUT = aiohttp.ClientTimeout(total=20, connect=5, sock_read=15)


class ResourceNotFoundError(Exception):
    pass


async def fetch_with_retry(
    session: aiohttp.ClientSession, url: str, max_retries: int = 3
) -> Optional[dict]:
    for attempt in range(1, max_retries + 1):
        try:
            async with SEMAPHORE:
                async with session.get(url, timeout=TIMEOUT) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        raise ResourceNotFoundError()
                    else:
                        logger.warning(
                            f"Попытка {attempt}: статус {response.status} для {url}"
                        )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Попытка {attempt}: сетевая ошибка {e} для {url}")
        except ResourceNotFoundError:
            raise

        if attempt < max_retries:
            await asyncio.sleep(2**attempt)

    return None


async def fetch_all_person_ids(session: aiohttp.ClientSession) -> List[int]:
    person_ids = []
    page = 1

    while True:
        url = f"{BASE_URL}/people/?page={page}&limit=10"
        try:
            data = await fetch_with_retry(session, url)
        except ResourceNotFoundError:
            logger.info(f"Достигнут конец пагинации на странице {page}")
            break

        if not data:
            logger.error(f"Не удалось загрузить страницу {page}, пропускаем")
            page += 1
            continue

        for person in data.get("results", []):
            uid = person.get("uid")
            if uid:
                person_ids.append(int(uid))

        if data.get("next"):
            page += 1
        else:
            break

    logger.info(f"Найдено {len(person_ids)} персонажей")
    return person_ids


async def fetch_name_from_url(
    session: aiohttp.ClientSession, url: str, cache: Dict[str, str]
) -> Optional[str]:
    if not url:
        return None
    if url in cache:
        return cache[url]
    try:
        data = await fetch_with_retry(session, url)
    except ResourceNotFoundError:
        return None

    if data:
        name = data.get("result", {}).get("properties", {}).get("name") or data.get(
            "result", {}
        ).get("properties", {}).get("title")
        if name:
            cache[url] = name
            return name
    return None


async def fetch_names_from_urls(
    session: aiohttp.ClientSession, urls: List[str], cache: Dict[str, str]
) -> str:
    if not urls:
        return ""
    tasks = [fetch_name_from_url(session, url, cache) for url in urls]
    names = await asyncio.gather(*tasks)
    return ", ".join([name for name in names if name])


async def fetch_person_details(
    session: aiohttp.ClientSession, person_id: int, cache: Dict[str, str]
) -> Optional[dict]:
    url = f"{BASE_URL}/people/{person_id}"
    try:
        data = await fetch_with_retry(session, url)
    except ResourceNotFoundError:
        logger.warning(f"Персонаж {person_id} не найден")
        return None

    if not data:
        return None

    props = data.get("result", {}).get("properties", {})
    if not props:
        logger.warning(f"У персонажа {person_id} отсутствуют свойства")
        return None

    homeworld_task = asyncio.create_task(
        fetch_name_from_url(session, props.get("homeworld"), cache)
    )
    films_task = asyncio.create_task(
        fetch_names_from_urls(session, props.get("films", []), cache)
    )
    species_task = asyncio.create_task(
        fetch_names_from_urls(session, props.get("species", []), cache)
    )
    starships_task = asyncio.create_task(
        fetch_names_from_urls(session, props.get("starships", []), cache)
    )
    vehicles_task = asyncio.create_task(
        fetch_names_from_urls(session, props.get("vehicles", []), cache)
    )

    homeworld_name = await homeworld_task
    films_str = await films_task
    species_str = await species_task
    starships_str = await starships_task
    vehicles_str = await vehicles_task

    return {
        "id": int(person_id),
        "birth_year": props.get("birth_year"),
        "eye_color": props.get("eye_color"),
        "gender": props.get("gender"),
        "hair_color": props.get("hair_color"),
        "height": props.get("height"),
        "mass": props.get("mass"),
        "name": props.get("name"),
        "skin_color": props.get("skin_color"),
        "homeworld": homeworld_name or "",
        "films": films_str,
        "species": species_str,
        "starships": starships_str,
        "vehicles": vehicles_str,
    }


async def load_data_to_db():
    async with Database(DATABASE_URL) as database:
        async with aiohttp.ClientSession() as session:
            person_ids = await fetch_all_person_ids(session)
            if not person_ids:
                logger.error("Не удалось получить список персонажей")
                return

            cache: Dict[str, str] = {}
            tasks = [fetch_person_details(session, pid, cache) for pid in person_ids]

            results = await tqdm_asyncio.gather(
                *tasks, desc="Загрузка деталей", unit="перс"
            )

            valid_results = [res for res in results if res is not None]
            failed_count = len(results) - len(valid_results)

            logger.info(
                f"Успешно загружено {len(valid_results)} персонажей из {len(person_ids)}"
            )
            if failed_count > 0:
                logger.warning(f"Не удалось загрузить данные для {failed_count} персонажей")

            if valid_results:
                sample = valid_results[0]
                logger.info(
                    f"Пример записи перед вставкой:\n"
                    f"  id={sample['id']}\n"
                    f"  name={sample['name']}\n"
                    f"  homeworld='{sample['homeworld']}'\n"
                    f"  films='{sample['films']}'\n"
                    f"  species='{sample['species']}'\n"
                    f"  starships='{sample['starships']}'\n"
                    f"  vehicles='{sample['vehicles']}'"
                )

                insert_query = """
                INSERT INTO characters(
                    id, birth_year, eye_color, gender, hair_color, height, mass, name, skin_color,
                    homeworld, films, species, starships, vehicles
                )
                VALUES (
                    :id, :birth_year, :eye_color, :gender, :hair_color, :height, :mass, :name, :skin_color,
                    :homeworld, :films, :species, :starships, :vehicles
                )
                ON CONFLICT(id) DO UPDATE SET
                    birth_year=excluded.birth_year,
                    eye_color=excluded.eye_color,
                    gender=excluded.gender,
                    hair_color=excluded.hair_color,
                    height=excluded.height,
                    mass=excluded.mass,
                    name=excluded.name,
                    skin_color=excluded.skin_color,
                    homeworld=excluded.homeworld,
                    films=excluded.films,
                    species=excluded.species,
                    starships=excluded.starships,
                    vehicles=excluded.vehicles;
                """
                await database.execute_many(query=insert_query, values=valid_results)
                logger.info(f"В базу данных добавлено/обновлено {len(valid_results)} персонажей.")
            else:
                logger.warning("Нет данных для вставки.")


if __name__ == "__main__":
    asyncio.run(load_data_to_db())
from collections.abc import Iterable
import asyncio
import pathlib
import csv

from loguru import logger
import psycopg
import tqdm


async def init(cur: psycopg.AsyncCursor):
    logger.info("Creating tables")
    await cur.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cuisine TEXT NOT NULL
        )
    """)

    await cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient TEXT NOT NULL PRIMARY KEY
        )
    """)

    await cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_id INTEGER NOT NULL REFERENCES recipes(id),
            ingredient TEXT NOT NULL REFERENCES ingredients(ingredient),
            original_ingredient TEXT NOT NULL
        )
    """)


async def insert(
    cur: psycopg.AsyncCursor,
    data: tuple | list,
):
    async with cur.copy(
        "COPY recipes (id, name, cuisine) FROM STDIN"
    ) as copy:
        for recipe in tqdm.tqdm(data[0]):
            await copy.write_row(recipe)

    async with cur.copy(
        "COPY ingredients (ingredient) FROM STDIN"
    ) as copy:
        for ingredient in tqdm.tqdm(data[1]):
            await copy.write_row(ingredient)

    async with cur.copy(
        "COPY recipe_ingredients \
        (recipe_id, ingredient, original_ingredient) \
        FROM STDIN"
    ) as copy:
        for foreign_key in tqdm.tqdm(data[2]):
            await copy.write_row(foreign_key)


async def indexes(cur: psycopg.AsyncCursor):
    await cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    await cur.execute("""
        CREATE INDEX IF NOT EXISTS trgm_idx ON recipe_ingredients USING GIN (
            ingredient gin_trgm_ops
        )
    """)


def read(
    data: Iterable[pathlib.Path]
):
    logger.info("Reading data")
    (
        recipes_path,
        ingredients_path,
        compound_ingredients_path,
        foreign_keys_path,
    ) = data

    logger.info("Reading recipes")

    recipes = set()
    ingredients = set()
    foreign_keys = set()

    with recipes_path.open() as f:
        reader = csv.reader(f)
        for row in tqdm.tqdm(reader):
            recipes.add((int(row[0]), row[1].lower(), row[3].lower()))

    logger.info("Reading ingredients")

    with compound_ingredients_path.open() as f:
        reader = csv.reader(f)
        for row in tqdm.tqdm(reader):
            ingredients.add((row[0].lower().strip(),))

    with ingredients_path.open() as f:
        reader = csv.reader(f)
        for row in tqdm.tqdm(reader):
            ingredients.add((row[0].lower().strip(),))

    logger.info("Reading foreign keys")

    with foreign_keys_path.open() as f:
        reader = csv.reader(f)
        for row in tqdm.tqdm(reader):
            foreign_keys.add((
                int(row[0]),
                row[2].lower().strip(),
                row[1].lower(),
            ))

            ingredients.add((row[2].lower().strip(),))

    return recipes, ingredients, foreign_keys


async def run():
    async with await psycopg.AsyncConnection.connect(
        dbname="food",
        user="postgres",
    ) as conn:
        async with conn.cursor() as cur:
            await init(cur)

            paths = (
                pathlib.Path("./data/01_Recipe_Details.csv"),
                pathlib.Path("./data/02_Ingredients.csv"),
                pathlib.Path("./data/03_Compound_Ingredients.csv"),
                pathlib.Path("./data/04_Recipe-Ingredients_Aliases.csv"),
            )
            data = read(paths)

            await insert(cur, data)


asyncio.run(run())

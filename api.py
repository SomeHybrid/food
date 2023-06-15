import asyncio

from fastapi import FastAPI, HTTPException
import psycopg
import pydantic
import uvicorn


app = FastAPI()

conn: psycopg.AsyncConnection = asyncio.run(
    psycopg.AsyncConnection.connect(dbname="food", user="postgres")
)


class Recipe(pydantic.BaseModel):
    id: int
    name: str
    ingredients: list[str]
    original_ingredients: list[str]
    cuisine: str


@app.get("/api/from_ingredient/{ingredient}")
async def from_ingredient(ingredient: str) -> list[Recipe]:
    ingredients = ingredient.split(",")
    async with conn.cursor() as cur:
        query = "SELECT * FROM recipe_ingredients WHERE " + " OR ".join(
            ["ingredient %% (%s)" for _ in ingredients]
        ) + " LIMIT 20"

        await cur.execute(query, ingredients)
        ingredients = await cur.fetchall()

        if len(ingredients) == 0:
            raise HTTPException(
                status_code=404,
                detail="No recipes found"
            )

        ids = tuple(i[0] for i in ingredients)

        query = "SELECT * FROM recipes WHERE " + " OR ".join(
            ["id=(%s)" for _ in ids]
        )

        await cur.execute(query, ids)
        recipes = await cur.fetchall()

        data = []

        query = """SELECT (
            ingredient,
            original_ingredient
        ) FROM recipe_ingredients WHERE recipe_id=(%s)"""
        for id, name, cuisine in recipes:
            await cur.execute(query, (id,))
            ingredients = await cur.fetchall()
            ingredients = sum(ingredients, ())

            ingredients, original_ingredients = list(zip(*ingredients))
            data.append(
                Recipe(
                    id=id,
                    name=name,
                    ingredients=ingredients,
                    original_ingredients=original_ingredients,
                    cuisine=cuisine,
                ),
            )

        return data


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=4269)

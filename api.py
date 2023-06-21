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
    name: str
    ingredients: list[str]
    original_ingredients: list[str]
    cuisine: str


@app.get("/api/from_ingredient/{ingredient}")
async def from_ingredient(ingredient: str):
    ingredients = ingredient.split(",")
    async with conn.cursor() as cur:
        placeholder = " OR ".join(
            ["recipe_ingredients.ingredient %% (%s)" for _ in ingredients]
        )
        query = f"""
            SELECT
                recipes.*,
                array_agg(recipe_ingredients.ingredient) as "ingredients",
                array_agg(recipe_ingredients.original_ingredient)
                    as "original_ingredients",
                SUM(
                    CASE WHEN {placeholder}
                        THEN 1 ELSE 0 
                    END
                ) AS matched_ingredients
            FROM recipes
            INNER JOIN recipe_ingredients
            ON recipe_ingredients.recipe_id = recipes.id
            WHERE recipes.id IN (
                SELECT recipe_id FROM recipe_ingredients
                WHERE {placeholder}
                GROUP BY recipe_id
            )
            GROUP BY recipes.id
            ORDER BY matched_ingredients DESC
            LIMIT 20
        """

        await cur.execute(query, ingredients + ingredients)
        recipes = await cur.fetchall()

        if not recipes:
            HTTPException(status_code=404, detail="No recipes found")
        
        data = [
            Recipe(
                name=i[1], 
                cuisine=i[2], 
                ingredients=i[3], 
                original_ingredients=i[4]
            )
            for i in recipes
        ]

        return data


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=4269)

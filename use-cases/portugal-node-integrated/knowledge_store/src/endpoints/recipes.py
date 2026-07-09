from fastapi import APIRouter

router = APIRouter()

@router.get("/recipes/list")
async def list_recipes():
    return {"recipes": []}

@router.post("/recipes/apply")
async def apply_recipe():
    return {"result": "recipe applied"}

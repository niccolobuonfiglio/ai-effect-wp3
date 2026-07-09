from fastapi import APIRouter

router = APIRouter()

@router.get("/examples/list")
async def list_examples():
    return {"examples": []}

@router.get("/examples/{dataset_name}")
async def get_example(dataset_name: str):
    return {"example": dataset_name}

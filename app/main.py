from fastapi import FastAPI
from app.routers import example_route

app = FastAPI(
    title= "Sales API",
    description="API for sales data",
    version="0.0.1"
)

app.include_router(example_route.router)

@app.get("/")
def get_root():
    return {"message": "Hello"}
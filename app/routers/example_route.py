from fastapi import APIRouter, HTTPException

# "/"
# "/example"
router = APIRouter(
    prefix="/example",
    tags=["example"],
    responses={404: {"description": "Not found"}}
)

# localhost:{port_num}/example/
router.get("/")
def get_sales_root():
    return {"message": "Hello from example"}

router.get("/exception")
def get_exception():
    raise HTTPException(status_code=404, detail="Not found")
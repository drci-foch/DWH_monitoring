from fastapi import FastAPI
from app.core.config import settings
from app.api.main import api_router

app = FastAPI(title="Monitoring of the DWH database")
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
async def startup_event():
    routes = [{"path": route.path, "name": route.name} for route in app.routes]
    print("Available routes:")
    for route in routes:
        print(f"Path: {route['path']}, Name: {route['name']}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
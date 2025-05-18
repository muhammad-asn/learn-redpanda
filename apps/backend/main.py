from fastapi import FastAPI
import uvicorn
from api.routes import router
from config.settings import ServerSettings

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=ServerSettings.HOST,
        port=ServerSettings.PORT,
        reload=True,
        reload_delay=0.25,
        workers=1
    )

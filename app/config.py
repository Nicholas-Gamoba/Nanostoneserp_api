import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    API_VERSION = "1.0.0"
    API_TITLE = "Nanostone SERP API"

    DATABASE_URL = os.getenv("DATABASE_URL", "")
    DFS_USERNAME = os.getenv("DFS_USERNAME")
    DFS_PASSWORD = os.getenv("DFS_PASSWORD")

    VERCEL_WEBHOOK_URL = os.getenv("VERCEL_WEBHOOK_URL")
    VERCEL_AUTOMATION_BYPASS_SECRET = os.getenv("VERCEL_AUTOMATION_BYPASS_SECRET")

    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://neondb_owner:npg_UoOMjHr5F9Ab@ep-summer-tooth-al96eadj-pooler.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require",
    )

    API_BASE_URL = os.getenv("API_BASE_URL", "https://nanostoneserp-api.onrender.com")

    CRON_SECRET = os.getenv("CRON_SECRET")


settings = Settings()

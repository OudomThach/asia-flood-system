"""
Production FastAPI Application Entrypoint for Asian Regional Multi-Hazard Flood Warning Platform.
Author: Oudom Thach
Standard FastAPI ASGI module: run with `uvicorn asia_flood_core.main:app --host 0.0.0.0 --port 8000`
"""

import os

from asia_flood_core.admin_server import app


def main():
    """Console-script entrypoint (`asia-flood`): launches the ASGI server."""
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("asia_flood_core.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()

import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer(auto_error=False)


def get_api_key() -> str:
    """Return the configured API key or generate one if not set."""
    key = os.environ.get("PEERTUBE2NOSTR_API_KEY")
    if not key:
        key = os.environ.get("API_KEY")
    if key:
        return key
    return ""


async def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> None:
    api_key = get_api_key()
    if not api_key:
        return
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required. Set PEERTUBE2NOSTR_API_KEY or pass Authorization: Bearer <key>",
        )
    if not secrets.compare_digest(credentials.credentials, api_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

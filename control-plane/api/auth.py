"""
Single shared-secret auth for local multi-user demos.
Set CP_SECRET in the environment to enable; leave it unset to run open (dev convenience).
"""
import os
import secrets

from fastapi import Header, HTTPException, status

_SECRET: str = os.getenv("CP_SECRET", "")


def require_auth(authorization: str = Header(default="")) -> None:
    if not _SECRET:
        return  # auth disabled when CP_SECRET is not set
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(token.encode(), _SECRET.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# hobby-session-19

# hobby-session-72

# hobby-session-86

# hobby-session-239

# hobby-session-435

# hobby-session-95

# hobby-session-129

# hobby-session-245

# hobby-session-30

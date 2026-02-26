"""
Configuration for the API service.
"""
import os
# import redis  # COMMENTED OUT - Redis will be added later
from typing import Optional

# Redis configuration (COMMENTED OUT - TO BE ADDED LATER)
# REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# API configuration
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
API_WORKERS = int(os.getenv("API_WORKERS", "4"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Redis client singleton (COMMENTED OUT - TO BE ADDED LATER)
# _redis_client: Optional[redis.Redis] = None


# def get_redis_client() -> redis.Redis:
#     """
#     Get Redis client singleton.
#     
#     Returns:
#         Redis client instance
#     """
#     global _redis_client
#     
#     if _redis_client is None:
#         _redis_client = redis.Redis(
#             host=REDIS_HOST,
#             port=REDIS_PORT,
#             db=REDIS_DB,
#             decode_responses=False  # Keep binary for flexibility
#         )
#     
#     return _redis_client


# def close_redis_client():
#     """Close Redis client connection."""
#     global _redis_client
#     
#     if _redis_client:
#         _redis_client.close()
#         _redis_client = None

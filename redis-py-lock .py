"""使用 redis-py-lock 实现分布式锁
安装依赖

pip install redis redis-py-lock
"""

import redis
from redis_lock import Lock

# 初始化 Redis 客户端
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# 创建分布式锁
lock_key = "my_lock_key"

# 尝试获取锁
lock = Lock(redis_client, lock_key, expire=10, auto_renewal=True)

if lock.acquire(blocking=True, timeout=5):
    print("Lock acquired.")

    try:
        # 模拟任务处理
        print("Processing task...")
    finally:
        # 释放锁
        lock.release()
        print("Lock released.")
else:
    print("Failed to acquire lock.")

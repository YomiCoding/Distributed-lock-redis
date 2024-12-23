"""使用 redis-py 实现分布式锁
安装依赖
pip install redis
"""

import redis
import uuid
import time


class RedisDistributedLock:
    def __init__(self, redis_client):
        self.redis = redis_client

    def acquire_lock(self, lock_key, lock_ttl=10):
        """
        获取分布式锁
        :param lock_key: 锁的键
        :param lock_ttl: 锁的过期时间（秒）
        :return: 唯一标识符（成功），或 None（失败）
        """
        lock_value = str(uuid.uuid4())  # 生成唯一标识符
        if self.redis.set(lock_key, lock_value, ex=lock_ttl, nx=True):
            return lock_value  # 成功获取锁，返回标识符
        return None

    def release_lock(self, lock_key, lock_value):
        """
        释放分布式锁（原子操作）
        :param lock_key: 锁的键
        :param lock_value: 锁的值
        :return: 是否成功释放
        """
        release_lock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = self.redis.eval(release_lock_script, 1, lock_key, lock_value)
        return result == 1

    def extend_lock(self, lock_key, lock_value, additional_ttl):
        """
        续命锁（延长过期时间）
        :param lock_key: 锁的键
        :param lock_value: 锁的值
        :param additional_ttl: 需要延长的时间（秒）
        :return: 是否成功续命
        """
        extend_lock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = self.redis.eval(
            extend_lock_script, 1, lock_key, lock_value, additional_ttl
        )
        return result == 1


# 使用示例
if __name__ == "__main__":
    # 初始化 Redis 客户端
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # 创建分布式锁
    lock = RedisDistributedLock(redis_client)
    lock_key = "my_lock_key"
    lock_ttl = 15  # 锁的超时时间

    # 获取锁
    lock_value = lock.acquire_lock(lock_key, lock_ttl)
    if lock_value:
        print(f"Lock acquired with value: {lock_value}")

        try:
            # 模拟业务处理
            time.sleep(5)

            # 续命锁
            if lock.extend_lock(lock_key, lock_value, 15):
                print("Lock extended successfully.")

            # 完成任务
            print("Task completed.")

        finally:
            # 释放锁
            if lock.release_lock(lock_key, lock_value):
                print("Lock released successfully.")
            else:
                print("Failed to release lock.")
    else:
        print("Failed to acquire lock.")

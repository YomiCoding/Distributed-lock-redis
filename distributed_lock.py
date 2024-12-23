import redis
import time
from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist
from typing import List


class YourClass:
    def __init__(self, redis_client=None):
        # 初始化时传入 Redis 客户端（默认连接本地 Redis）
        self.redis = redis_client or redis.StrictRedis(
            host="localhost", port=6379, db=0, decode_responses=True
        )

    def get_books(self, query: str, by: str) -> List[books]:
        ...

        if new_books:
            for new_book in new_books:

                # 使用 Redis 锁对 name 进行加锁
                lock_key = f"book_lock:{new_book.name}"
                lock_acquired = self._acquire_lock(lock_key)

                if lock_acquired:
                    try:
                        # 开启事务进行数据库操作
                        with transaction.atomic():
                            book, created = Book.objects.update_or_create(
                                ldap_name=new_book.name,
                                defaults=model_to_dict(
                                    new_book,
                                    exclude=["id", "create_time", "last_modified_time"],
                                ),
                            )
                    finally:
                        # 操作完成后释放锁
                        self._release_lock(lock_key)
                else:
                    logger.info(
                        f"book {new_book.name} is already being processed by another instance."
                    )

            new_books = Book.objects.filter(**{field: query})
            return list(new_books)
        else:
            Book.objects.filter(**{field: query}).delete()
            return new_books

    def _acquire_lock(self, lock_key: str, timeout: int = 10) -> bool:
        """
        尝试获取分布式锁。使用 Redis SETNX 命令保证只有一个实例能获取到锁。
        :param lock_key: 锁的 key，通常可以使用用户的唯一标识（如 ldap_name）。
        :param timeout: 锁的超时时间（秒）。
        :return: 如果锁成功获取返回 True，否则返回 False。
        """
        lock_value = str(time.time())  # 使用时间戳作为锁的值
        is_locked = self.redis.setnx(
            lock_key, lock_value
        )  # 尝试获取锁，只有没有锁时返回 True

        if is_locked:
            # 设置锁的超时时间，防止死锁
            self.redis.expire(lock_key, timeout)
            # 启动续命机制
            self._extend_lock_lifetime(lock_key, timeout)
            return True
        else:
            # 如果锁已存在，检查锁是否已经过期
            current_lock_value = self.redis.get(lock_key)
            if current_lock_value and float(current_lock_value) + timeout < time.time():
                # 锁已过期，尝试重新获取锁
                self.redis.set(lock_key, lock_value)
                self.redis.expire(lock_key, timeout)
                # 启动续命机制
                self._extend_lock_lifetime(lock_key, timeout)
                return True
            return False

    def _extend_lock_lifetime(self, lock_key: str, timeout: int):
        """
        锁续命机制：定期更新锁的超时时间，确保操作时间长的锁不会过期。
        :param lock_key: 锁的 key
        :param timeout: 锁的超时时间（秒）
        """
        # 使用一个后台线程或定时器定期延长锁的生命周期，防止过期
        # 每隔 `timeout / 2` 时间更新一次锁的超时时间
        while True:
            time.sleep(timeout / 2)
            # 确保锁依然存在
            if self.redis.exists(lock_key):
                self.redis.expire(lock_key, timeout)  # 更新超时时间
            else:
                break

    def _release_lock(self, lock_key: str) -> None:
        """
        释放分布式锁。使用 Redis 的原子操作确保锁释放过程不受干扰。
        :param lock_key: 锁的 key。
        """

    try:
        # 使用 Redis WATCH 来监控锁的状态
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    # 监控锁的状态
                    pipe.watch(lock_key)
                    # 如果锁的键不存在，表示锁已经被释放，不需要操作
                    if not self.redis.exists(lock_key):
                        break
                    # 执行删除操作
                    pipe.multi()  # 开始事务
                    pipe.delete(lock_key)  # 删除锁
                    pipe.execute()  # 执行事务

                    logger.info(f"Lock {lock_key} successfully released.")
                    break  # 成功释放锁后跳出循环

                except redis.WatchError:
                    # 如果在执行过程中发现锁已经被改变，则重新尝试
                    logger.warning(f"Lock {lock_key} was modified, retrying...")
                    continue
    except Exception as e:
        logger.error(f"Error while releasing lock {lock_key}: {str(e)}")

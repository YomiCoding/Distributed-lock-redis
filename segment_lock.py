import redis
import hashlib
import logging
from typing import List
from django.db import transaction
from django.forms.models import model_to_dict
from redis.exceptions import RedisError


class UserService:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def _get_segment_lock_key(self, name: str, num_segments: int) -> str:
        """根据 name 生成分段锁的 key"""
        segment = int(hashlib.md5(name.encode()).hexdigest(), 16) % num_segments
        return f"book_lock:{segment}"

    def _acquire_segment_lock(self, lock_key: str) -> bool:
        """尝试获取分段锁"""
        try:
            # 设置超时，避免死锁
            return self.redis.setnx(lock_key, "locked", ex=30)  # 锁30秒
        except RedisError:
            return False

    def _release_segment_lock(self, lock_key: str) -> None:
        """释放分段锁"""
        try:
            self.redis.delete(lock_key)
        except RedisError:
            logging.error(f"Failed to release lock {lock_key}")

    def get_bookss(self, query: str, by: str, num_segments: int = 10) -> List[Books]:
        """使用分段锁"""
        ...
        if new_books:
            for new_book in new_books:
                # 计算分段锁的 key
                segment_lock_key = self._get_segment_lock_key(
                    new_book.name, num_segments
                )

                # 获取分段锁
                if self._acquire_segment_lock(segment_lock_key):
                    try:
                        # 在事务中进行数据库操作
                        with transaction.atomic():
                            # 检查用户是否已经存在
                            book, _ = Book.objects.update_or_create(
                                ldap_name=new_book.name,
                                defaults=model_to_dict(
                                    new_book,
                                    exclude=["id", "create_time", "last_modified_time"],
                                ),
                            )

                    except Exception as e:
                        logger.error(f"Error processing {new_book.name}: {str(e)}")
                    finally:
                        # 释放锁
                        self._release_segment_lock(segment_lock_key)
                else:
                    logger.warning(
                        f"Lock for {new_book.name} already acquired, skipping..."
                    )

            new_books = Book.objects.filter(**{field: query})
            return list(new_books)
        else:
            # 如果没有找到用户，删除记录
            Book.objects.filter(**{field: query}).delete()
            return new_books

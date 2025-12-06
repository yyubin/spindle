use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::Arc;

/// 버퍼 풀 통계
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// 풀에서 할당된 버퍼 수
    pub allocated: usize,
    /// 풀로 반환된 버퍼 수
    pub returned: usize,
    /// 새로 생성된 버퍼 수 (풀이 비어있을 때)
    pub created: usize,
    /// 현재 풀에 있는 버퍼 수
    pub available: usize,
}

/// Lock-based 버퍼 풀
/// BytesMut 버퍼를 재사용하여 힙 할당을 줄이기
/// Java의 ByteBuffer pooling과 유사한 느낌
pub struct BufferPool {
    /// 버퍼 저장소
    pool: Mutex<Vec<BytesMut>>,
    /// 각 버퍼의 capacity
    buffer_capacity: usize,
    /// 풀에 보관할 최대 버퍼 수
    max_pool_size: usize,
    /// 통계
    stats: Mutex<PoolStats>,
}

impl BufferPool {
    /// 새로운 버퍼 풀 생성
    ///
    /// # Arguments
    /// * `buffer_capacity` - 각 버퍼의 capacity (bytes)
    /// * `initial_size` - 초기에 생성할 버퍼 수
    /// * `max_pool_size` - 풀에 보관할 최대 버퍼 수
    pub fn new(buffer_capacity: usize, initial_size: usize, max_pool_size: usize) -> Arc<Self> {
        let mut pool = Vec::with_capacity(max_pool_size);

        // 초기 버퍼 생성
        for _ in 0..initial_size {
            pool.push(BytesMut::with_capacity(buffer_capacity));
        }

        Arc::new(Self {
            pool: Mutex::new(pool),
            buffer_capacity,
            max_pool_size,
            stats: Mutex::new(PoolStats {
                available: initial_size,
                ..Default::default()
            }),
        })
    }

    /// 기본 설정으로 버퍼 풀 생성
    /// - buffer_capacity: 8KB
    /// - initial_size: 100
    /// - max_pool_size: 1000
    pub fn with_defaults() -> Arc<Self> {
        Self::new(8 * 1024, 100, 1000)
    }

    /// 버퍼 획득
    /// 풀에서 버퍼를 가져오거나 없으면 새로 생성
    pub fn acquire(&self) -> BytesMut {
        let mut stats = self.stats.lock();
        stats.allocated += 1;

        let mut pool = self.pool.lock();

        if let Some(mut buf) = pool.pop() {
            // 풀에서 가져온 버퍼는 clear (len=0, capacity 유지)
            buf.clear();
            stats.available = pool.len();
            drop(pool);
            drop(stats);
            buf
        } else {
            // 풀이 비어있으면 새로 생성
            stats.created += 1;
            stats.available = 0;
            drop(pool);
            drop(stats);
            BytesMut::with_capacity(self.buffer_capacity)
        }
    }

    /// 버퍼 반환
    /// 버퍼를 풀로 돌려주기
    /// capacity가 다르거나 풀이 가득 차면 drop
    pub fn release(&self, mut buf: BytesMut) {
        let mut stats = self.stats.lock();
        stats.returned += 1;

        // capacity 체크
        if buf.capacity() != self.buffer_capacity {
            // 크기가 다르면 풀에 넣지 않고 drop
            return;
        }

        let mut pool = self.pool.lock();

        if pool.len() < self.max_pool_size {
            buf.clear();
            pool.push(buf);
            stats.available = pool.len();
        } else {
            // 풀이 가득 차면 drop
            stats.available = pool.len();
        }
    }

    /// 현재 통계 조회
    pub fn stats(&self) -> PoolStats {
        self.stats.lock().clone()
    }

    /// 버퍼 capacity 조회
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// 풀 최대 크기 조회
    pub fn max_pool_size(&self) -> usize {
        self.max_pool_size
    }
}

/// RAII 스타일 버퍼 핸들
/// Drop 시 자동으로 풀로 반환됩니다.
pub struct PooledBuffer {
    buffer: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl PooledBuffer {
    /// 풀에서 버퍼 획득
    pub fn acquire(pool: Arc<BufferPool>) -> Self {
        let buffer = pool.acquire();
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// 내부 버퍼에 대한 mutable reference
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().expect("Buffer already taken")
    }

    /// 내부 버퍼에 대한 reference
    pub fn buffer(&self) -> &BytesMut {
        self.buffer.as_ref().expect("Buffer already taken")
    }

    /// 버퍼를 소유권과 함께 가져가기 (더 이상 자동 반환 안됨)
    pub fn into_inner(mut self) -> BytesMut {
        self.buffer.take().expect("Buffer already taken")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buffer.take() {
            self.pool.release(buf);
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new(1024, 10, 100);

        let buf1 = pool.acquire();
        assert_eq!(buf1.capacity(), 1024);

        pool.release(buf1);

        let stats = pool.stats();
        assert_eq!(stats.allocated, 1);
        assert_eq!(stats.returned, 1);
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let pool = BufferPool::new(1024, 0, 100);

        // 첫 번째 할당 - 새로 생성
        let buf1 = pool.acquire();
        assert_eq!(pool.stats().created, 1);

        pool.release(buf1);

        // 두 번째 할당 - 재사용
        let buf2 = pool.acquire();
        assert_eq!(pool.stats().created, 1); // 여전히 1
        assert_eq!(pool.stats().allocated, 2);

        pool.release(buf2);
    }

    #[test]
    fn test_pooled_buffer_auto_return() {
        let pool = BufferPool::new(1024, 5, 100);

        {
            let _buf = PooledBuffer::acquire(pool.clone());
            assert_eq!(pool.stats().allocated, 1);
        } // buf drops here

        // 자동으로 반환됨
        let stats = pool.stats();
        assert_eq!(stats.returned, 1);
        assert_eq!(stats.available, 5); // 초기 5개 + 반환 1개 - 할당 1개 = 5개
    }

    #[test]
    fn test_max_pool_size() {
        let pool = BufferPool::new(1024, 0, 2);

        let buf1 = pool.acquire();
        let buf2 = pool.acquire();
        let buf3 = pool.acquire();

        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3); // 이건 drop될 것

        let stats = pool.stats();
        assert_eq!(stats.available, 2); // max_pool_size
    }
}

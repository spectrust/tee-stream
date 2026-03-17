use std::{
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll, Waker, ready},
};

use futures::Stream;
use parking_lot::Mutex;

/// One side of a teed stream.
pub struct TeedStream<S>
where
    S: Stream,
    S::Item: Clone,
{
    source: Arc<Mutex<TeeBuilder<S>>>,
    side: Either,
}
impl<S> Drop for TeedStream<S>
where
    S: Stream,
    S::Item: Clone,
{
    fn drop(&mut self) {
        self.source.lock().drop_state(self.side);
    }
}
impl<S> Stream for TeedStream<S>
where
    S: Stream + Unpin,
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut source = self.source.lock();

        // We only need to attempt to wake the other side if our buffer was
        // full: otherwise, we would not have previously returned pending for
        // the other side, so there is no need to wake it.
        let buffer_was_full = source.buffer_full(self.side);
        if let Some(v) = source.pop(self.side) {
            if buffer_was_full {
                // If the other side has a waker, it is because this side's buffer
                // was previously full. Now that we have consumed off of it, we
                // have space for another item, so wake the other side so that it
                // can continue processing.
                source.wake_other_side(self.side);
            }
            return Poll::Ready(Some(v));
        }

        // If the other side's buffer is full, we can't keep consuming off the
        // stream. Apply some back-pressure by returning Pending, and set up
        // a waker so that, when the other stream next consumes a value out
        // of its buffer, it can wake us up.
        if source.other_buffer_full(self.side) {
            source.set_waker(self.side, cx.waker());
            return Poll::Pending;
        }

        let stream = pin!(&mut source.stream);

        let v = ready!(stream.poll_next(cx));

        if let Some(ref v) = v {
            // We're going to return the value from the source stream to whoever
            // is executing this teed stream. Push a clone of the value to the
            // other side's buffer, so that it will be immediatley ready for
            // consumption by that stream.
            source.push_to_other_side(self.side, v);
        }
        Poll::Ready(v)
    }
}

/// A state tracker that doubles as a builder for teed streams.
///
/// This struct holds the underlying source stream. It also holds configuration
/// and some state for each of the teed streams.
pub struct TeeBuilder<S>
where
    S: Stream,
{
    /// The inner stream
    stream: S,
    /// The maximum size of the buffers for the left and right teed streams.
    buffer_limit: BufferLimit,
    /// State tracker for the left stream.
    left: Option<TeedStreamState<S::Item>>,
    /// State tracker for the right stream.
    right: Option<TeedStreamState<S::Item>>,
}
impl<S> TeeBuilder<S>
where
    S: Stream,
    S::Item: Clone,
{
    #[must_use]
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            buffer_limit: BufferLimit::Bytes(1_000_000),
            left: Some(Default::default()),
            right: Some(Default::default()),
        }
    }

    #[must_use]
    pub fn with_max_buffer_bytes(mut self, bytes: usize) -> Self {
        self.buffer_limit = BufferLimit::Bytes(bytes);
        self
    }

    #[must_use]
    pub fn with_max_buffer_len(mut self, length: usize) -> Self {
        self.buffer_limit = BufferLimit::Length(length);
        self
    }

    #[must_use]
    pub fn with_unlimited_buffer(mut self) -> Self {
        self.buffer_limit = BufferLimit::None;
        self
    }

    #[must_use]
    pub fn tee(self) -> (TeedStream<S>, TeedStream<S>) {
        let source = Arc::new(Mutex::new(self));
        let left = TeedStream {
            source: source.clone(),
            side: Either::Left,
        };
        let right = TeedStream {
            source,
            side: Either::Right,
        };
        (left, right)
    }

    fn state_mut(&mut self, side: Either) -> Option<&mut TeedStreamState<S::Item>> {
        match side {
            Either::Left => self.left.as_mut(),
            Either::Right => self.right.as_mut(),
        }
    }

    fn state(&self, side: Either) -> Option<&TeedStreamState<S::Item>> {
        match side {
            Either::Left => self.left.as_ref(),
            Either::Right => self.right.as_ref(),
        }
    }

    #[must_use]
    fn pop(&mut self, side: Either) -> Option<S::Item> {
        self.state_mut(side).and_then(|s| s.pop())
    }

    fn push_to_other_side(&mut self, side: Either, v: &S::Item) {
        let limit = self.buffer_limit;
        if let Some(s) = self.state_mut(side.other()) {
            s.push(v.clone(), limit);
        }
    }

    fn set_waker(&mut self, side: Either, waker: &Waker) {
        if let Some(state) = self.state_mut(side) {
            state.set_waker(waker);
        }
    }

    fn other_buffer_full(&self, side: Either) -> bool {
        self.buffer_full(side.other())
    }

    fn buffer_full(&self, side: Either) -> bool {
        self.state(side).map(|s| s.buffer_full()).unwrap_or(false)
    }

    fn wake_other_side(&mut self, side: Either) {
        if let Some(s) = self.state_mut(side.other()) {
            s.wake();
        }
    }

    fn drop_state(&mut self, side: Either) {
        match side {
            Either::Left => self.left = None,
            Either::Right => self.right = None,
        }
    }
}

use teed_stream_state::TeedStreamState;

/// An isolated module for teed stream state, since it has struct properties
/// that MUST be accessed through methods.
mod teed_stream_state {
    use super::BufferLimit;
    use std::{collections::VecDeque, task::Waker};

    #[derive(Clone)]
    pub(crate) struct TeedStreamState<I> {
        buffer: VecDeque<I>,
        waker: Option<Waker>,
        // cache the "full" state to avoid calculation, since it can only change
        // when pushing/popping.
        full: bool,
    }
    impl<I> TeedStreamState<I> {
        pub(crate) fn wake(&mut self) {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }

        pub(crate) fn set_waker(&mut self, waker: &Waker) {
            // We only need to clone the waker and set it if we either have no
            // registered waker OR the incoming waker is not referring to the same
            // task as the previous one.
            if let Some(ref previous_waker) = self.waker {
                if !waker.will_wake(previous_waker) {
                    // If the incoming waker is not for the same task as our previously
                    // stored waker, we *need* to wake the previous waker before
                    // discarding it, otherwise we can deadlock. This can happen,
                    // for example, when teeing an already teed future.
                    previous_waker.wake_by_ref();
                    self.waker = Some(waker.clone());
                }
            } else {
                self.waker = Some(waker.clone());
            }
        }

        pub(crate) fn push(&mut self, v: I, limit: BufferLimit) {
            self.buffer.push_back(v);
            let full = match limit {
                BufferLimit::None => false,
                BufferLimit::Bytes(limit) => {
                    let size = const { size_of::<I>() }.saturating_mul(self.buffer.len());
                    size >= limit
                }
                BufferLimit::Length(limit) => self.buffer.len() >= limit,
            };
            self.full = full;
        }

        pub(crate) fn pop(&mut self) -> Option<I> {
            let i = self.buffer.pop_front();
            if self.full {
                self.full = false;
            }
            i
        }

        pub(crate) fn buffer_full(&self) -> bool {
            self.full
        }
    }
    impl<I> Default for TeedStreamState<I> {
        fn default() -> Self {
            Self {
                buffer: Default::default(),
                waker: Default::default(),
                full: Default::default(),
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BufferLimit {
    Bytes(usize),
    Length(usize),
    None,
}

#[derive(Debug, Clone, Copy)]
enum Either {
    Left,
    Right,
}
impl Either {
    #[inline]
    fn other(&self) -> Either {
        match self {
            Either::Left => Either::Right,
            Either::Right => Either::Left,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, poll, stream};

    use crate::SpecStreamExt;

    use super::*;

    #[tokio::test]
    async fn basic_tee_parallel() {
        let count = 10_000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee();

        // Consume the streams in parallel, return sum
        let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc + i).await });
        let h2 = tokio::spawn(async move { right.fold(0, async |acc, i| acc + i).await });

        assert_eq!(exp_sum, h1.await.unwrap());
        assert_eq!(exp_sum, h2.await.unwrap());
    }

    #[tokio::test]
    async fn basic_tee_concurrent() {
        let count = 10_000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee();

        // Consume the streams concurrently, return sum
        let (left_sum, right_sum) = futures::join!(
            left.fold(0, async |acc, i| acc + i),
            right.fold(0, async |acc, i| acc + i),
        );

        assert_eq!(exp_sum, left_sum);
        assert_eq!(exp_sum, right_sum);
    }

    #[tokio::test]
    async fn one_at_a_time() {
        let count = 10_000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee();

        let left_sum = left.fold(0, async |acc, i| acc + i).await;
        assert_eq!(exp_sum, left_sum);

        let right_sum = right.fold(0, async |acc, i| acc + i).await;
        assert_eq!(exp_sum, right_sum);
    }

    #[tokio::test]
    async fn buffer_full() {
        let limit = 100;
        let count = 10_000;

        let stream = stream::iter(1..=count);
        let (mut left, mut right) = stream.tee_builder().with_max_buffer_len(limit).tee();

        // Consume the limit number of items out of the left stream. This should
        // mean the right stream's buffer is now full.
        for _ in 0..limit {
            left.next().await;
        }
        assert!(right.source.lock().buffer_full(Either::Right));

        // The next poll of the left stream should return Pending, with the full
        // right buffer providing back-pressure. Note this means that .await
        // would block.
        assert!(matches!(poll!(left.next()), Poll::Pending));

        // If we now consume something off of the right stream, the left stream
        // should no longer be pending.
        assert_eq!(1, right.next().await.unwrap()); // first item off the stream

        // Right is no longer full, and we can get the next value out of left.
        assert!(!right.source.lock().buffer_full(Either::Right));
        assert_eq!(limit + 1, left.next().await.unwrap());

        // But, since we have only consumed one item from right and have now
        // pulled another item out of left, right should be full again and
        // left should be pending.
        assert!(right.source.lock().buffer_full(Either::Right));
        assert!(matches!(poll!(left.next()), Poll::Pending));

        // We can now finish consuming both streams concurrently, even though
        // one is starting out in a pending state.
        let (left_max, right_max) = futures::join!(
            left.fold(0, async |acc, i| acc.max(i)),
            right.fold(0, async |acc, i| acc.max(i)),
        );

        assert_eq!(count, left_max);
        assert_eq!(count, right_max);
    }

    #[tokio::test]
    async fn buffer_full_bytes() {
        // One u64 is 8 bytes, so 100 usizes is 800 bytes.
        let limit_count = 100;
        let limit = limit_count * size_of::<usize>();
        let count = 10_000;

        let stream = stream::iter(1_usize..=count);
        let (mut left, mut right) = stream.tee_builder().with_max_buffer_bytes(limit).tee();

        // Consume items off the left stream, filling up the right stream's buffer,
        // until we have consumed enough such that the right stream should be
        // taking up its memory limit.
        for _ in 0..limit_count {
            left.next().await;
        }

        assert!(right.source.lock().buffer_full(Either::Right));
        assert!(matches!(poll!(left.next()), Poll::Pending));

        assert_eq!(1, right.next().await.unwrap());
        // now that we have pulled one off of right, we can pull off of left again.
        assert_eq!(limit_count + 1, left.next().await.unwrap());

        // And we can still consume both streams concurrently.
        assert_eq!(
            (count, count),
            futures::join!(
                left.fold(0, async |acc, i| acc.max(i)),
                right.fold(0, async |acc, i| acc.max(i)),
            )
        );
    }

    #[tokio::test]
    async fn dropped_stream_never_consumed() {
        // It's okay to drop one of the two streams.
        let count = 1000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee_builder().tee();

        drop(right);

        // At this point, the shared state should no longer retain any state about the right stream.
        assert!(left.source.lock().right.is_none());
        // We can still consume the left stream just fine.
        assert_eq!(exp_sum, left.fold(0, async |acc, i| acc + i).await);
    }

    #[tokio::test]
    async fn dropped_stream_partially_consumed() {
        let count = 1000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee_builder().tee();

        // This actually partially consumes and drops the right stream.
        assert_eq!(
            100,
            right.take(100).fold(0, async |acc, i| acc.max(i)).await
        );

        // At this point, the shared state should no longer retain any state about the right stream.
        assert!(left.source.lock().right.is_none());
        // And we can still consume the left stream just fine.
        assert_eq!(exp_sum, left.fold(0, async |acc, i| acc + i).await);
    }

    #[tokio::test]
    async fn dropped_frees_up_other() {
        let limit = 100;
        let count = 1000;

        let stream = stream::iter(1..=count);
        let (mut left, right) = stream.tee_builder().with_max_buffer_len(limit).tee();

        // Consume the limit number of items out of the left stream. This should
        // mean the right stream's buffer is now full.
        for _ in 0..limit {
            left.next().await;
        }
        assert!(right.source.lock().buffer_full(Either::Right));

        // The next poll of the left stream should return Pending, with the full
        // right buffer providing back-pressure. Note this means that .await
        // would block.
        assert!(matches!(poll!(left.next()), Poll::Pending));

        // But if we drop the right stream, it will unlock the left.
        drop(right);

        assert_eq!(count, left.fold(0, async |acc, i| acc.max(i)).await);
    }

    #[tokio::test]
    async fn double_tee_with_buffer_limit() {
        let limit = 100;
        let count = 10_000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee_builder().tee();

        // Tee each side again, this time with buffer limits
        let (left1, left2) = left.tee_builder().with_max_buffer_len(limit).tee();
        let (right1, right2) = right.tee_builder().with_max_buffer_len(limit).tee();

        // Consume all four streams concurrently
        let (sum1, sum2, sum3, sum4) = futures::join!(
            left1.fold(0, async |acc, i| acc + i),
            left2.fold(0, async |acc, i| acc + i),
            right1.fold(0, async |acc, i| acc + i),
            right2.fold(0, async |acc, i| acc + i),
        );

        assert_eq!(exp_sum, sum1);
        assert_eq!(exp_sum, sum2);
        assert_eq!(exp_sum, sum3);
        assert_eq!(exp_sum, sum4);
    }

    #[tokio::test]
    async fn double_tee_with_buffer_limit_parallel() {
        let limit = 100;
        let count = 10_000;
        let exp_sum = (1..=count).sum::<usize>();

        let stream = stream::iter(1..=count);
        let (left, right) = stream.tee_builder().tee();

        // Tee each side again, this time with buffer limits
        let (left1, left2) = left.tee_builder().with_max_buffer_len(limit).tee();
        let (right1, right2) = right.tee_builder().with_max_buffer_len(limit).tee();

        // Consume all four streams in parallel as separate tasks
        let h1 = tokio::spawn(async move { left1.fold(0, async |acc, i| acc + i).await });
        let h2 = tokio::spawn(async move { left2.fold(0, async |acc, i| acc + i).await });
        let h3 = tokio::spawn(async move { right1.fold(0, async |acc, i| acc + i).await });
        let h4 = tokio::spawn(async move { right2.fold(0, async |acc, i| acc + i).await });

        assert_eq!(exp_sum, h1.await.unwrap());
        assert_eq!(exp_sum, h2.await.unwrap());
        assert_eq!(exp_sum, h3.await.unwrap());
        assert_eq!(exp_sum, h4.await.unwrap());
    }
}

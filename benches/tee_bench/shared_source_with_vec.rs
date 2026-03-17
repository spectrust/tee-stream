//! An alternative version of the teed stream where substream states
//! are stored in a Vec, and, when teeing an already teed stream,
//! rather than creating a tree of TeeBuilders with associated Left/Right
//! states and nested Arc<Mutex<>> locks around the parent builders,
//! the parent state is shared, with all substream states stored in
//! a single vec.
//!
//! Somewhat surprisingly, while the baseline performance is about the
//! same speed as the real implementation, this implementation is
//! actually _slower_ (around 20% slower) for tees of tees, despite
//! being able to avoid dealing with two layers of
//! Arc<Mutex<TeeBuilder>> state.

use std::{
    collections::VecDeque,
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
    idx: usize,
}
impl<S> TeedStream<S>
where
    S: Stream,
    S::Item: Clone,
{
    pub fn tee(self) -> (TeedStream<S>, TeedStream<S>) {
        let mut source = self.source.lock();

        let tee_state = source.states[self.idx].clone();

        source.states.push(tee_state.clone());
        let right_idx = source.states.len() - 1;

        drop(source);

        let left_source = self.source.clone();
        let left_idx = self.idx;

        let right_source = self.source.clone();

        drop(self);

        left_source.lock().states[left_idx] = tee_state;

        (
            Self {
                source: left_source,
                idx: left_idx,
            },
            Self {
                source: right_source,
                idx: right_idx,
            },
        )
    }
}
impl<S> Drop for TeedStream<S>
where
    S: Stream,
    S::Item: Clone,
{
    fn drop(&mut self) {
        self.source.lock().drop_state(self.idx);
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

        if let Some(v) = source.pop(self.idx) {
            // If the other side has a waker, it is because this side's buffer
            // was previously full. Now that we have consumed off of it, we
            // have space for another item, so wake the other side so that it
            // can continue processing.
            source.wake_others(self.idx);
            return Poll::Ready(Some(v));
        }

        // If the other side's buffer is full, we can't keep consuming off the
        // stream. Apply some back-pressure by returning Pending, and set up
        // a waker so that, when the other stream next consumes a value out
        // of its buffer, it can wake us up.
        if source.any_other_buffer_full(self.idx) {
            source.set_waker(self.idx, cx.waker().clone());
            return Poll::Pending;
        }

        let stream = pin!(&mut source.stream);

        let v = ready!(stream.poll_next(cx));

        if let Some(ref v) = v {
            // We're going to return the value from the source stream to whoever
            // is executing this teed stream. Push a clone of the value to the
            // other side's buffer, so that it will be immediatley ready for
            // consumption by that stream.
            source.push_to_others(self.idx, v);
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
    states: Vec<Option<TeedStreamState<S::Item>>>,
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
            states: vec![],
        }
    }

    fn register_waker(&mut self) -> usize {
        self.states.push(Some(TeedStreamState::default()));
        self.states.len() - 1
    }

    #[must_use]
    pub fn tee(mut self) -> (TeedStream<S>, TeedStream<S>) {
        let left_idx = self.register_waker();
        let right_idx = self.register_waker();

        let source = Arc::new(Mutex::new(self));

        let left = TeedStream {
            source: source.clone(),
            idx: left_idx,
        };
        let right = TeedStream {
            source,
            idx: right_idx,
        };
        (left, right)
    }

    fn state_mut(&mut self, idx: usize) -> Option<&mut TeedStreamState<S::Item>> {
        self.states[idx].as_mut()
    }

    fn others_mut(
        &mut self,
        idx: usize,
    ) -> impl Iterator<Item = &mut Option<TeedStreamState<S::Item>>> {
        self.states
            .iter_mut()
            .enumerate()
            .filter_map(move |(i, v)| if i != idx { Some(v) } else { None })
    }

    fn others(&self, idx: usize) -> impl Iterator<Item = &Option<TeedStreamState<S::Item>>> {
        self.states
            .iter()
            .enumerate()
            .filter_map(move |(i, v)| if i != idx { Some(v) } else { None })
    }

    #[must_use]
    fn pop(&mut self, idx: usize) -> Option<S::Item> {
        self.state_mut(idx).and_then(|s| s.buffer.pop_front())
    }

    fn push_to_others(&mut self, idx: usize, v: &S::Item) {
        self.others_mut(idx).for_each(|s| {
            if let Some(s) = s {
                s.buffer.push_back(v.clone());
            }
        });
    }

    fn set_waker(&mut self, idx: usize, waker: Waker) {
        if let Some(state) = self.state_mut(idx) {
            if let Some(ref previous_waker) = state.waker
                && !waker.will_wake(previous_waker)
            {
                // If the incoming waker is not for the same task as our previously
                // stored waker, we *need* to wake the previous waker before
                // discarding it, otherwise we can deadlock. This can happen,
                // for example, when teeing an already teed future.
                previous_waker.wake_by_ref();
            }
            state.waker = Some(waker);
        }
    }

    fn any_other_buffer_full(&self, idx: usize) -> bool {
        self.others(idx).any(|s| {
            if let Some(s) = s {
                s.buffer_full(&self.buffer_limit)
            } else {
                false
            }
        })
    }

    fn wake_others(&mut self, idx: usize) {
        self.others_mut(idx).for_each(|s| {
            if let Some(s) = s
                && let Some(waker) = s.waker.take()
            {
                waker.wake();
            }
        });
    }

    fn drop_state(&mut self, idx: usize) {
        self.states[idx] = None;
    }
}

enum BufferLimit {
    Bytes(usize),
}

#[derive(Clone)]
struct TeedStreamState<I> {
    buffer: VecDeque<I>,
    waker: Option<Waker>,
}
impl<I> TeedStreamState<I> {
    fn buffer_full(&self, limit: &BufferLimit) -> bool {
        match limit {
            BufferLimit::Bytes(limit) => {
                let size = const { size_of::<I>() }.saturating_mul(self.buffer.len());
                size >= *limit
            }
        }
    }
}
impl<I> Default for TeedStreamState<I> {
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            waker: Default::default(),
        }
    }
}

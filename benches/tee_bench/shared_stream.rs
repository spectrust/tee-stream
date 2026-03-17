use futures::Stream;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

// NB: real implementation would limit buffer sizes, but no need to do that for
// speed testing
struct TeeSource<T, I> {
    stream: T,
    left_buffer: Option<VecDeque<I>>,
    right_buffer: Option<VecDeque<I>>,
}

enum Either {
    Left,
    Right,
}

pub struct TeedStream<T, I> {
    source: Arc<Mutex<TeeSource<T, I>>>,
    side: Either,
}

pub fn tee<T, I>(stream: T) -> (TeedStream<T, I>, TeedStream<T, I>)
where
    T: Stream<Item = I> + Unpin,
    I: Clone,
{
    let source = Arc::new(Mutex::new(TeeSource {
        stream,
        left_buffer: Some(Default::default()),
        right_buffer: Some(Default::default()),
    }));

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

impl<T, I> Stream for TeedStream<T, I>
where
    T: Stream<Item = I> + Unpin,
    I: Clone,
{
    type Item = I;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut source = self.source.lock();

        match self.side {
            Either::Left => {
                if let Some(left) = source.left_buffer.as_mut()
                    && let Some(v) = left.pop_front()
                {
                    return Poll::Ready(Some(v));
                }
            }
            Either::Right => {
                if let Some(right) = source.right_buffer.as_mut()
                    && let Some(v) = right.pop_front()
                {
                    return Poll::Ready(Some(v));
                }
            }
        }

        let stream = std::pin::pin!(&mut source.stream);
        let next = stream.poll_next(cx);

        let next = match next {
            Poll::Pending => next,
            Poll::Ready(None) => next,
            Poll::Ready(Some(ref v)) => {
                match self.side {
                    Either::Left => {
                        if let Some(right) = source.right_buffer.as_mut() {
                            right.push_back(v.clone());
                        }
                    }
                    Either::Right => {
                        if let Some(left) = source.left_buffer.as_mut() {
                            left.push_back(v.clone());
                        }
                    }
                }
                next
            }
        };
        drop(source);
        next
    }
}

impl<T, I> Drop for TeedStream<T, I> {
    fn drop(&mut self) {
        let mut source = self.source.lock();
        match self.side {
            Either::Left => {
                source.left_buffer = None;
            }
            Either::Right => {
                source.right_buffer = None;
            }
        };
    }
}

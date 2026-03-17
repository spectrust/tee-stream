use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use tokio::sync::{
    Notify,
    mpsc::{self, Receiver},
};

pub struct StreamReceiver<I> {
    notifier: Arc<Notify>,
    rx: Receiver<I>,
}

impl<I> Stream for StreamReceiver<I> {
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.notifier.notify_one();
        self.rx.poll_recv(cx)
    }
}

pub fn tee<T, I>(mut stream: T) -> (StreamReceiver<I>, StreamReceiver<I>)
where
    T: Stream<Item = I> + Unpin + Send + 'static,
    I: Clone + Send + 'static,
{
    let n = Arc::new(Notify::new());

    let (tx1, rx1) = mpsc::channel::<I>(1000);
    let (tx2, rx2) = mpsc::channel::<I>(1000);

    let fut_n = n.clone();
    tokio::spawn(async move {
        let n = fut_n;
        loop {
            n.notified().await;

            let next = stream.next().await;

            if let Some(next) = next {
                if !tx1.is_closed() {
                    tx1.send(next.clone()).await.unwrap();
                }

                if !tx2.is_closed() {
                    tx2.send(next).await.unwrap();
                }
            } else {
                // The stream is exhausted, exit the future
                break;
            }
        }
    });

    let left = StreamReceiver {
        notifier: n.clone(),
        rx: rx1,
    };

    let right = StreamReceiver {
        notifier: n,
        rx: rx2,
    };

    (left, right)
}

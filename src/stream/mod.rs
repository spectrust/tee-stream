mod tee;

use futures::Stream;
pub use tee::{TeeBuilder, TeedStream};

pub trait SpecStreamExt: Stream {
    fn tee(self) -> (TeedStream<Self>, TeedStream<Self>)
    where
        Self: Sized,
        Self::Item: Clone,
    {
        TeeBuilder::new(self).tee()
    }

    fn tee_builder(self) -> TeeBuilder<Self>
    where
        Self: Sized,
        Self::Item: Clone,
    {
        TeeBuilder::new(self)
    }
}

impl<T> SpecStreamExt for T where T: Stream {}

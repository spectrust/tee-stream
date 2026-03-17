// ::START CLIPPY LINTS::
// -------------------------------------------------------------------
// Non-default Lints
// -------------------------------------------------------------------
// This block is auto-generated. Please do not edit it directly! If you would
// like to make changes, either adjust the global lint settings in the
// root-level Cargo.toml, override at your own package's Cargo.toml,
// or, where you need build-level config, update in `scripts/insert-clippy-lints.sh`.
// -------------------------------------------------------------------
// Always allow printing in test code
#![cfg_attr(test, allow(clippy::dbg_macro))]
#![cfg_attr(test, allow(clippy::print_stdout))]
#![cfg_attr(test, allow(clippy::print_stderr))]
// -------------------------------------------------------------------
// ::END CLIPPY LINTS::

mod shared_source_with_vec;
mod shared_stream;
mod task_push;

use criterion::{Criterion, criterion_group, criterion_main};
use futures::{StreamExt, stream};
use lib_futures::SpecStreamExt;
use tokio::runtime;

fn benchmark_tees(c: &mut Criterion) {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    {
        let mut group = c.benchmark_group("tee implementations");

        group.bench_function("shared stream", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = shared_stream::tee(stream);

                // Consume both streams in true parallel.

                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("task push", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = task_push::tee(stream);

                // Consume both streams in true parallel.

                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("task push: shared vec backing", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = shared_source_with_vec::TeeBuilder::new(stream).tee();

                // Consume both streams in true parallel.

                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        // This is where we would really expect to potentially see benefits, but
        // it winds up being significantly slower than the chosen implementation
        group.bench_function("task push: shared vec backing: double tee", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = shared_source_with_vec::TeeBuilder::new(stream).tee();
                let (left1, left2) = shared_source_with_vec::TeedStream::tee(left);
                let (right1, right2) = shared_source_with_vec::TeedStream::tee(right);

                // Consume all four streams in parallel.
                let h1 =
                    tokio::spawn(async move { left1.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { left2.fold(0, async |acc, i| acc.max(i)).await });
                let h3 =
                    tokio::spawn(async move { right1.fold(0, async |acc, i| acc.max(i)).await });
                let h4 =
                    tokio::spawn(async move { right2.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
                assert_eq!(1_000_000, h3.await.unwrap());
                assert_eq!(1_000_000, h4.await.unwrap());
            });
        });
    }

    {
        let mut group = c.benchmark_group("actual shared stream implementation");

        group.bench_function("parallel consumption", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().tee();

                // Consume both streams in true parallel.

                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("unlimited buffer", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().with_unlimited_buffer().tee();

                // Consume both streams in true parallel.

                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("drop one stream", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().tee();

                // Drop one stream immediately.
                drop(right);

                // Consume the remaining stream.
                assert_eq!(1_000_000, left.fold(0, async |acc, i| acc.max(i)).await);
            });
        });

        group.bench_function("double tee", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().tee();
                let (left1, left2) = left.tee();
                let (right1, right2) = right.tee();

                // Consume all four streams in parallel.
                let h1 =
                    tokio::spawn(async move { left1.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { left2.fold(0, async |acc, i| acc.max(i)).await });
                let h3 =
                    tokio::spawn(async move { right1.fold(0, async |acc, i| acc.max(i)).await });
                let h4 =
                    tokio::spawn(async move { right2.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
                assert_eq!(1_000_000, h3.await.unwrap());
                assert_eq!(1_000_000, h4.await.unwrap());
            });
        });

        group.bench_function("buffered tee", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().with_max_buffer_len(1_000).tee();

                // Consume both streams in true parallel.
                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("buffered tee very small buffer", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().with_max_buffer_len(10).tee();

                // Consume both streams in true parallel.
                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });

        group.bench_function("buffered tee buffer of one", |b| {
            b.to_async(&rt).iter(async || {
                let stream = stream::iter(1..=1_000_000);

                let (left, right) = stream.tee_builder().with_max_buffer_len(1).tee();

                // Consume both streams in true parallel.
                let h1 = tokio::spawn(async move { left.fold(0, async |acc, i| acc.max(i)).await });
                let h2 =
                    tokio::spawn(async move { right.fold(0, async |acc, i| acc.max(i)).await });

                assert_eq!(1_000_000, h1.await.unwrap());
                assert_eq!(1_000_000, h2.await.unwrap());
            });
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(200);
    targets = benchmark_tees
);
criterion_main!(benches);

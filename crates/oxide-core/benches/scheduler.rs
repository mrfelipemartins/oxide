use criterion::{Criterion, criterion_group, criterion_main};
use oxide_core::{ClaimRequest, Engine, StreamConfig, SubmitWork};

fn submit_and_claim(c: &mut Criterion) {
    c.bench_function("submit_1000_claim_1000", |b| {
        b.iter(|| {
            let mut engine = Engine::new();
            engine.create_stream(StreamConfig::new("bench")).unwrap();
            for i in 0..1_000 {
                let mut work = SubmitWork::new("bench", format!("payload-{i}").into_bytes());
                work.priority = i % 16;
                work.lane_key = Some(format!("lane-{}", i % 128));
                engine.submit(work).unwrap();
            }
            let mut claimed = 0;
            while claimed < 1_000 {
                let (_, batch) = engine
                    .claim(ClaimRequest {
                        stream: "bench".to_owned(),
                        consumer_group: "workers".to_owned(),
                        worker_id: "bench".to_owned(),
                        max: 128,
                        lease_ms: 30_000,
                    })
                    .unwrap();
                if batch.is_empty() {
                    break;
                }
                claimed += batch.len();
            }
            claimed
        });
    });
}

criterion_group!(benches, submit_and_claim);
criterion_main!(benches);

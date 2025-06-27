#[macro_export]
macro_rules! timeit {
    ($label:expr, $block:block) => {{
        let start = std::time::Instant::now();
        let res = { $block };
        let elapsed = start.elapsed();
        info!("{} in {:.2?}", $label, elapsed);
    }}
}


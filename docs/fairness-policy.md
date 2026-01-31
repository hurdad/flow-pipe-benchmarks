# Fairness Policy

This benchmark suite is intended to provide comparable results across Flow-Pipe and Spark by keeping inputs, configurations, and measurement practices consistent.

## Principles

- **Same data, same schema**: Both engines operate on the same dataset and schema definitions.
- **Fixed workload definitions**: Query logic is defined once per pipeline and re-used across engines.
- **Consistent warmup + measurement**: Each benchmark specifies warmup and measured runs in `benchmarks/*.yaml`.
- **Documented configuration**: Hardware assumptions and configuration overrides are captured in `configs/` and in result notes.

## Practical guidance

- Keep the dataset location identical across engines, or explicitly pass overrides when running benchmarks.
- Avoid tuning one engine without making the change visible (e.g., document or version control configuration changes).
- Run on an otherwise idle host whenever possible to minimize external contention.
- Record the software versions and command-line invocations used for each run.

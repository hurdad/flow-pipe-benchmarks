# Hardware Setup

Benchmarks are sensitive to CPU, memory, and storage performance. Use this guide to document the environment in which results were produced.

## Baseline template

The repository includes a starter configuration file:

- `configs/hardware/8core-32gb.yaml`

Update or duplicate this file to reflect the actual machine used for a run. Capture details such as:

- CPU model and core count
- Memory capacity
- Storage type (NVMe, SSD, HDD)
- Filesystem type

## Recommendations

- Prefer local SSD/NVMe storage for consistent IO throughput.
- Disable power-saving modes that reduce CPU frequency during sustained workloads.
- Ensure the host has enough memory to avoid swapping during query execution.

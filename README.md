# Hackathon Evaluation

This project provides a framework for evaluating and comparing the performance of various frameworks for bigdata. The evaluation is containerized using Docker to ensure consistent testing environments.

## Project Structure

```

## Prerequisites

- Docker installed and running
- Bash shell environment
- At least 8GB RAM available
- At least 4 CPU cores available

## Resource Requirements

- Memory: 8GB (configurable)
- CPUs: 4 cores (configurable)

## Quick Start

1. Clone the repository:

```bash
git clone <repository-url>
cd hackathon-evaluation
```

2. Make the evaluation script executable:

```bash
chmod +x evaluation.sh
```

3. Run the evaluation:

- For PySpark only:

```bash
./evaluation.sh --pyspark
```

- For Dask only:

```bash
./evaluation.sh --dask
```

- For both frameworks:

```bash
./evaluation.sh
```

## Components

### 1. Dockerfiles

- `Dockerfile.spark`: Sets up PySpark environment with Python 3.10
- `Dockerfile.dask`: Sets up Dask environment with Python 3.10

### 2. Processing Scripts

- `generate_sample_data.py`: Generates sample dataset for testing
- `process_pyspark.py`: Implements data processing using PySpark
- `process_dask.py`: Implements data processing using Dask

### 3. Evaluation Script

`evaluation.sh` provides:

- Resource-limited container execution
- Framework-specific testing
- Performance metrics collection
- Docker environment cleanup

## Configuration

Resource limits can be modified in `evaluation.sh`:

```bash
MEMORY="8g"  # Memory limit
CPUS="4"     # CPU limit
```

## Output

Results are stored in the `output/` directory:

- Framework-specific metrics
- Processing statistics
- Performance comparisons

## Docker Images

### PySpark Image

- Base: Python 3.10-slim
- Key packages:
  - pyspark==3.5.0
  - pandas==2.1.1
  - pyarrow==14.0.1

### Dask Image

- Base: Python 3.10-slim
- Key packages:
  - dask[complete]==2024.1.1
  - pandas==2.1.1
  - pyarrow==14.0.1

## Troubleshooting

1. If Docker fails to build:

```bash
docker system prune -f
./evaluation.sh --pyspark  # or --dask
```

2. Check Docker resources:

```bash
docker system df
docker system info
```

3. Verify file permissions:

```bash
ls -l evaluation.sh
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request


## Authors

Rahul Rao

from fastapi import FastAPI
import time
import socket
import random

app = FastAPI(title="Cloud VM Benchmarking API")

# -------------------------------------------------
# 1️⃣ CPU-INTENSIVE ENDPOINT
# -------------------------------------------------
def fib(n):
    if n <= 1:
        return n
    return fib(n-1) + fib(n-2)

@app.get("/cpu")
def cpu_workload(n: int = 35):
    start = time.time()
    result = fib(n)
    end = time.time()

    return {
        "workload": "CPU Intensive",
        "result": result,
        "execution_time_sec": end - start
    }

# -------------------------------------------------
# 2️⃣ MEMORY-INTENSIVE ENDPOINT
# -------------------------------------------------
@app.get("/memory")
def memory_workload(size: int = 200_000_000):
    start = time.time()

    arr = [1] * size
    total = sum(arr)

    end = time.time()

    return {
        "workload": "Memory Intensive",
        "array_size": size,
        "execution_time_sec": end - start
    }

# -------------------------------------------------
# 3️⃣ NETWORK-INTENSIVE ENDPOINT
# -------------------------------------------------
@app.get("/network")
def network_workload(size_mb: int = 500):
    start = time.time()

    data = b"x" * 1024 * 1024  # 1 MB
    total_bytes = 0

    for _ in range(size_mb):
        total_bytes += len(data)

    end = time.time()

    return {
        "workload": "Network Intensive (Simulated)",
        "data_mb": size_mb,
        "execution_time_sec": end - start
    }

# -------------------------------------------------
# 4️⃣ BALANCED WORKLOAD ENDPOINT
# -------------------------------------------------
@app.get("/balanced")
def balanced_workload(n: int = 400):
    start = time.time()

    # Memory
    A = [[random.random() for _ in range(n)] for _ in range(n)]
    B = [[random.random() for _ in range(n)] for _ in range(n)]

    # CPU
    C = [[0]*n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            for k in range(n):
                C[i][j] += A[i][k] * B[k][j]

    # Disk I/O
    with open("output.txt", "w") as f:
        f.write(str(C[0][0]))

    end = time.time()

    return {
        "workload": "Balanced",
        "matrix_size": n,
        "execution_time_sec": end - start
    }
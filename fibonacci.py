def fibonacci(n):
    """Return the first n Fibonacci numbers."""
    if n <= 0:
        return []
    if n == 1:
        return [0]
    seq = [0, 1]
    for _ in range(2, n):
        seq.append(seq[-1] + seq[-2])
    return seq


if __name__ == "__main__":
    print(fibonacci(10))

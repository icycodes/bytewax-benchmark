import math

def calculate_stats(acc):
    count, sum_x, sum_x2 = acc
    if count == 0:
        return 0.0, 0.0
    mean = sum_x / count
    # Population variance
    variance = (sum_x2 - (sum_x ** 2) / count) / count
    # Handle floating point inaccuracies
    if variance < 0:
        variance = 0.0
    return mean, math.sqrt(variance)

acc = (0, 0.0, 0.0)
acc = (acc[0] + 1, acc[1] + 20.0, acc[2] + 20.0**2)
print(calculate_stats(acc))

acc = (acc[0] + 1, acc[1] + 22.0, acc[2] + 22.0**2)
print(calculate_stats(acc))

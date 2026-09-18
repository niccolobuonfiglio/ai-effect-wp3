"""Exponential backoff shared by task retries and the circuit breaker.

A fixed delay makes a short blip as expensive as a real outage: a service that
is back after two seconds still costs the full retry and reset delay. Starting
small and doubling up to a cap keeps short interruptions cheap, while a long
outage still settles at the gentle, capped rate.
"""

# 2**62 is far beyond any cap in seconds; stop growing the exponent there.
_MAX_EXPONENT = 62


def backoff_delay(attempt: int, initial: float, maximum: float) -> float:
    """Delay before retry number ``attempt`` (1-based).

    Returns ``initial``, then twice that, four times, … never more than
    ``maximum``. With ``initial >= maximum`` every delay is ``maximum``, which
    is the old fixed-delay behaviour.
    """
    if attempt < 1:
        raise ValueError("attempt must be at least 1")
    if initial <= 0:
        raise ValueError("initial must be positive")
    if maximum <= 0:
        raise ValueError("maximum must be positive")

    if initial >= maximum:
        return maximum
    return min(maximum, initial * (2 ** min(attempt - 1, _MAX_EXPONENT)))

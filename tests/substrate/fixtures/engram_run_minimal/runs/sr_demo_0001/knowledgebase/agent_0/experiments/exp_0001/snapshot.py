# Candidate solution: cost-tuned weighted-random router (Python).
def route(request, backends, weights):
    """Pick a backend by cost-tuned weighted sampling."""
    import random
    total = sum(weights)
    if total <= 0:
        return backends[0]
    r = random.random() * total
    upto = 0.0
    for b, w in zip(backends, weights):
        upto += w
        if upto >= r:
            return b
    return backends[-1]

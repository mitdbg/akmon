# Candidate solution: greedy-cheapest-first router (Python).
def route(request, backends, costs):
    """Pick the backend with the lowest predicted cost."""
    if not backends:
        raise ValueError("no backends available")
    paired = list(zip(backends, costs))
    paired.sort(key=lambda x: x[1])
    return paired[0][0]

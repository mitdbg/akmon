"""AKB replay runner (#4279): replay one deployment's question set against a pinned setup.

Run file ``okg.replay.run.v1`` (config.py) -> setup record and digest
(setup.py) -> graph server started and pinned (serve.py) -> answers through
the OpenAI Responses API under a ledger cap (openai_client.py, ledger.py,
agent.py) -> judge passes with a verdict cache (judge.py) -> an
``okg.akb.measurement.v1`` record and the acceptance-gate inputs (measure.py)
-> a noise report between two runs (compare.py). Evidence that may be
committed goes through evidence.py. Entry point: ``python -m
okg.optimizer.replay``.

Generic: nothing here names a deployment, question set or prompt of its own
except the judge prompt it ports.
"""

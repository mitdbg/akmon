"""Proposer: turn a diagnosis into one change on an experiment branch (issue #4282).

``apply`` writes a validated, leak-checked change as deployment files (a
served-text branch layer, or a declarative cross-link rule with its
narrowing); ``loop`` is the capped model loop that chooses the change;
``driver`` builds the model's context, commits the change, creates the
experiment branch, publishes it through the normal publisher and returns
the deployment to its base. ``python -m okg.optimizer.propose`` runs it.
"""

"""Diagnose each miss and wasted call to the stage that can fix it (issue #4281).

``traces`` reads tries and misses into one format; ``waste`` labels wasted
work by rule (no model call); ``labeler`` labels each miss with a mechanism
class and a stage (one capped model call per miss); ``agreement`` compares
two labellings with Cohen's kappa. ``python -m okg.optimizer.diagnose`` runs
them.
"""

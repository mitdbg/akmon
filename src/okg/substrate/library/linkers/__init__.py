"""Substrate-library enrichers.

Each module here implements the
`okg.substrate.enrichers.Enricher` Protocol. They run after
Phase A node projection and before
`project_new_edges`, deriving edge_facts from the just-projected
`nodes_live`.
"""

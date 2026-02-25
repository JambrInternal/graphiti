"""
Neo4j vector search helpers using SEARCH INDEX syntax with safe fallback.
"""

from __future__ import annotations

import logging
from typing import Any

from graphiti_core.driver.driver import GraphProvider
from graphiti_core.graph_queries import (
    NEO4J_COMMUNITY_VECTOR_INDEX,
    NEO4J_EDGE_VECTOR_INDEX,
    NEO4J_ENTITY_VECTOR_INDEX,
)
from graphiti_core.models.edges.edge_db_queries import get_entity_edge_return_query
from graphiti_core.models.nodes.node_db_queries import (
    COMMUNITY_NODE_RETURN,
    get_entity_node_return_query,
)
from graphiti_core.search.search_filters import (
    SearchFilters,
    edge_search_filter_query_constructor,
    node_search_filter_query_constructor,
)

logger = logging.getLogger(__name__)

_VECTOR_SEARCH_MODE_ATTR = '_neo4j_vector_search_mode'
_VECTOR_SEARCH_FALLBACK_LOGGED_ATTR = '_neo4j_vector_search_fallback_logged'


def should_use_search_index(target: Any) -> bool:
    return getattr(target, _VECTOR_SEARCH_MODE_ATTR, 'search') != 'fallback'


def mark_search_index_fallback(target: Any, exc: Exception) -> None:
    if not getattr(target, _VECTOR_SEARCH_FALLBACK_LOGGED_ATTR, False):
        logger.warning('Neo4j SEARCH INDEX unavailable, falling back: %s', exc)
        setattr(target, _VECTOR_SEARCH_FALLBACK_LOGGED_ATTR, True)
    setattr(target, _VECTOR_SEARCH_MODE_ATTR, 'fallback')


def split_group_ids(group_ids: list[str] | None) -> tuple[str | None, list[str] | None]:
    if group_ids and len(group_ids) == 1:
        return group_ids[0], None
    return None, group_ids


def build_node_vector_search_query(
    search_filter: SearchFilters,
    group_ids: list[str] | None,
) -> tuple[str, dict[str, Any]]:
    search_group_id, post_group_ids = split_group_ids(group_ids)

    filter_queries, filter_params = node_search_filter_query_constructor(
        search_filter, GraphProvider.NEO4J
    )

    if post_group_ids is not None:
        filter_queries.append('n.group_id IN $group_ids')
        filter_params['group_ids'] = post_group_ids

    filter_query = ''
    if filter_queries:
        filter_query = ' WHERE ' + (' AND '.join(filter_queries))

    score_filter = ' WHERE score > $min_score'
    if filter_query:
        score_filter = filter_query + ' AND score > $min_score'

    search_where = ''
    search_params: dict[str, Any] = {}
    if search_group_id is not None:
        search_where = ' WHERE n.group_id = $group_id'
        search_params['group_id'] = search_group_id

    query = (
        """
        MATCH (n:Entity)
        SEARCH n IN (
            VECTOR INDEX """
        + NEO4J_ENTITY_VECTOR_INDEX
        + """
            FOR $search_vector"""
        + search_where
        + """
            LIMIT $limit
        ) SCORE AS score
        """
        + score_filter
        + """
        RETURN
        """
        + get_entity_node_return_query(GraphProvider.NEO4J)
        + """
        ORDER BY score DESC
        LIMIT $limit
        """
    )

    params = {**search_params, **filter_params}
    return query, params


def build_community_vector_search_query(
    group_ids: list[str] | None,
) -> tuple[str, dict[str, Any]]:
    search_group_id, post_group_ids = split_group_ids(group_ids)

    filter_query = ''
    filter_params: dict[str, Any] = {}
    if post_group_ids is not None:
        filter_query = ' WHERE c.group_id IN $group_ids'
        filter_params['group_ids'] = post_group_ids

    score_filter = ' WHERE score > $min_score'
    if filter_query:
        score_filter = filter_query + ' AND score > $min_score'

    search_where = ''
    search_params: dict[str, Any] = {}
    if search_group_id is not None:
        search_where = ' WHERE c.group_id = $group_id'
        search_params['group_id'] = search_group_id

    query = (
        """
        MATCH (c:Community)
        SEARCH c IN (
            VECTOR INDEX """
        + NEO4J_COMMUNITY_VECTOR_INDEX
        + """
            FOR $search_vector"""
        + search_where
        + """
            LIMIT $limit
        ) SCORE AS score
        """
        + score_filter
        + """
        RETURN
        """
        + COMMUNITY_NODE_RETURN
        + """
        ORDER BY score DESC
        LIMIT $limit
        """
    )

    params = {**search_params, **filter_params}
    return query, params


def build_edge_vector_search_query(
    search_filter: SearchFilters,
    group_ids: list[str] | None,
    source_node_uuid: str | None,
    target_node_uuid: str | None,
) -> tuple[str, dict[str, Any]]:
    search_group_id, post_group_ids = split_group_ids(group_ids)

    filter_queries, filter_params = edge_search_filter_query_constructor(
        search_filter, GraphProvider.NEO4J
    )

    if post_group_ids is not None:
        filter_queries.append('e.group_id IN $group_ids')
        filter_params['group_ids'] = post_group_ids

    if group_ids is not None:
        if source_node_uuid is not None:
            filter_params['source_uuid'] = source_node_uuid
            filter_queries.append('n.uuid = $source_uuid')

        if target_node_uuid is not None:
            filter_params['target_uuid'] = target_node_uuid
            filter_queries.append('m.uuid = $target_uuid')

    filter_query = ''
    if filter_queries:
        filter_query = ' WHERE ' + (' AND '.join(filter_queries))

    score_filter = ' WHERE score > $min_score'
    if filter_query:
        score_filter = filter_query + ' AND score > $min_score'

    search_where = ''
    search_params: dict[str, Any] = {}
    if search_group_id is not None:
        search_where = ' WHERE e.group_id = $group_id'
        search_params['group_id'] = search_group_id

    query = (
        """
        MATCH ()-[e:RELATES_TO]-()
        SEARCH e IN (
            VECTOR INDEX """
        + NEO4J_EDGE_VECTOR_INDEX
        + """
            FOR $search_vector"""
        + search_where
        + """
            LIMIT $limit
        ) SCORE AS score
        WITH e, score
        MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
        """
        + score_filter
        + """
        RETURN
        """
        + get_entity_edge_return_query(GraphProvider.NEO4J)
        + """
        ORDER BY score DESC
        LIMIT $limit
        """
    )

    params = {**search_params, **filter_params}
    return query, params

"""Derived learning outcome metrics."""

from __future__ import annotations

import sqlite3


def compute_learning_outcome_metrics(conn: sqlite3.Connection) -> dict[str, int]:
    same_gap = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
          SELECT update_target
          FROM learning_assets
          WHERE lifecycle='active'
            AND memory_class='gap'
            AND scope='project_local'
          GROUP BY update_target
          HAVING COUNT(DISTINCT json_extract(provenance_json, '$.project')) >= 2
        )
        """
    ).fetchone()
    adr_rediscussion = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
          SELECT update_target
          FROM learning_assets
          WHERE lifecycle='active'
            AND memory_class='adr'
            AND scope='project_local'
          GROUP BY update_target
          HAVING COUNT(DISTINCT json_extract(provenance_json, '$.project')) >= 2
        )
        """
    ).fetchone()
    knowledge_requery = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
          SELECT lpa.asset_key
          FROM learning_packet_assets lpa
          JOIN learning_assets la ON la.asset_key = lpa.asset_key
          JOIN learning_applications app ON app.packet_id = lpa.packet_id
          WHERE la.memory_class='knowledge'
          GROUP BY lpa.asset_key
          HAVING COUNT(app.application_id) >= 2
        )
        """
    ).fetchone()
    cross_client = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
          SELECT lpa.asset_key
          FROM learning_packet_assets lpa
          JOIN learning_applications app ON app.packet_id = lpa.packet_id
          GROUP BY lpa.asset_key
          HAVING COUNT(DISTINCT app.source_client) >= 2
        )
        """
    ).fetchone()
    return {
        "same_gap_recurrence": int(same_gap["count"]),
        "knowledge_requery": int(knowledge_requery["count"]),
        "adr_rediscussion": int(adr_rediscussion["count"]),
        "cross_client_consistency": int(cross_client["count"]),
    }

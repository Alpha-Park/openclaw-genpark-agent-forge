# -*- coding: utf-8 -*-
"""
OpenClaw GenPark Agent Forge Client
Multi-agent DAG workflow compiler with Kahn's topological sort and Critical Path Method (CPM).
Zero external dependencies -- Python 3.9+ standard library only.
"""

from typing import Dict, Any, List

class OpenClawGenParkAgentForge:
    """
    Agent DAG Orchestration Compiler:
    Validates task dependency directed acyclic graphs (DAG),
    computes topological execution stages for maximum parallel concurrency,
    and identifies the Critical Path Method (CPM) bottlenecks.
    """

    def __init__(self):
        pass

    def compile_workflow_dag(
        self,
        task_nodes: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Compiles and optimizes multi-agent task graph.
        """
        if task_nodes is None:
            task_nodes = [
                {"id": "T1", "name": "Ingest Market Data", "duration_ms": 450, "depends_on": []},
                {"id": "T2", "name": "Extract Entity Sentiment", "duration_ms": 620, "depends_on": ["T1"]},
                {"id": "T3", "name": "Audit Financial Liquidity", "duration_ms": 800, "depends_on": ["T1"]},
                {"id": "T4", "name": "Synthesize Quantitative Thesis", "duration_ms": 510, "depends_on": ["T2", "T3"]},
                {"id": "T5", "name": "Execute Trade Order", "duration_ms": 280, "depends_on": ["T4"]}
            ]

        # 1. Build adjacency list and in-degree map
        adj = {t["id"]: [] for t in task_nodes}
        in_degree = {t["id"]: 0 for t in task_nodes}
        durations = {t["id"]: t.get("duration_ms", 100) for t in task_nodes}

        for t in task_nodes:
            for dep in t.get("depends_on", []):
                if dep in adj:
                    adj[dep].append(t["id"])
                    in_degree[t["id"]] += 1

        # 2. Kahn's Algorithm for Topological Sort & Staging
        zero_in = [tid for tid, deg in in_degree.items() if deg == 0]
        stages = []
        ordered_ids = []

        while zero_in:
            stages.append(list(zero_in))
            next_zero = []
            for u in zero_in:
                ordered_ids.append(u)
                for v in adj[u]:
                    in_degree[v] -= 1
                    if in_degree[v] == 0:
                        next_zero.append(v)
            zero_in = next_zero

        has_cycle = (len(ordered_ids) != len(task_nodes))
        if has_cycle:
            return {
                "is_dag_valid": False,
                "error": "Cyclical dependency detected in agent task graph; execution impossible."
            }

        # 3. Critical Path Method (CPM): Earliest Completion Time
        earliest_finish = {t["id"]: 0 for t in task_nodes}
        for stage in stages:
            for tid in stage:
                t_obj = next(t for t in task_nodes if t["id"] == tid)
                dep_finishes = [earliest_finish[d] for d in t_obj.get("depends_on", [])]
                start_t = max(dep_finishes) if dep_finishes else 0
                earliest_finish[tid] = start_t + durations[tid]

        critical_path_duration_ms = max(earliest_finish.values()) if earliest_finish else 0

        # Backtrack critical path
        critical_path = []
        curr = max(earliest_finish, key=earliest_finish.get) if earliest_finish else None
        while curr:
            critical_path.append(curr)
            curr_obj = next(t for t in task_nodes if t["id"] == curr)
            deps = curr_obj.get("depends_on", [])
            if deps:
                curr = max(deps, key=lambda d: earliest_finish[d])
            else:
                curr = None
        critical_path.reverse()

        return {
            "is_dag_valid": True,
            "total_tasks_count": len(task_nodes),
            "parallel_execution_stages": stages,
            "topological_sequence": ordered_ids,
            "critical_path_nodes": critical_path,
            "minimum_project_duration_ms": critical_path_duration_ms,
            "concurrency_speedup_factor": round(sum(durations.values()) / max(1, critical_path_duration_ms), 2)
        }

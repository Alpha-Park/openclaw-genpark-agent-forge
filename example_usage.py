# -*- coding: utf-8 -*-
"""
Example usage of OpenClawGenParkAgentForge
Demonstrates DAG workflow compilation and Critical Path identification.
"""

from client import OpenClawGenParkAgentForge

def main():
    forge = OpenClawGenParkAgentForge()

    res = forge.compile_workflow_dag()

    print("================================================================")
    print("OpenClaw GenPark Agent Forge - DAG Workflow Compilation")
    print("================================================================\n")
    print(f"DAG Validity          : {res['is_dag_valid']}")
    print(f"Total Tasks           : {res['total_tasks_count']}")
    print(f"Min Total Duration    : {res['minimum_project_duration_ms']} ms")
    print(f"Concurrency Speedup   : {res['concurrency_speedup_factor']}x faster than sequential\n")
    print(f"Critical Path Nodes   : {' -> '.join(res['critical_path_nodes'])}")
    print("Parallel Execution Stages:")
    for idx, stage in enumerate(res['parallel_execution_stages']):
        print(f"  Stage {idx + 1}: {', '.join(stage)}")

if __name__ == "__main__":
    main()

from hera.shared import global_config
from hera.workflows import Steps, Workflow, WorkflowsService, script

global_config.verify_ssl = False


@script()
def echo(message: str) -> None:
    print(message)


with Workflow(
    generate_name="hello-world-",
    entrypoint="steps",
    namespace="argo",
    workflows_service=WorkflowsService(host="https://localhost:2746"),
) as w:
    with Steps(name="steps"):
        echo(arguments={"message": "Hello world!"})

submitted_workflow = w.create()
print(f"Workflow at https://localhost:2746/workflows/argo/{submitted_workflow.metadata.name}")

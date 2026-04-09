from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from reslib.constants import HpaMetricSourceEnum, K8DeploymentKind, WorkloadStatusEnum


class ProbeHttpGet(BaseModel):
    """Probe HTTP get model."""

    path: Optional[str] = None
    port: Optional[Any] = None
    host: Optional[str] = None
    scheme: Optional[str] = None


class ContainerHealthSpec(BaseModel):
    """Container health model."""

    readiness: Optional[ProbeHttpGet] = None
    liveness: Optional[ProbeHttpGet] = None
    startup: Optional[ProbeHttpGet] = None


class ResourceRequirements(BaseModel):
    """
    Kubernetes-like resource requirements.
    Values are strings because K8s quantities are strings: "250m", "1", "128Mi",
    "1Gi", etc.
    """

    requests: Dict[str, Any] = Field(default_factory=dict)
    limits: Dict[str, Any] = Field(default_factory=dict)


class ContainerSpec(BaseModel):
    """Container specifications."""

    name: str
    resources: Optional[ResourceRequirements] = None
    health: Optional[ContainerHealthSpec] = None


class K8Condition(BaseModel):
    """Kubernetes condition model."""

    type: str
    status: str
    reason: Optional[str] = None
    message: Optional[str] = None
    last_transition_time: Optional[datetime] = None


class HPAMetricSpec(BaseModel):
    """Horizontal Pod Autoscaler metrics."""

    type: HpaMetricSourceEnum
    resource: Dict[Any, Any] = Field(
        default_factory=dict, description="HPA resource dict"
    )


class HPAConfig(BaseModel):
    """Horizontal Pod Autoscaler configuration."""

    name: str
    min_replicas: int = Field(..., ge=1, description="Minimum number of replicas")
    max_replicas: int = Field(..., ge=1, description="Maximum number of replicas")
    metrics: List[HPAMetricSpec] = Field(..., description="HPA metrics")


class PDBConfig(BaseModel):
    """Pod Disruption Budget configuration."""

    min_available: Optional[int] = Field(
        default=None, ge=0, description="Minimum number of pods that must be available"
    )
    max_unavailable: Optional[int] = Field(
        default=None, ge=0, description="Maximum number of pods that can be unavailable"
    )


class WorkloadSpec(BaseModel):
    """Desired configuration of a workload."""

    name: str = Field(..., description="Workload name")
    service_name: Optional[str] = Field(
        default=None, description="Primary Service name targeting this workload"
    )
    kind: K8DeploymentKind = Field(..., description="Kubernetes workload type")
    replicas: int = Field(..., ge=0, description="Desired number of replicas")
    hpa: Optional[HPAConfig] = Field(
        default=None, description="HPA configuration if present"
    )
    labels: Optional[Dict[str, str]] = Field(
        default=None,
        description="Pod labels to select pods belonging to this workload",
    )
    containers: Optional[List[ContainerSpec]] = Field(
        default=None, description="Pod containers"
    )


class WorkloadPolicies(BaseModel):
    """Workload policy constraints like PDB."""

    pdb: Optional[PDBConfig] = Field(
        default=None, description="Pod Disruption Budget if present"
    )


class WorkloadRuntimeState(BaseModel):
    """Observed runtime state of a workload, used for stability checks."""

    ready_replicas: int = Field(..., ge=0, description="Number of ready replicas")
    status: WorkloadStatusEnum = Field(..., description="Workload status")
    generation: Optional[int] = Field(
        default=None, description="Generation of the workload spec currently desired"
    )
    observed_generation: Optional[int] = Field(
        default=None,
        description=(
            "Generation of the workload spec currently applied by the controller"
        ),
    )
    conditions: Optional[List[K8Condition]] = Field(
        default=None, description="Workload conditions"
    )


class WorkloadState(BaseModel):
    """Aggregated workload model combining spec, policies, and runtime status."""

    spec: WorkloadSpec
    policies: Optional[WorkloadPolicies] = None
    runtime: Optional[WorkloadRuntimeState] = None


class NamespaceState(BaseModel):
    """State of workloads within a namespace."""

    name: str = Field(..., description="Namespace name")
    workloads: List[WorkloadState] = Field(
        default_factory=list, description="Workloads list"
    )


class DiscoveryNamespaceConfigSchema(BaseModel):
    """Agent discovery namespace config schema"""

    namespaces: List[str] = Field(
        default_factory=list, description="List of namespaces name"
    )

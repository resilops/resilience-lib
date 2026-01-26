from datetime import datetime
from typing import Dict, Optional

from pydantic import BaseModel, Field

from reslib.constants import K8DeploymentKind


class HPAConfig(BaseModel):
    """Horizontal Pod Autoscaler configuration."""

    min_replicas: int = Field(..., ge=1, description="Minimum number of replicas")
    max_replicas: int = Field(..., ge=1, description="Maximum number of replicas")


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
    kind: K8DeploymentKind = Field(..., description="Kubernetes workload type")
    replicas: int = Field(..., ge=0, description="Desired number of replicas")
    hpa: Optional[HPAConfig] = Field(
        default=None, description="HPA configuration if present"
    )


class WorkloadPolicies(BaseModel):
    """Workload policy constraints like PDB."""

    pdb: Optional[PDBConfig] = Field(
        default=None, description="Pod Disruption Budget if present"
    )


class WorkloadStatus(BaseModel):
    """Observed runtime state of a workload, used for stability checks."""

    ready_replicas: int = Field(..., ge=0, description="Number of ready replicas")
    serving_traffic: Optional[bool] = Field(
        default=None, description="Whether the workload is currently serving traffic"
    )
    reconciling: Optional[bool] = Field(
        default=None,
        description=(
            "Whether Kubernetes is actively reconciling this "
            "workload (rollout or scaling)"
        ),
    )
    restart_events: int = Field(
        default=0,
        ge=0,
        description="Number of pod restart events observed during the stability window",
    )
    spec_generation: Optional[int] = Field(
        default=None, description="Generation of the workload spec currently desired"
    )
    spec_applied_generation: Optional[int] = Field(
        default=None,
        description=(
            "Generation of the workload spec currently applied by the controller"
        ),
    )
    last_transition_time: Optional[datetime] = Field(
        default=None, description="Last time the workload status changed"
    )


class WorkloadState(BaseModel):
    """Aggregated workload model combining spec, policies, and runtime status."""

    spec: WorkloadSpec
    policies: Optional[WorkloadPolicies] = None
    status: Optional[WorkloadStatus] = None


class NamespaceState(BaseModel):
    """State of workloads within a namespace."""

    name: str = Field(..., description="Namespace name")
    labels: Dict[str, str] = Field(default_factory=dict, description="Namespace labels")
    workloads: Dict[str, WorkloadState] = Field(
        default_factory=dict, description="Workloads keyed by name"
    )


class ClusterState(BaseModel):
    """Snapshot of Kubernetes cluster state."""

    name: str = Field(..., description="Cluster name or identifier")
    namespaces: Dict[str, NamespaceState] = Field(
        default_factory=dict, description="Namespaces keyed by name"
    )

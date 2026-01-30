from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional

from kubernetes.client import (
    V1Deployment,
    V1PodDisruptionBudget,
    V2HorizontalPodAutoscaler,
)


@dataclass(frozen=True)
class NamespaceSnapshot:
    """
    Immutable snapshot of namespace-scoped workload policies.

    Attributes:
        hpas:
            Mapping of workload (deployment) name to its HorizontalPodAutoscaler.
        pdbs:
            List of PodDisruptionBudgets defined in the namespace.
    """

    hpas: Dict[str, V2HorizontalPodAutoscaler]
    pdbs: List[V1PodDisruptionBudget]

    def get_hpa(self, deployment: V1Deployment) -> Optional[V2HorizontalPodAutoscaler]:
        """
        Return the HorizontalPodAutoscaler for a given Deployment, if it exists.

        Args:
            deployment: The Deployment object.

        Returns:
            The matching HPA, or None if no HPA targets this deployment.
        """
        return self.hpas.get(deployment.metadata.name)

    def get_pdb(self, pod_labels: Mapping[str, str]) -> Optional[V1PodDisruptionBudget]:
        """
        Return the PodDisruptionBudget that applies to a workload's pods.

        A PDB is considered applicable if all labels in its selector are present
        in the given pod labels (i.e. selector ⊆ pod_labels).

        Args:
            pod_labels:
                Labels applied to pods created by the workload
                (usually deployment.spec.template.metadata.labels).

        Returns:
            The matching PodDisruptionBudget, or None if no PDB applies.
        """
        for pdb in self.pdbs:
            selector: Dict[str, str] = pdb.spec.selector.match_labels or {}
            if selector.items() <= pod_labels.items():
                return pdb

        return None

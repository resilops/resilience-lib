from pydantic import BaseModel, ConfigDict


class PDBConfiguration(BaseModel):
    """PDB guardrail configuration."""

    model_config = ConfigDict(extra="forbid")

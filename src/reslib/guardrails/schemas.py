from pydantic import BaseModel, Field


class PDBConfigurationAllowMissing(BaseModel):
    """Allow missing PDB configuration"""

    allow_missing_pdb: bool = Field(
        default=False, description="Allow missing PDB configuration"
    )

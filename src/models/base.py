from pydantic import BaseModel as PydanticBaseModel, ConfigDict

class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        use_attribute_docstrings = True,
    )


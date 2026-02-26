# file: engine/stage_registry.py

from stages import export_stage
from stages import load_stage
from stages import transform_stage
from stages import report_stage

STAGE_REGISTRY = {
    "export":      export_stage.run,
    "load":        load_stage.run,   # job yml에서 쓰는 이름
    "load_local":  load_stage.run,   # 하위 호환
    "transform":   transform_stage.run,
    "report":      report_stage.run,
}

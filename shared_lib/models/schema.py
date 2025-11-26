from datetime import datetime
from pydantic import BaseModel, conint

class MeterEvent(BaseModel):
    meter_id: int
    building_id: int
    timestamp: datetime
    power_kw: float
    voltage_v: float
    status: conint(ge=0, le=15)  
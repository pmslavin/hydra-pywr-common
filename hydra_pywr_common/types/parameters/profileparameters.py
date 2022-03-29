from hydra_pywr_common.types import PywrParameter
from hydra_pywr_common.types.mixins import ArbitraryDirectAttrs

class PywrRES_hourly_profile(PywrParameter, ArbitraryDirectAttrs):
    key = "RES_hourly_profile"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)

    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret


class PywrDemand_hourly_profile(PywrParameter, ArbitraryDirectAttrs):
    key = "Demand_hourly_profile"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)

    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret


class PywrDemand_hourly_profile_ELS(PywrParameter, ArbitraryDirectAttrs):
    key = "Demand_hourly_profile_ELS"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)

    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret


class PywrDemand_proj_rate_parameter(PywrParameter, ArbitraryDirectAttrs):
    key = "demand_proj_rate_parameter"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)

    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret

class PywrCurrentYearThresholdParameter(PywrParameter, ArbitraryDirectAttrs):
    key = "CurrentYearThresholdParameter"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)

    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret

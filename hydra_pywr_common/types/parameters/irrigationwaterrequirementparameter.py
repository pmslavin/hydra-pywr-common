from hydra_pywr_common.types import PywrParameter

class PywrIrrigationWaterRequirementParameter(PywrParameter):
    key = "irrigationwaterrequirement"
    hydra_data_type = "PYWR_PARAMETER"

    """
    def __init__(self, name, data, **kwargs):
        super().__init__(name)

        self.node = data["node"]
        self.flows = data["flows"]
        self.values = data["values"]
        if "__recorder__" in data:
            self.__recorder__ = data["__recorder__"]


    @property
    def value(self):
        ret =  { "type": self.key,
                 "node": self.node,
                 "flows": self.flows,
                 "values": self.values
               }

        if hasattr(self, "__recorder__"):
            ret.update({ "__recorder__": self.__recorder__ })

        return ret
    """

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)


    @property
    def value(self):
        ret = self.get_attr_values()
        ret.update( {"type": self.key} )
        return ret

PywrParameter.parameter_type_map["irrigationwaterrequirementparameter"] = PywrIrrigationWaterRequirementParameter

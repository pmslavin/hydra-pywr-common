from hydra_pywr_common.types import PywrParameter
from hydra_pywr_common.types.mixins import ArbitraryDirectAttrs

class PywrUnknownParameter(PywrParameter, ArbitraryDirectAttrs):
    key = "unknownparameter"
    hydra_data_type = "PYWR_PARAMETER"

    def __init__(self, name, data, **kwargs):
        super().__init__(name)
        self.add_attrs(data)


    @property
    def value(self):
        ret = self.get_attr_values()
        #ret.update( {"type": self.key} )
        return ret


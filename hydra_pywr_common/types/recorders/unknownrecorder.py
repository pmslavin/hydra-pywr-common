from hydra_pywr_common.types.mixins import ArbitraryDirectAttrs

from hydra_pywr_common.types import(
    PywrRecorder
)

class PywrUnknownRecorder(PywrRecorder, ArbitraryDirectAttrs):
    key = "unknownrecorder"
    hydra_data_type = "PYWR_RECORDER"

    def __init__(self, name, data):
        super().__init__(name)
        #self.add_attrs(data)
        self.key = data.get("key", self.__class__.key)
        self._data = data


    @property
    def value(self):
        #ret = self.get_attr_values()
        #ret.update({ "type": self.key })
        ret = self._data
        return ret

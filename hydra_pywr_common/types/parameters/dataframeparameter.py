from hydra_pywr_common.types import PywrParameter
from hydra_pywr_common.types.mixins import ArbitraryDirectAttrs

class PywrDataframeParameter(PywrParameter, ArbitraryDirectAttrs):
    key = "dataframeparameter"
    hydra_data_type = "PYWR_DATAFRAME"

    def __init__(self, name, argdata, **kwargs):
        super().__init__(name)

        if "data" in argdata:
            data = argdata.get("data")
            self.basekey = next(iter(data))  # The first key in data dict
            series = data[self.basekey]
            self.set_value(series)
        else:
            self.add_attrs(argdata)


    def set_value(self, data):
        self._value = data

    @property
    def value(self):
        if hasattr(self, "_value"):
            ret = { "type": self.key,
                    "data": { self.basekey: self._value}
                  }
        else:
            ret = self.get_attr_values()
            ret.update({ "type": self.key })

        # pandas_kwargs no longer accepted by Pywr, parse_dates
        # essential for dataframe_tools.align_and_resample
        ret.update({ "parse_dates": True })
        ret.pop("pandas_kwargs", None)

        return ret

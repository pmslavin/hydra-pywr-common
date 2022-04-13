import json
from collections import defaultdict
from numbers import Number
import pandas as pd

from hydra_pywr.exporter import PywrHydraExporter
from hydra_pywr_common.types.network import PywrNetwork

from pywrparser.types.network import PywrNetwork as NewPywrNetwork
from pywrparser.types import (
    PywrParameter,
    PywrRecorder,
    PywrTimestepper,
    PywrMetadata,
    PywrTable,
    PywrScenario,
    PywrNode,
    PywrEdge
)


PARAMETER_HYDRA_TYPE_MAP = {
    "aggregatedparameter": "PYWR_PARAMETER_AGGREGATED",
    "constantscenarioparameter": "PYWR_PARAMETER_CONSTANT_SCENARIO",
    "controlcurveindexparameter": "PYWR_PARAMETER_CONTROL_CURVE_INDEX",
    "controlcurveinterpolatedparameter": "PYWR_PARAMETER_CONTROL_CURVE_INTERPOLATED",
    "dataframeparameter": "PYWR_DATAFRAME",
    "indexedarrayparameter": "PYWR_PARAMETER_INDEXED_ARRAY",
    "monthlyprofileparameter": "PYWR_PARAMETER_MONTHLY_PROFILE"
}

RECORDER_HYDRA_TYPE_MAP = {
    "flowdurationcurvedeviationrecorder": "PYWR_RECORDER_FDC_DEVIATION"
}

class PywrTypeEncoder(json.JSONEncoder):
    def default(self, inst):
        if isinstance(inst, (PywrParameter, PywrRecorder)):
            return inst.data
        else:
            return json.JSONEncoder.default(self, inst)



"""
    PywrNetwork => Pywr_json
"""
class PywrJsonWriter():

    def __init__(self, network):
        self.network = network

    def as_dict(self):
        output = {}

        output["timestepper"] = self.process_timestepper()
        output["metadata"] = self.process_metadata()
        output["parameters"] = self.process_parameters()
        output["recorders"] = self.process_recorders()
        output["nodes"] = self.process_nodes()
        output["edges"] = self.process_edges()
        if self.network.tables:
            output["tables"] = self.process_tables()
        if self.network.scenarios:
            output["scenarios"] = self.process_scenarios()

        return output

    def as_json(self, **json_opts):
        output = self.as_dict()
        return json.dumps(output, **json_opts)


    def process_timestepper(self):
        timestepper = self.network.timestepper
        return timestepper.get_values()

    def process_metadata(self):
        metadata = self.network.metadata
        return { "title": metadata.title.value,
                 "description": metadata.description.value
               }

    def process_parameters(self):
        parameters = self.network.parameters
        return { ref: param.value for ref, param in parameters.items() }

    def process_recorders(self):
        recorders = self.network.recorders
        return { ref: rec.value for ref, rec in recorders.items() }

    def process_nodes(self):
        nodes = self.network.nodes
        return [ node.pywr_node for node in nodes.values() ]

    def process_edges(self):
        edges = self.network.edges
        return [ edge.value for edge in edges.values() ]

    def process_tables(self):
        tables = self.network.tables
        return { table_name: table.get_values() for table_name, table in tables.items() }

    def process_scenarios(self):
        scenarios = self.network.scenarios
        if isinstance(scenarios, list):
            scenario_values = scenarios
        elif isinstance(scenarios, dict):
            scenario_values = scenarios.values()

        return [ scenario.get_values() for scenario in scenario_values ]


"""
    PywrIntegratedNetwork => Pynsim config & Pywr json
"""
class PywrIntegratedJsonWriter():
    def __init__(self, network):
        self.network = network

    def as_dict(self):
        ww = PywrJsonWriter(self.network.water)
        ew = PywrJsonWriter(self.network.energy)

        self.water_output = ww.as_dict()
        self.energy_output = ew.as_dict()
        self.config = self.network.config.get_values()

        return { **self.config,
                 "water": self.water_output,
                 "energy": self.energy_output
               }


    def write_as_pynsim(self, pynsim_file="pynsim_model.json"):
        combined = self.as_dict()
        def _lookup_outfile(engine_name):
            engines = combined["config"]["engines"]
            f = filter(lambda e: e["name"] == engine_name, engines)
            return next(iter(f))["args"][0]

        outputs = {"engines": self.network.domains}

        for engine in self.network.domains:
            outfile = _lookup_outfile(engine)
            with open(outfile, 'w') as fp:
                json.dump(combined[engine], fp, indent=2)
                outputs[engine] = {"file": outfile}

        with open(pynsim_file, 'w') as fp:
            json.dump(combined["config"], fp, indent=2)
            outputs["config"] = pynsim_file

        return outputs


"""
    PywrNetwork => hydra_network
"""
def make_hydra_attr(name, desc=None):
    return { "name": name,
             "description": desc if desc else name
           }

class NewPywrHydraWriter():

    default_map_projection = None

    def __init__(self, network,
                       hydra = None,
                       hostname=None,
                       session_id=None,
                       user_id=None,
                       template_id=None,
                       project_id=None):
        self.hydra = hydra
        self.network = network
        self.hostname = hostname
        self.session_id = session_id
        self.user_id = user_id
        self.template_id = template_id
        self.project_id = project_id

        self._next_node_id = 0
        self._next_link_id = 0
        self._next_attr_id = 0


    def get_typeid_by_name(self, name):
        for t in self.template["templatetypes"]:
            if t["name"].lower() == name.lower():
                return t["id"]

    def get_hydra_network_type(self):
        for t in self.template["templatetypes"]:
            if t["resource_type"] == "NETWORK":
                return t

    def get_hydra_attrid_by_name(self, attr_name):
        if attr_name in self.template_attributes:
            return self.template_attributes[attr_name]

        for attr in self.hydra_attributes:
            if attr["name"] == attr_name:
                return attr["id"]

    def get_next_node_id(self):
        self._next_node_id -= 1
        return self._next_node_id

    def get_next_link_id(self):
        self._next_link_id -= 1
        return self._next_link_id

    def get_next_attr_id(self):
        self._next_attr_id -= 1
        return self._next_attr_id

    def get_node_by_name(self, name):
        for node in self.hydra_nodes:
            if node["name"] == name:
                return node

    def make_baseline_scenario(self, resource_scenarios):
        return { "name": "Baseline",
                 "description": "hydra-pywr Baseline scenario",
                 "resourcescenarios": resource_scenarios if resource_scenarios else []
               }


    def initialise_hydra_connection(self):
        if not self.hydra:
            from hydra_client.connection import JSONConnection
            self.hydra = JSONConnection(self.hostname, session_id=self.session_id, user_id=self.user_id)

        print(f"Retrieving template id '{self.template_id}'...")
        self.template = self.hydra.get_template(self.template_id)


    def build_hydra_network(self, projection=None, domain=None):
        if projection:
            self.projection = projection
        else:
            self.projection = self.network.metadata.data.get("projection")
            if not self.projection:
                self.projection = NewPywrHydraWriter.default_map_projection

        self.initialise_hydra_connection()

        self.network.attach_parameters()
        #self.network.detach_parameters()


        self.template_attributes = self.collect_template_attributes()
        self.hydra_attributes = self.register_hydra_attributes()

        """ Build network elements and resource_scenarios with datasets """
        self.hydra_nodes, node_scenarios = self.build_hydra_nodes()

        if domain:
            self.network_attributes, network_scenarios = self.build_network_descriptor_attributes(domain)
        else:
            self.network_attributes, network_scenarios = self.build_network_attributes()

        self.hydra_links, link_scenarios = self.build_hydra_links()
        paramrec_attrs, paramrec_scenarios = self.build_parameters_recorders()

        self.network_attributes += paramrec_attrs

        self.resource_scenarios = node_scenarios + network_scenarios + link_scenarios + paramrec_scenarios

        """ Create baseline scenario with resource_scenarios """
        baseline_scenario = self.make_baseline_scenario(self.resource_scenarios)

        """ Assemble complete network """
        network_name = self.network.metadata.data["title"]
        network_description = self.network.metadata.data["description"]
        self.network_hydratype = self.get_hydra_network_type()

        self.hydra_network = {
            "name": network_name,
            "description": network_description,
            "project_id": self.project_id,
            "nodes": self.hydra_nodes,
            "links": self.hydra_links,
            "layout": None,
            "scenarios": [baseline_scenario],
            "projection": self.projection,
            "attributes": self.network_attributes,
            "types": [{ "id": self.network_hydratype["id"], "child_template_id": self.template_id }]
        }
        return self.hydra_network

    def build_network_attributes(self):
        exclude_metadata_attrs = ("title", "description", "projection")
        hydra_network_attrs = []
        resource_scenarios = []

        for attr_name in self.network.timestepper.data:
            ra, rs = self.make_resource_attr_and_scenario(self.network.timestepper, f"timestepper.{attr_name}")
            hydra_network_attrs.append(ra)
            resource_scenarios.append(rs)

        for attr_name in (a for a in self.network.metadata.data if a not in exclude_metadata_attrs):
            ra, rs = self.make_resource_attr_and_scenario(self.network.metadata, f"metadata.{attr_name}")
            hydra_network_attrs.append(ra)
            resource_scenarios.append(rs)

        for table_name, table in self.network.tables.items():
            for attr_name in table.data:
                ra, rs = self.make_resource_attr_and_scenario(table, f"tbl_{table_name}.{attr_name}")
                hydra_network_attrs.append(ra)
                resource_scenarios.append(rs)

        scenario_data = [ scenario.data for scenario in self.network.scenarios ]
        if scenario_data:
            attr_name = "scenarios"
            ra, rs = self.make_direct_resource_attr_and_scenario(
                    {"scenarios": scenario_data},
                    attr_name,
                    "PYWR_SCENARIOS"
            )
            """
            dataset = { "name":  attr_name,
                        "type":  "PYWR_SCENARIOS",
                        "value": json.dumps({"scenarios": scenario_data}),
                        "metadata": "{}",
                        "unit": "-",
                        "hidden": 'N'
                      }

            local_attr_id = self.get_next_attr_id()
            resource_attribute = { "id": local_attr_id,
                                   "attr_id": self.get_hydra_attrid_by_name(attr_name),
                                   "attr_is_var": "N"
                                 }

            resource_scenario = { "resource_attr_id": local_attr_id,
                                  "dataset": dataset
                                }
            """
            hydra_network_attrs.append(ra)
            resource_scenarios.append(rs)

        return hydra_network_attrs, resource_scenarios

    def collect_template_attributes(self):
        template_attrs = {}
        for tt in self.template["templatetypes"]:
            for ta in tt["typeattrs"]:
                attr = ta["attr"]
                template_attrs[attr["name"]] = attr["id"]

        return template_attrs

    def register_hydra_attributes(self):
        timestepper_attrs = { 'timestepper.start', 'timestepper.end', 'timestepper.timestep'}
        excluded_attrs = { 'position', 'type' }
        pending_attrs = timestepper_attrs

        pending_attrs.add("scenarios")

        for node in self.network.nodes.values():
            for attr_name in node.data:
                pending_attrs.add(attr_name)

        for param_name in self.network.parameters:
            pending_attrs.add(param_name)

        for rec_name in self.network.recorders:
            pending_attrs.add(rec_name)

        for meta_attr in self.network.metadata.data:
            pending_attrs.add(f"metadata.{meta_attr}")

        for table_name, table in self.network.tables.items():
            for attr_name in table.data.keys():
                pending_attrs.add(f"tbl_{table_name}.{attr_name}")

        #for scenario in self.network.scenarios:
        #    pending_attrs.add(f"scenario_{scenario.name}")

        attrs = [ make_hydra_attr(attr_name) for attr_name in pending_attrs - excluded_attrs.union(set(self.template_attributes.keys())) ]

        return self.hydra.add_attributes(attrs)


    def make_resource_attr_and_scenario(self, element, attr_name, datatype=None):
        local_attr_id = self.get_next_attr_id()

        if isinstance(element, (PywrParameter, PywrRecorder)):
            resource_scenario = self.make_paramrec_resource_scenario(element, attr_name, local_attr_id)
        elif isinstance(element, (PywrMetadata, PywrTimestepper, PywrTable)):
            base, name = attr_name.split('.')
            resource_scenario = self.make_network_resource_scenario(element, name, local_attr_id)
        else:
            resource_scenario = self.make_resource_scenario(element, attr_name, local_attr_id)

        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        return resource_attribute, resource_scenario


    def make_direct_resource_attr_and_scenario(self, value, attr_name, hydra_datatype):

        local_attr_id = self.get_next_attr_id()

        dataset = { "name":  attr_name,
                    "type":  hydra_datatype,
                    "value": json.dumps(value),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        return resource_attribute, resource_scenario


    def make_network_resource_scenario(self, element, attr_name, local_attr_id):

        value = element.data[attr_name]
        hydra_datatype = self.lookup_hydra_datatype(value)

        dataset = { "name":  attr_name,
                    "type":  hydra_datatype,
                    "value": value,
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }
        return resource_scenario

    def make_paramrec_resource_scenario(self, element, attr_name, local_attr_id):

        value = element.data
        hydra_datatype = self.lookup_hydra_datatype(element)

        dataset = { "name":  element.name,
                    "type":  hydra_datatype,
                    "value": json.dumps(value),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }
        return resource_scenario

    def make_resource_scenario(self, element, attr_name, local_attr_id):

        value = element.data[attr_name]
        hydra_datatype = self.lookup_hydra_datatype(value)

        dataset = { "name":  attr_name,
                    "type":  hydra_datatype,
                    "value": json.dumps(value, cls=PywrTypeEncoder),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return resource_scenario


    def lookup_parameter_hydra_datatype(self, value):
        ptype = value.type
        if not ptype.endswith("parameter"):
            ptype += "parameter"

        return PARAMETER_HYDRA_TYPE_MAP.get(ptype, "PYWR_PARAMETER")

    def lookup_recorder_hydra_datatype(self, value):
        rtype = value.type
        if not rtype.endswith("recorder"):
            rtype += "recorder"

        return RECORDER_HYDRA_TYPE_MAP.get(rtype, "PYWR_RECORDER")



    def lookup_hydra_datatype(self, attr_value):
        if isinstance(attr_value, Number):
            return "SCALAR"
        elif isinstance(attr_value, list):
            return "ARRAY"
        elif isinstance(attr_value, dict):
            return "DATAFRAME"
        elif isinstance(attr_value, str):
            return "DESCRIPTOR"
        elif isinstance(attr_value, PywrParameter):
            return self.lookup_parameter_hydra_datatype(attr_value)
        elif isinstance(attr_value, PywrRecorder):
            return self.lookup_recorder_hydra_datatype(attr_value)

        """ TODO raise """
        breakpoint()


    def build_hydra_nodes(self):
        hydra_nodes = []
        resource_scenarios = []

        for node in self.network.nodes.values():
            resource_attributes = []

            exclude = ("name", "position", "type", "comment")

            for attr_name in node.data:
                if attr_name in exclude:
                    continue
                ra, rs = self.make_resource_attr_and_scenario(node, attr_name)
                if ra["attr_id"] == None:
                    breakpoint()
                resource_attributes.append(ra)
                resource_scenarios.append(rs)

            hydra_node = {}
            hydra_node["resource_type"] = "NODE"
            hydra_node["id"] = self.get_next_node_id()
            hydra_node["name"] = node.name
            if comment := node.data.get("comment"):
                hydra_node["description"] = comment
            hydra_node["layout"] = {}
            hydra_node["attributes"] = resource_attributes
            hydra_node["types"] = [{ "id": self.get_typeid_by_name(node.type.lower()),
                                     "child_template_id": self.template_id
                                  }]

            if "position" in node.data:
                #key = "geographic" if self.projection else "schematic"
                proj_data = node.data["position"]
                for coords in proj_data.values():
                    x, y = coords[0], coords[1]
                hydra_node["x"] = x
                hydra_node["y"] = y

            hydra_nodes.append(hydra_node)

        return hydra_nodes, resource_scenarios

    def build_hydra_links(self):
        hydra_links = []
        resource_scenarios = []

        for edge in self.network.edges:
            resource_attributes = []

            src = edge.data[0]
            dest = edge.data[1]
            name = f"{src} to {dest}"

            hydra_link = {}
            hydra_link["resource_type"] = "LINK"
            hydra_link["id"] = self.get_next_link_id()
            hydra_link["name"] = name
            hydra_link["node_1_id"] = self.get_node_by_name(src)["id"]
            hydra_link["node_2_id"] = self.get_node_by_name(dest)["id"]
            hydra_link["layout"] = {}
            hydra_link["resource_attributes"] = resource_attributes
            hydra_link["types"] = [{ "id": self.get_typeid_by_name("edge") }]

            hydra_links.append(hydra_link)

        return hydra_links, resource_scenarios

    def build_parameters_recorders(self):
        resource_attrs = []
        resource_scenarios = []

        for param_name, param in self.network.parameters.items():
            ra, rs = self.make_resource_attr_and_scenario(param, param_name)
            resource_attrs.append(ra)
            resource_scenarios.append(rs)

        for rec_name, rec in self.network.recorders.items():
            ra, rs = self.make_resource_attr_and_scenario(rec, rec_name)
            resource_attrs.append(ra)
            resource_scenarios.append(rs)

        return resource_attrs, resource_scenarios

    def build_network_descriptor_attributes(self, attr_key):

        attr_name = f"{attr_key}_data"
        attrs = [ make_hydra_attr(attr_name) ]
        self.hydra_attributes += self.hydra.add_attributes(attrs)

        scenarios = None
        tables = None

        timestepper = self.network.timestepper.data
        metadata = self.network.metadata.data
        tables = [ table.data for table in self.network.tables.values() ]
        scenarios = [ scenario.data for scenario in self.network.scenarios ]

        attr_data = {"timestepper": timestepper,
                     "metadata": metadata
                    }
        if tables:
            attr_data["tables"] = tables
        if scenarios:
            attr_data["scenarios"] = scenarios

        dataset = { "name":  attr_name,
                    "type":  "DESCRIPTOR",
                    "value": json.dumps(attr_data),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        local_attr_id = self.get_next_attr_id()
        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return [resource_attribute], [resource_scenario]

    def add_network_to_hydra(self):
        """ Pass network to Hydra"""
        self.hydra.add_network(self.hydra_network)


class PywrHydraWriter():

    default_map_projection = None

    def __init__(self, network,
                       hydra = None,
                       hostname=None,
                       session_id=None,
                       user_id=None,
                       template_id=None,
                       project_id=None):
        self.hydra = hydra
        self.network = network
        self.hostname = hostname
        self.session_id = session_id
        self.user_id = user_id
        self.template_id = template_id
        self.project_id = project_id

        self._next_node_id = 0
        self._next_link_id = 0
        self._next_attr_id = 0


    def get_typeid_by_name(self, name):
        for t in self.template["templatetypes"]:
            if t["name"] == name:
                return t["id"]

    def get_hydra_network_type(self):
        for t in self.template["templatetypes"]:
            if t["resource_type"] == "NETWORK":
                return t

    def get_hydra_attrid_by_name(self, attr_name):
        if attr_name in self.template_attributes:
            return self.template_attributes[attr_name]

        for attr in self.hydra_attributes:
            if attr["name"] == attr_name:
                return attr["id"]

    def get_next_node_id(self):
        self._next_node_id -= 1
        return self._next_node_id

    def get_next_link_id(self):
        self._next_link_id -= 1
        return self._next_link_id

    def get_next_attr_id(self):
        self._next_attr_id -= 1
        return self._next_attr_id

    def get_node_by_name(self, name):
        for node in self.hydra_nodes:
            if node["name"] == name:
                return node

    def make_baseline_scenario(self, resource_scenarios):
        return { "name": "Baseline",
                 "description": "hydra-pywr Baseline scenario",
                 "resourcescenarios": resource_scenarios if resource_scenarios else []
               }


    def initialise_hydra_connection(self):
        if not self.hydra:
            from hydra_client.connection import JSONConnection
            self.hydra = JSONConnection(self.hostname, session_id=self.session_id, user_id=self.user_id)

        print(f"Retrieving template id '{self.template_id}'...")
        self.template = self.hydra.get_template(self.template_id)


    def build_hydra_network(self, projection=None, domain=None):
        if projection:
            self.projection = projection
        else:
            self.projection = self.network.metadata.projection.value if hasattr(self.network.metadata, "projection") else PywrHydraWriter.default_map_projection

        self.initialise_hydra_connection()
        """ Register Hydra attributes """

        self.network.resolve_parameter_references()
        """
        self.network.resolve_recorder_references()
        try:
            self.network.resolve_backwards_parameter_references()
        except:
            pass
        try:
            self.network.resolve_backwards_recorder_references()
        except:
            pass
        self.network.speculative_forward_references()
        """

        self.template_attributes = self.collect_template_attributes()
        self.hydra_attributes = self.register_hydra_attributes()

        """ Build network elements and resource_scenarios with datasets """
        self.hydra_nodes, node_scenarios = self.build_hydra_nodes()

        if domain:
            self.network_attributes, network_scenarios = self.build_network_descriptor_attributes(domain)
        else:
            self.network_attributes, network_scenarios = self.build_network_attributes()

        self.hydra_links, link_scenarios = self.build_hydra_links()

        paramrec_attrs, paramrec_scenarios = self.build_parameters_recorders()

        self.network_attributes += paramrec_attrs

        self.resource_scenarios = node_scenarios + network_scenarios + link_scenarios + paramrec_scenarios

        """ Create baseline scenario with resource_scenarios """
        baseline_scenario = self.make_baseline_scenario(self.resource_scenarios)

        """ Assemble complete network """
        network_name = self.network.metadata.title.value
        self.network_hydratype = self.get_hydra_network_type()
        network_description = self.network.metadata.description.value

        self.hydra_network = {
            "name": network_name,
            "description": network_description,
            "project_id": self.project_id,
            "nodes": self.hydra_nodes,
            "links": self.hydra_links,
            "layout": None,
            "scenarios": [baseline_scenario],
            "projection": self.projection,
            "attributes": self.network_attributes,
            "types": [{ "id": self.network_hydratype["id"], "child_template_id": self.template_id }]
        }
        return self.hydra_network


    def add_network_to_hydra(self):
        """ Pass network to Hydra"""
        self.hydra.add_network(self.hydra_network)

    def collect_template_attributes(self):
        template_attrs = {}
        for tt in self.template["templatetypes"]:
            for ta in tt["typeattrs"]:
                attr = ta["attr"]
                template_attrs[attr["name"]] = attr["id"]

        return template_attrs

    def register_hydra_attributes(self):
        timestepper_attrs = { 'timestepper.start', 'timestepper.end', 'timestepper.timestep'}
        excluded_attrs = { 'position', 'intrinsic_attrs', 'type' }
        pending_attrs = timestepper_attrs

        for node in self.network.nodes.values():
            for attr_name in node.intrinsic_attrs:
                pending_attrs.add(attr_name)

        for param_name in self.network.parameters:
            pending_attrs.add(param_name)

        for rec_name in self.network.recorders:
            pending_attrs.add(rec_name)

        for meta_attr in self.network.metadata.intrinsic_attrs:
            pending_attrs.add(f"metadata.{meta_attr}")

        for table_name, table in self.network.tables.items():
            for attr_name in table.intrinsic_attrs:
                pending_attrs.add(f"tbl_{table_name}.{attr_name}")

        attrs = [ make_hydra_attr(attr_name) for attr_name in pending_attrs - excluded_attrs.union(set(self.template_attributes.keys())) ]

        return self.hydra.add_attributes(attrs)

    def build_parameters_recorders(self):
        resource_attrs = []
        resource_scenarios = []

        for param_name, param in self.network.parameters.items():
            ra, rs = self.make_resource_attr_and_scenario(param, param_name)
            resource_attrs.append(ra)
            resource_scenarios.append(rs)

        for rec_name, rec in self.network.recorders.items():
            ra, rs = self.make_resource_attr_and_scenario(rec, rec_name)
            resource_attrs.append(ra)
            resource_scenarios.append(rs)

        return resource_attrs, resource_scenarios


    def make_resource_attr_and_scenario(self, element, attr_name, datatype=None):
        local_attr_id = self.get_next_attr_id()
        resource_scenario = self.make_resource_scenario(element, attr_name, local_attr_id, datatype)
        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        return resource_attribute, resource_scenario


    def make_resource_scenario(self, element, attr_name, local_attr_id, datatype=None):
        if hasattr(element, "hydra_data_type") and element.hydra_data_type.startswith(("PYWR_PARAMETER", "PYWR_RECORDER")):
            dataset = self.make_paramrec_dataset(element)
        else:
            dataset = element.attr_dataset(attr_name)

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return resource_scenario


    def make_paramrec_dataset(self, element):

        dataset = { "name":  element.name,
                    "type":  element.hydra_data_type,
                    "value": json.dumps(element.value),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        return dataset


    def build_hydra_nodes(self):
        hydra_nodes = []
        resource_scenarios = []

        exclude_node_attrs = ('type', "Turbine", "cc_level", "Evap_max_flow")

        for node in self.network.nodes.values():
            resource_attributes = []

            # TODO Move this to node ctor path???
            for attr_name in filter(lambda a: a not in exclude_node_attrs, node.intrinsic_attrs):
                ra, rs = self.make_resource_attr_and_scenario(node, attr_name)
                if ra["attr_id"] == None:
                    breakpoint()
                resource_attributes.append(ra)
                resource_scenarios.append(rs)

            hydra_node = {}
            hydra_node["resource_type"] = "NODE"
            hydra_node["id"] = self.get_next_node_id()
            hydra_node["name"] = node.name
            if hasattr(node, "comment"):
                hydra_node["description"] = node.comment
            hydra_node["layout"] = {}
            hydra_node["attributes"] = resource_attributes
            hydra_node["types"] = [{ "id": self.get_typeid_by_name(node.key),
                                     "child_template_id": self.template_id
                                  }]

            if hasattr(node, "position") and node.position is not None:
                key = "geographic" if self.projection else "schematic"
                proj_data = node.position.value
                x, y = proj_data.get(key, (0,0))
                hydra_node["x"] = x
                hydra_node["y"] = y

            hydra_nodes.append(hydra_node)

        return hydra_nodes, resource_scenarios


    def build_network_attributes(self):
        exclude_metadata_attrs = ("title", "description", "projection")
        hydra_network_attrs = []
        resource_scenarios = []

        for attr_name in self.network.timestepper.intrinsic_attrs:
            ra, rs = self.make_resource_attr_and_scenario(self.network.timestepper, f"timestepper.{attr_name}")
            hydra_network_attrs.append(ra)
            resource_scenarios.append(rs)

        for attr_name in (a for a in self.network.metadata.intrinsic_attrs if a not in exclude_metadata_attrs):
            ra, rs = self.make_resource_attr_and_scenario(self.network.metadata, f"metadata.{attr_name}")
            hydra_network_attrs.append(ra)
            resource_scenarios.append(rs)

        for table_name, table in self.network.tables.items():
            for attr_name in table.intrinsic_attrs:
                ra, rs = self.make_resource_attr_and_scenario(table, f"tbl_{table_name}.{attr_name}")
                hydra_network_attrs.append(ra)
                resource_scenarios.append(rs)

        return hydra_network_attrs, resource_scenarios

    def build_network_descriptor_attributes(self, attr_key):

        attr_name = f"{attr_key}_data"
        attrs = [ make_hydra_attr(attr_name) ]
        self.hydra_attributes += self.hydra.add_attributes(attrs)

        timestepper = self.network.timestepper.get_values()
        metadata = self.network.metadata.get_values()
        tables = [ table.get_values() for table in self.network.tables.values() ]
        scenarios = None
        if hasattr(self.network.scenarios, "values"):
            scenarios = [ scenario.get_values() for scenario in self.network.scenarios.values() ]

        attr_data = {"timestepper": timestepper,
                     "metadata": metadata
                    }
        if tables:
            attr_data["tables"] = tables
        if scenarios:
            attr_data["scenarios"] = scenarios

        dataset = { "name":  attr_name,
                    "type":  "DESCRIPTOR",
                    "value": json.dumps(attr_data),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        local_attr_id = self.get_next_attr_id()
        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return [resource_attribute], [resource_scenario]


    def build_hydra_links(self):
        hydra_links = []
        resource_scenarios = []

        for edge in self.network.edges.values():
            resource_attributes = []

            hydra_link = {}
            hydra_link["resource_type"] = "LINK"
            hydra_link["id"] = self.get_next_link_id()
            hydra_link["name"] = edge.name
            hydra_link["node_1_id"] = self.get_node_by_name(edge.src)["id"]
            hydra_link["node_2_id"] = self.get_node_by_name(edge.dest)["id"]
            hydra_link["layout"] = {}
            hydra_link["resource_attributes"] = resource_attributes
            hydra_link["types"] = [{ "id": self.get_typeid_by_name(edge.key) }]

            hydra_links.append(hydra_link)

        return hydra_links, resource_scenarios

"""
    PywrIntegratedNetwork => Hydra
"""
class PywrHydraIntegratedWriter():

    def __init__(self, pin,
                       hostname=None,
                       session_id=None,
                       user_id=None,
                       water_template_id=None,
                       energy_template_id=None,
                       project_id=None):
        self.pin = pin
        self.hostname = hostname
        self.session_id = session_id
        self.user_id = user_id
        self.template_ids = (water_template_id, energy_template_id)
        self.project_id = project_id


    def get_hydra_network_types(self):
        types = []
        for template in self.templates:
            for t in template["templatetypes"]:
                if t["resource_type"] == "NETWORK":
                    types.append(t)

        return types

    def initialise_hydra_connection(self):
        from hydra_client.connection import JSONConnection
        self.hydra = JSONConnection(self.hostname, session_id=self.session_id, user_id=self.user_id)


    def build_hydra_integrated_network(self, projection=None):
        self.projection = projection
        self.initialise_hydra_connection()

        water_writer = PywrHydraWriter(self.pin.water,
                hydra = self.hydra,
                hostname = self.hostname,
                session_id = self.session_id,
                user_id = self.user_id,
                template_id = self.template_ids[0],
                project_id = self.project_id
               )

        self.water_writer = water_writer
        self.hydra_water_network = water_writer.build_hydra_network(projection="EPSG:4326", domain="water")

        energy_writer = PywrHydraWriter(self.pin.energy,
                hydra = self.hydra,
                hostname = self.hostname,
                session_id = self.session_id,
                user_id = self.user_id,
                template_id = self.template_ids[1],
                project_id = self.project_id
               )

        energy_writer._next_attr_id = water_writer._next_attr_id
        energy_writer._next_node_id = water_writer._next_node_id
        energy_writer._next_link_id = water_writer._next_link_id

        self.energy_writer = energy_writer
        self.hydra_energy_network = energy_writer.build_hydra_network(projection="EPSG:4326", domain="energy")

        self.hydra_nodes = self.water_writer.hydra_nodes + self.energy_writer.hydra_nodes
        self.hydra_links = self.water_writer.hydra_links + self.energy_writer.hydra_links
        self.network_attributes = self.water_writer.network_attributes + self.energy_writer.network_attributes
        network_hydratypes = [ { "id": self.water_writer.network_hydratype["id"], "child_template_id": self.template_ids[0] },
                               { "id": self.energy_writer.network_hydratype["id"], "child_template_id": self.template_ids[1] }
                             ]

        self.resource_scenarios = self.water_writer.resource_scenarios + self.energy_writer.resource_scenarios

        config_attribute, config_scenario = self.build_network_config_attribute()
        self.network_attributes += config_attribute
        self.resource_scenarios += config_scenario

        """ Create baseline scenario with resource_scenarios """
        baseline_scenario = self.make_baseline_scenario(self.resource_scenarios)

        config = self.pin.config.get_values()

        self.hydra_network = {
            "name": config["name"],
            "description": config.get("description",""),
            "project_id": self.project_id,
            "nodes": self.hydra_nodes,
            "links": self.hydra_links,
            "layout": None,
            "scenarios": [baseline_scenario],
            "projection": self.projection,
            "attributes": self.network_attributes,
            "types": network_hydratypes
        }
        #print(self.hydra_network)


    def build_network_config_attribute(self, attr_name="config"):
        """ Delegate hydra ops to energy writer for connection and attr_ids """

        attrs = [ make_hydra_attr(attr_name) ]
        self.energy_writer.hydra_attributes += self.energy_writer.hydra.add_attributes(attrs)

        config = self.pin.config.get_values()

        attr_data = {"config": config}

        dataset = { "name":  attr_name,
                    "type":  "DESCRIPTOR",
                    "value": json.dumps(attr_data),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        local_attr_id = self.energy_writer.get_next_attr_id()
        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.energy_writer.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return [resource_attribute], [resource_scenario]


    def make_baseline_scenario(self, resource_scenarios):
        return { "name": "Baseline",
                 "description": "hydra-pywr Baseline scenario",
                 "resourcescenarios": resource_scenarios if resource_scenarios else []
               }

    def add_network_to_hydra(self):
        """ Pass network to Hydra"""
        self.hydra.add_network(self.hydra_network)

"""
    Hydra => PywrNetwork
"""

class HydraToPywrNetwork():

    exclude_hydra_attrs = (
        "id", "status", "cr_date",
        "network_id", "x", "y",
        "types", "attributes", "layout",
        "network", "description"
    )

    def __init__(self, client, network, scenario_id, attributes, template, **kwargs):
        self.hydra = client
        self.data = network
        self.scenario_id = scenario_id
        self.attributes = attributes
        self.template = template

        self.type_id_map = {}
        for tt in self.template.templatetypes:
            self.type_id_map[tt.id] = tt

        self.attr_unit_map = {}
        self.hydra_node_by_id = {}

        self._parameter_recorder_flags = {}
        self._inline_parameter_recorder_flags = defaultdict(dict)
        self._node_recorder_flags = {}

        self.nodes = {}
        self.edges = []
        self.parameters = {}
        self.recorders = {}
        self.tables = {}
        self.scenarios = []


    @classmethod
    def from_scenario_id(cls, client, scenario_id, template_id=None, index=0):

        scenario = client.get_scenario(scenario_id, include_data=True, include_results=False, include_metadata=False, include_attr=False)
        network = client.get_network(scenario.network_id, include_data=True, include_results=False, template_id=None)
        network.scenarios = [scenario]
        network.rules = client.get_resource_rules('NETWORK', scenario.network_id)

        attributes = client.get_attributes()
        attributes = {attr.id: attr for attr in attributes}

        print(f"Retreiving template {network.types[index].template_id}")
        template = client.get_template(network.types[index].template_id)

        return cls(client, network, scenario_id, attributes, template)


    def write_rules_as_module(self):
        filename = "hydra_pywr_custom_module.py"

        prelude = (
            "from pywr import recorders",
            "from pywr import parameters",
            "import pandas",
            "import numpy as np",
            "import scipy",
            "from pywr.nodes import *",
            "from pywr.parameters.control_curves import *",
            "from pywr.parameters._thresholds import *",
            "from pywr.parameters._hydropower import *",
            "from pywr.domains.river import *"
        )

        forbidden = ("import", "eval", "exec")

        with open(filename, 'w') as fp:
            for p in prelude:
                fp.write(f"{p}\n")
            fp.write("\n")
            for rule in self.data.rules:
                for forbid in forbidden:
                    if forbid in rule["value"]:
                        raise PermissionError(f"Use of {forbid} statement forbidden in custom rules.")
                fp.write(rule["value"])
                fp.write("\n\n")


    def build_pywr_network(self, domain=None):
        self.build_pywr_nodes()
        self.edges = self.build_edges()
        self.parameters, self.recorders = self.build_parameters_recorders()
        breakpoint()
        if domain:
            self.timestepper, self.metadata, self.scenarios = self.build_integrated_network_attrs(domain)
        else:
            self.timestepper, self.metadata, self.tables, self.scenarios = self.build_network_attrs()

        if len(self.data.rules) > 0:
            self.write_rules_as_module()

        return self


    def build_pywr_nodes(self):

        for node in self.data["nodes"]:
            pywr_node = {"name": node["name"]}

            self.hydra_node_by_id[node["id"]] = node

            if comment := node.get("description"):
                pywr_node["comment"] = comment

            # Get the type for this node from the template
            pywr_node_type = None
            real_template_id = node["types"][0]["template_id"]

            for node_type in node["types"]:
                try:
                    #log.info(f"====\nnode: {node}")
                    if real_template_id != self.template["id"]:
                        continue
                    pywr_node_type = self.type_id_map[node_type["id"]]["name"]
                    break
                except KeyError:
                    # Skip as not in this template...
                    pywr_node_type = None
                    continue

            #log.info(f"Found node type {pywr_node_type} for node {node['name']} with nt_id {node_type['id']} on template {self.template['id']}\n====")

            #if pywr_node_type is None:
            #    raise ValueError('Template does not contain node of type "{}".'.format(pywr_node_type))

            # Skip if not in this template...
            if pywr_node_type:
                #log.info(f"Building node {node['name']} as {pywr_node_type}...")
                self.build_node_and_references(node, pywr_node_type)


    def build_edges(self):
        edges = []

        for hydra_edge in self.data["links"]:
            src_hydra_node = self.hydra_node_by_id[hydra_edge["node_1_id"]]
            dest_hydra_node = self.hydra_node_by_id[hydra_edge["node_2_id"]]
            # Retrieve nodes from PywrNode store to verify presence
            try:
                src_node = self.nodes[src_hydra_node["name"]]
                dest_node = self.nodes[dest_hydra_node["name"]]
            except KeyError:
                # Not in this template...
                continue

            edge = PywrEdge([src_node.name, dest_node.name])
            edges.append(edge)

        return edges


    def build_parameters_recorders(self):
        # attr_id = data.network.attributes[x].id
        parameters = {} # {name: P()}
        recorders = {} # {name: R()}

        for attr in self.data.attributes:
            ds = self.get_dataset_by_attr_id(attr.id)
            if not ds:
                continue
            if not ds["type"].startswith(("PYWR_PARAMETER", "PYWR_RECORDER")):
                continue
            if ds["type"].startswith("PYWR_PARAMETER"):
                value = json.loads(ds["value"])
                p = PywrParameter(ds["name"], value)
                parameters[p.name] = p
            elif ds["type"].startswith("PYWR_RECORDER"):
                value = json.loads(ds["value"])
                try:
                    r = PywrRecorder(ds["name"], value)
                except:
                    breakpoint()
                recorders[r.name] = r

        return parameters, recorders

    def build_network_attrs(self):
        """ TimeStepper, Metadata, and Tables instances """

        timestep = {}
        ts_keys = ("start", "end", "timestep")

        for attr in self.data["attributes"]:
            attr_group, *subs = attr.name.split('.')
            if attr_group != "timestepper":
                continue
            dataset = self.get_dataset_by_attr_id(attr.id)
            ts_key = subs[-1]
            try:
                value = json.loads(dataset["value"])
            except json.decoder.JSONDecodeError:
                value = dataset["value"]
            timestep[ts_key] = value


        ts_val = timestep.get("timestep",1)
        try:
            tv = int(float(ts_val))
        except ValueError:
            tv = ts_val
        timestep["timestep"] = tv
        ts_inst = PywrTimestepper(timestep)

        """ Metadata """
        metadata = {"title": self.data['name'],
                    "description": self.data['description']
                   }
        for attr in self.data["attributes"]:
            attr_group, *subs = attr.name.split('.')
            if attr_group != "metadata":
                continue
            dataset = self.get_dataset_by_attr_id(attr.id)
            meta_key = subs[-1]
            try:
                value = json.loads(dataset["value"])
            except json.decoder.JSONDecodeError:
                value = dataset["value"]
            metadata[meta_key] = value

        meta_inst = PywrMetadata(metadata)

        """ Tables """

        table_prefix = "tbl_"
        tables_data = defaultdict(dict)
        tables = {}
        for attr in self.data["attributes"]:
            if not attr.name.startswith(table_prefix):
                continue
            table_name, table_attr = attr.name[len(table_prefix):].split('.')
            dataset = self.get_dataset_by_attr_id(attr.id)
            try:
                value = json.loads(dataset["value"])
            except json.decoder.JSONDecodeError:
                value = dataset["value"]
            tables_data[table_name][table_attr] = value

        for tname, tdata in tables_data.items():
            tables[tname] = PywrTable(tname, tdata)

        """ Scenarios """

        try:
            scenarios_dataset = self.get_network_attr(self.scenario_id, self.data["id"], "scenarios")
            scenarios = [ PywrScenario(scenario) for scenario in scenarios_dataset["scenarios"] ]
        except ValueError as e:
            scenarios = []

        return ts_inst, meta_inst, tables, scenarios


    def build_integrated_network_attrs(self, domain):
        domain_data_key = f"{domain}_data"
        domain_attr = self.get_attr_by_name(domain_data_key)
        dataset = self.get_dataset_by_attr_id(domain_attr.id)
        data = json.loads(dataset["value"])

        timestep = data["timestepper"]
        ts_val = timestep.get("timestep",1)
        try:
            tv = int(float(ts_val))
        except ValueError:
            tv = ts_val

        timestep["timestep"] = tv
        ts_inst = PywrTimestepper(timestep)

        metadata = data["metadata"]
        meta_inst = PywrMetadata(metadata)

        scen_insts = [ PywrScenario(s) for s in data.get("scenarios") ]

        return ts_inst, meta_inst, scen_insts


    def get_network_attr(self, scenario_id, network_id, attr_key):
        net_attr = self.hydra.get_attribute_by_name_and_dimension(attr_key, None)
        ra = self.hydra.get_resource_attributes("network", network_id)
        ra_id = None
        for r in ra:
            if r["attr_id"] == net_attr["id"]:
                ra_id = r["id"]

        if not ra_id:
            raise ValueError(f"Resource attribute for {attr_key} not found in scenario {scenario_id} on network {network_id}")

        data = self.hydra.get_resource_scenario(ra_id, scenario_id, get_parent_data=False)
        attr_data = json.loads(data["dataset"]["value"])

        return attr_data # NB: String keys


    def get_dataset_by_attr_id(self, attr_id):
        # d = data.scenarios[0].resourcescenarios[x]
        # d.resource_attr_id == attr_id
        # d.dataset

        scenario = self.data.scenarios[0]
        for rs in scenario.resourcescenarios:
            if rs.resource_attr_id == attr_id:
                return rs.dataset

    def _get_resource_scenario(self, resource_attribute_id):

        for scenario in self.data["scenarios"]:
            for resource_scenario in scenario["resourcescenarios"]:
                if resource_scenario["resource_attr_id"] == resource_attribute_id:
                    return resource_scenario

        raise ValueError(f"No resource scenario found for resource attribute id: {resource_attribute_id}")


    def build_node_and_references(self, nodedata, pywr_node_type):

        for resource_attribute in nodedata["attributes"]:
            attribute = self.attributes[resource_attribute["attr_id"]]
            try:
                resource_scenario = self._get_resource_scenario(resource_attribute["id"])
            except ValueError:
                continue  # No data associated with this attribute.

            # Allow export of probable recorders
            if resource_attribute["attr_is_var"] == 'Y' and recorder not in attribute["name"].lower():
                continue

            attribute_name = attribute["name"]
            dataset = resource_scenario["dataset"]
            dataset_type = dataset["type"]
            value = dataset["value"]

            try:
                typedval = json.loads(value)
            except json.decoder.JSONDecodeError as e:
                typedval = value
            nodedata[attribute_name] = typedval

        nodedata["type"] = pywr_node_type
        node_attr_data = {a:v for a,v in nodedata.items() if a not in self.exclude_hydra_attrs}
        position = {"geographic": [ nodedata.get("x",0), nodedata.get("y",0) ]}
        node_attr_data["position"] = position

        if comment := nodedata.get("description"):
            node_attr_data["comment"] = comment

        if node_attr_data["name"] == "BR_Existing_turbine":
            breakpoint()
        node = PywrNode(node_attr_data)

        self.nodes[node.name] = node
        #self.parameters.update(node.parameters)
        #self.recorders.update(node.recorders)

"""
    Integrated model output.h5 => Updated Hydra network
"""

class IntegratedOutputWriter():
    domain_attr_map = {"water": "simulated_flow", "energy": "flow"}

    def __init__(self, scenario_id, template_id, output_file, metric_file, domain, hydra=None, hostname=None, session_id=None, user_id=None):
        import tables
        self.scenario_id = scenario_id
        self.template_id = template_id
        self.data = tables.open_file(output_file)
        self.metrics = pd.HDFStore(metric_file)
        self.domain = domain

        self.hydra = hydra
        self.hostname = hostname
        self.session_id = session_id
        self.user_id = user_id

        self.initialise_hydra_connection()


    def initialise_hydra_connection(self):
        if not self.hydra:
            from hydra_client.connection import JSONConnection
            self.hydra = JSONConnection(self.hostname, session_id=self.session_id, user_id=self.user_id)

        self.scenario = self.hydra.get_scenario(self.scenario_id, include_data=True, include_results=False, include_metadata=False, include_attr=False)
        self.network = self.hydra.get_network(self.scenario.network_id, include_data=False, include_results=False, template_id=self.template_id)

    def _copy_scenario(self):
        json_scenario = self.scenario.as_json()
        scenario = json.loads(json_scenario)
        scenario["resourcescenarios"] = []
        return scenario

    def build_hydra_output(self):
        output_scenario = self._copy_scenario()
        output_attr = self.domain_attr_map[self.domain]

        self.times = build_times(self.data)
        node_datasets = self.process_node_results(output_attr)
        parameter_datasets = self.process_parameter_results()
        node_metrics = self.process_metrics()

        node_metric_scenarios = self.add_node_metrics(node_metrics)
        node_scenarios = self.add_node_attributes(node_datasets, output_attr=output_attr)

        output_scenario["resourcescenarios"] = node_scenarios + node_metric_scenarios

        self.hydra.update_scenario(output_scenario)

    def process_metrics(self):
        exclude = "hydropowerrecorder"
        strtok = ':'

        # e.g. ['/__Irrigation south__:Curtailment'
        groups = [group for group in self.metrics if not exclude in group.lower()]
        node_attrs = {}
        for group in groups:
            group_data = build_metric(group, self.metrics[group])
            if group.startswith('/'):
                group = group[1:]

            name, attr = group.split(strtok)
            name = name.strip('_')
            node_attrs[(name, attr)] = group_data

        return node_attrs


    def process_node_results(self, node_attr):
        node_datasets = {}
        for node in self.data.get_node("/nodes"):
            ds = build_node_dataset(node, self.times, node_attr)
            node_datasets[node.name] = ds

        return node_datasets


    def process_parameter_results(self):
        param_datasets = []
        for param in self.data.get_node("/parameters"):
            ds = build_parameter_dataset(param, self.times)
            param_datasets.append(ds)

        return param_datasets

    def add_node_metrics(self, node_metrics):

        resource_scenarios = []
        volumetric_flow_rate_dim = "Volumetric flow rate"

        for (node_name, attr), data in node_metrics.items():
            hydra_node = self.get_node_by_name(node_name)
            if not hydra_node:
                print(f"Skipping attr {attr} on {node_name}")
                continue

            result_dim = self.hydra.get_dimension_by_name(volumetric_flow_rate_dim)

            if attr.lower().endswith("_values"):
                result_attr = self.hydra.get_attribute_by_name_and_dimension("Curtailment_value", result_dim["id"])
                data_type = "SCALAR"
                value = json.dumps(data[0])
            else:
                result_attr = self.hydra.get_attribute_by_name_and_dimension("simulated_Curtailment", result_dim["id"])
                data_type = "DATAFRAME"
                data.index = data.index.to_timestamp()
                data.index = data.index.map(str)
                value = data.to_json()

            result_unit = self.hydra.get_unit_by_abbreviation("Mm³/day")

            result_unit_id = result_unit["id"] if result_unit is not None else "-"
            sf_res_attr = self.hydra.add_resource_attribute("NODE", hydra_node["id"], result_attr["id"], is_var='Y', error_on_duplicate=False)

            dataset = { "name":  result_attr["name"],
                        "type":  data_type,
                        "value": value,
                        "metadata": "{}",
                        "unit_id": result_unit_id,
                        "hidden": 'N'
                      }

            resource_scenario = { "resource_attr_id": sf_res_attr["id"],
                                  "dataset": dataset
                                }

            resource_scenarios.append(resource_scenario)

        return resource_scenarios


    def add_node_attributes(self, node_datasets, output_attr="simulated_flow"):
        """
        sf_attr = make_hydra_attr(output_attr)
        hydra_attrs = self.hydra.add_attributes([sf_attr])
        sf_hydra_attr = hydra_attrs[0]
        """

        resource_scenarios = []

        for node_name, node_ds in node_datasets.items():
            print(f"{self.domain} => {node_name}")
            hydra_node = self.get_node_by_name(node_name)
            if not hydra_node:
                print(f"Skipping {node_name}")
                continue

            volume_dim = "Volume"
            volumetric_flow_rate_dim = "Volumetric flow rate"
            power_dim = "Power"
            storage_types = ("reservoir", "storage")
            energy_types = ("generator", "bus", "line", "load", "battery")

            hydra_node_type = hydra_node["types"][0]["name"]

            if hydra_node_type.lower() in storage_types:
                result_dim = self.hydra.get_dimension_by_name(volume_dim)
                result_attr = self.hydra.get_attribute_by_name_and_dimension("simulated_volume", result_dim["id"])
                result_unit = self.hydra.get_unit_by_abbreviation("Mm³")
            elif hydra_node_type.lower() in energy_types:
                result_dim = self.hydra.get_dimension_by_name(power_dim)
                result_attr = self.hydra.get_attribute_by_name_and_dimension("flow", result_dim["id"])
                result_unit = self.hydra.get_unit_by_abbreviation("MW")
            else:
                result_dim = self.hydra.get_dimension_by_name(volumetric_flow_rate_dim)
                result_attr = self.hydra.get_attribute_by_name_and_dimension("simulated_flow", result_dim["id"])
                result_unit = self.hydra.get_unit_by_abbreviation("Mm³/day")

            result_unit_id = result_unit["id"] if result_unit is not None else "-"

            sf_res_attr = self.hydra.add_resource_attribute("NODE", hydra_node["id"], result_attr["id"], is_var='Y', error_on_duplicate=False)

            dataset = { "name":  result_attr["name"],
                        "type":  "DATAFRAME",
                        "value": json.dumps(node_ds),
                        "metadata": "{}",
                        "unit_id": result_unit_id,
                        "hidden": 'N'
                      }

            resource_scenario = { "resource_attr_id": sf_res_attr["id"],
                                  "dataset": dataset
                                }

            resource_scenarios.append(resource_scenario)

        return resource_scenarios


    def get_node_by_name(self, name):
        for node in self.network["nodes"]:
            if node["name"] == name:
                return node

class NetworkTool():
    def __init__(self):
        """
        self.client = client
        self.scenarios = scenarios
        self.project = project

        self.merge_networks(client, scenarios, project)
        """

    def get_hydra_network_types(self):
        types = []
        for template in self.templates:
            for t in template["templatetypes"]:
                if t["resource_type"] == "NETWORK":
                    types.append(t)

        return types


    def initialise_hydra_connection(self):
        from hydra_client.connection import JSONConnection
        self.hydra = JSONConnection(self.hostname, session_id=self.session_id, user_id=self.user_id)


    def export_multi(self, client, scenario_id, network_id):
        self.client = client
        config = self.get_network_attr(scenario_id, network_id, attr_key="config")
        profile = self.get_network_attr(scenario_id, network_id, attr_key="network_profile")

        for template_id, networks in ((int(tid), net) for tid, net in profile.items()):
            for net_desc in networks:
                scenario_id = net_desc["scenario_id"]
                net_name = net_desc["network"]

                #exporter = PywrHydraExporter.from_scenario_id(client, scenario_id)
                exporter = HydraToPywrNetwork.from_scenario_id(client, scenario_id)
                network_data = exporter.build_pywr_network()
                pywr_network = NewPywrNetwork(network_data)
                #writer = PywrJsonWriter(pnet)
                output = pywr_network.as_dict()
                outfile = f"{net_name}.json"
                with open(outfile, mode='w') as fp:
                    json.dump(output, fp, indent=2)

        with open("multiconfig.json", 'w') as fp:
            json.dump(config, fp, indent=2)


    def get_network_attr(self, scenario_id, network_id, attr_key):
        net_attr = self.client.get_attribute_by_name_and_dimension(attr_key, None)
        ra = self.client.get_resource_attributes("network", network_id)
        ra_id = None
        for r in ra:
            if r["attr_id"] == net_attr["id"]:
                ra_id = r["id"]

        data = self.client.get_resource_scenario(ra_id, scenario_id, get_parent_data=False)
        attr_data = json.loads(data["dataset"]["value"])

        return attr_data # NB: String keys


    def merge_multi(self, client, template_map, project_id, **kwargs):

        self.client = client

        next_attr_id = -1
        next_node_id = -1

        hydra_nodes = []
        hydra_links = []
        network_attributes = []
        network_hydratypes = set()
        resource_scenarios = []

        for template_id in template_map:
            for scenario in template_map[template_id]:
                network = scenario["network"]
                writer = NewPywrHydraWriter(
                            network,
                            hydra = client,
                            template_id = template_id,
                            project_id = project_id
                         )
                writer._next_attr_id = next_attr_id
                writer._next_node_id = next_node_id
                hydra_net = writer.build_hydra_network(projection="EPSG:4326", domain=network.title.replace(' ','_'))
                next_attr_id = writer._next_attr_id
                next_node_id = writer._next_node_id

                network_hydratypes.add((writer.network_hydratype["id"], template_id))
                hydra_nodes += writer.hydra_nodes
                hydra_links += writer.hydra_links
                network_attributes += writer.network_attributes
                resource_scenarios += writer.resource_scenarios

        next_attr_id -= 1

        config_attribute, config_scenario = self.build_network_config_attribute(next_attr_id)
        network_attributes += config_attribute
        resource_scenarios += config_scenario

        next_attr_id -= 1

        profile_attribute, profile_scenario = self.build_network_profile_attribute(next_attr_id, template_map)
        network_attributes += profile_attribute
        resource_scenarios += profile_scenario

        hydra_network_types = [{"id": nid, "child_template_id": tid} for (nid, tid) in network_hydratypes]

        baseline_scenario = {
            "name": "Baseline",
            "description": "Baseline scenario",
            "resourcescenarios": resource_scenarios
        }

        hydra_network = {
            "name": "Unified multi 03",
            "description": "Unified multi desc",
            "project_id": project_id,
            "nodes": hydra_nodes,
            "links": hydra_links,
            "layout": None,
            "scenarios": [baseline_scenario],
            "projection": "EPSG:4326",
            "attributes": network_attributes,
            "types": hydra_network_types
        }

        client.add_network(hydra_network)


    def build_network_profile_attribute(self, next_attr_id, template_map, attr_name="network_profile"):
        attr = [ {"name": attr_name, "description": "Network profile"} ]
        resp_attr = self.client.add_attributes(attr)

        for tid, scenarios in template_map.items():
            for scenario in scenarios:
                scenario["network"] = scenario["network"].title.replace(' ', '_')


        #attr_data = {"scenarios": scenario_map}
        attr_data = template_map

        dataset = { "name":  attr_name,
                    "type":  "DESCRIPTOR",
                    "value": json.dumps(attr_data),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        local_attr_id = next_attr_id
        resource_attribute = { "id": local_attr_id,
                               "attr_id": resp_attr[0]["id"],
                               "attr_is_var": "N"
                             }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return [resource_attribute], [resource_scenario]


    def build_network_config_attribute(self, next_attr_id, attr_name="config"):

        attrs = [ {"name": attr_name, "description": "Pynsim config"} ]
        resp_attr = self.client.add_attributes(attrs)

        attr_data = config_template

        dataset = { "name":  attr_name,
                    "type":  "DESCRIPTOR",
                    "value": json.dumps(attr_data),
                    "metadata": "{}",
                    "unit": "-",
                    "hidden": 'N'
                  }

        local_attr_id = next_attr_id
        resource_attribute = { "id": local_attr_id,
                               "attr_id": resp_attr[0]["id"],
                               "attr_is_var": "N"
                             }

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return [resource_attribute], [resource_scenario]


    def merge_networks(self, client, scenarios, project_id, **kwargs):
        template_map = {}
        networks = []
        unified = {}
        attributes = client.get_attributes()
        attributes = {attr.id: attr for attr in attributes}

        for scenario_id in scenarios:
            scenario = client.get_scenario(scenario_id, include_data=True, include_results=True, include_metadata=False, include_attr=False)
            network = client.get_network(scenario.network_id, include_data=True, include_results=True)
            template_id = network.types[0].template_id
            if template_id not in template_map:
                print(f"Retreiving template {template_id} for network {scenario.network_id}")
                template = client.get_template(template_id)
                template_map[template_id] = template
            network.scenarios = [scenario]
            network.rules = client.get_resource_rules('NETWORK', scenario.network_id)

            networks.append(network)

        hydra_nodes = []
        hydra_links = []
        resource_scenarios = []
        hydra_network_attrs = []
        hydra_network_types = set()


        for network in networks:
            template_id = network.types[0].template_id
            network_type = self.get_hydra_network_type(template_map[template_id])["id"]
            #hydra_network_types.append({"id": network_type, "child_template_id": template_id})
            hydra_network_types.add((network_type, template_id))

            hydra_nodes += network.nodes
            hydra_links += network.links
            non_net_scenarios = [ s for s in network.scenarios[0]["resourcescenarios"] if s["resourceattr"]["ref_key"] != "NETWORK" ]
            for s in non_net_scenarios:
                s["resourceattr"]["attr_id"] = -s["resourceattr"]["attr_id"]
            resource_scenarios += non_net_scenarios

        baseline_scenario = self.make_baseline_scenario(resource_scenarios)

        hydra_network_types = [{"id": nid, "child_template_id": tid} for (nid, tid) in hydra_network_types]

        unified = {
            "name": "Unified network",
            "description": "Unified network desc",
            "project_id": project_id,
            "nodes": hydra_nodes,
            "links": hydra_links,
            "layout": None,
            "scenarios": [baseline_scenario],
            "projection": "EPSG:4326",
            "attributes": hydra_network_attrs,
            "types": hydra_network_types
        }

        client.add_network(unified)

        """
        >> rs[0]["resourcescenarios"][0]
        {'dataset_id': 3404315, 'scenario_id': 72877, 'source': None, 'resource_attr_id': 9273054, 'cr_date': '2022-03-28 09:40:21', 'dataset':
        {'type': 'DESCRIPTOR', 'id': 3404315, 'unit_id': None, 'cr_date': '2022-03-24 16:48:53', 'value': '1972-01-01', 'updated_by': None,
        'updated_at': '2022-03-24 16:48:38', 'name': 'timestepper.start', 'hash': -8650504358077606075, 'hidden': 'N', 'created_by': 12,
        'metadata': {}}, 'resourceattr': {'attr_id': 80, 'network_id': 36712, 'node_id': None, 'group_id': None, 'attr_is_var': 'N', 'id':
        9273054, 'ref_key': 'NETWORK', 'project_id': None, 'link_id': None, 'cr_date': '2022-03-28 09:40:21'}}
        >> rs[0]["resourcescenarios"][0].keys()
        dict_keys(['dataset_id', 'scenario_id', 'source', 'resource_attr_id', 'cr_date', 'dataset', 'resourceattr'])
        """

    def make_resource_scenario(self, element, attr_name, local_attr_id, datatype=None):
        dataset = element.attr_dataset(attr_name)

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return resource_scenario


    def make_baseline_scenario(self, resource_scenarios):
        return { "name": "Baseline",
                 "description": "hydra-pywr Baseline scenario",
                 "resourcescenarios": resource_scenarios if resource_scenarios else []
               }

    def get_hydra_network_type(self, template):
        for t in template["templatetypes"]:
            if t["resource_type"] == "NETWORK":
                return t
"""
    Utilities
"""
def unwrap_list(node_data):
    return [ i[0] for i in node_data ]

def build_times(data, node="/time"):
    raw_times = data.get_node(node).read().tolist()
    """ Profile times to determine period.
        A more rigorous solution is to include a token
        indicating the period (e.g. H, D, W, or M)
        in the hdf output.
    """
    if len(raw_times) > 1 and (raw_times[0][0] == raw_times[1][0] or raw_times[-2][0] == raw_times[-1][0]):
        # Probably hours...
        times = [ f"{t[0]:02}-{t[2]:02}-{t[3]} {t[1] % 24:02}:00:00" for t in raw_times ]
    else:
        # ...assume single day values
        times = [ f"{t[0]:02}-{t[2]:02}-{t[3]}" for t in raw_times ]

    return times

def build_node_dataset(node, times, node_attr="value"):
    raw_node_data = node.read().tolist()
    node_data = unwrap_list(raw_node_data)

    series = {}
    dataset = { "value": series}

    for t,v in zip(times, node_data):
        series[t] = v

    return dataset

def build_metric(node, data):
    return data


def build_parameter_dataset(param, times, stok='_'):
    node, _, attr = param.name.partition(stok)
    raw_param_data = param.read().tolist()
    param_data = unwrap_list(raw_param_data)

    series = {}
    dataset = { node: { attr: series} }

    for t,v in zip(times, param_data):
        series[t] = v

    return dataset


config_template = {
    "name": "Water-energy system",
    "timesteps": {
        "start": "1970-01-01",
        "end": "1970-12-31",
        "freq": "W"
    },
    "engines": [{
            "name": "water",
            "engine": "PywrEngine",
            "args": ["Water_system.json"],
            "kwargs": {
                "output_directory": "output"
            },
            "end_points": [{
                    "type": "pywr_array_recorder",
                    "name": "HP_out",
                    "recorder": "__node__:attr"
                }
            ]
        },
        {
            "name": "energy",
            "engine": "PywrEngine",
            "args": ["Energy_system.json"],
            "kwargs": {
                "solver": "glpk-dcopf",
                "output_directory": "output"
            },
            "end_points": [{
                    "type": "pywr_parameter",
                    "name": "HP_in",
                    "parameter": "__node__:attr"
                }
            ]
        }
    ],
    "connections": [{
            "start": "HP_out",
            "end": "HP_in"
        }
    ]
}

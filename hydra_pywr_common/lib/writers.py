import json
import pandas as pd


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

        self.template_attributes = self.collect_template_attributes()
        self.hydra_attributes = self.register_hydra_attributes()

        """ Build network elements and resource_scenarios with datasets """
        self.hydra_nodes, node_scenarios = self.build_hydra_nodes()

        if domain:
            self.network_attributes, network_scenarios = self.build_network_descriptor_attributes(domain)
        else:
            self.network_attributes, network_scenarios = self.build_network_attributes()

        self.hydra_links, link_scenarios = self.build_hydra_links()

        self.resource_scenarios = node_scenarios + network_scenarios + link_scenarios

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

        for meta_attr in self.network.metadata.intrinsic_attrs:
            pending_attrs.add(f"metadata.{meta_attr}")

        for table_name, table in self.network.tables.items():
            for attr_name in table.intrinsic_attrs:
                pending_attrs.add(f"tbl_{table_name}.{attr_name}")

        attrs = [ make_hydra_attr(attr_name) for attr_name in pending_attrs - excluded_attrs.union(set(self.template_attributes.keys())) ]

        return self.hydra.add_attributes(attrs)


    def make_resource_attr_and_scenario(self, element, attr_name, datatype=None):
        local_attr_id = self.get_next_attr_id()
        resource_scenario = self.make_resource_scenario(element, attr_name, local_attr_id, datatype)
        resource_attribute = { "id": local_attr_id,
                               "attr_id": self.get_hydra_attrid_by_name(attr_name),
                               "attr_is_var": "N"
                             }


        return resource_attribute, resource_scenario


    def make_resource_scenario(self, element, attr_name, local_attr_id, datatype=None):
        dataset = element.attr_dataset(attr_name)

        resource_scenario = { "resource_attr_id": local_attr_id,
                              "dataset": dataset
                            }

        return resource_scenario


    def build_hydra_nodes(self):
        hydra_nodes = []
        resource_scenarios = []

        exclude_node_attrs = ('type', "Turbine", "cc_level", "Evap_max_flow")

        for node in self.network.nodes.values():
            resource_attributes = []

            # TODO Move this to node ctor path???
            for attr_name in filter(lambda a: a not in exclude_node_attrs, node.intrinsic_attrs):
                ra, rs = self.make_resource_attr_and_scenario(node, attr_name)
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


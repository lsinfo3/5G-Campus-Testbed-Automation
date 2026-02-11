from nicegui import ui, app
from typing import Literal, Union, get_args, get_origin
from abc import ABC, abstractmethod
import os
import types
import copy
import queue
import hashlib
import dataclasses
from collections import namedtuple
from natsort import natsorted
# import functools


import json
import yaml
yaml.Dumper.ignore_aliases = lambda *args : True # Don't reference identical types

# @app.exception_handler(Exception)
# @app.on_exception
# async def global_handler(exc):
#     ui.notify(
#         f'Error: {exc}',
#         type='negative',
#         close_button=True,
#     )

# def notify_exceptions(func):
#     @functools.wraps(func)
#     async def wrapper(*args, **kwargs):
#         try:
#             result = func(*args, **kwargs)
#             if callable(getattr(result, "__await__", None)):
#                 await result
#         except Exception as e:
#             ui.notify(
#                 str(e),
#                 type='negative',
#                 position='bottom',
#                 close_button=True,
#             )
#     return wrapper



@dataclasses.dataclass(frozen=True)
class Baseclass(ABC):
    """
    Collection of helper functions which are attached to all dataclasses for the measurement series definion.
    """

    @classmethod
    def validate_dict_types(cls, dikt):
        """
        Limited type-checking validation if the dataclass is constructed from a dict.
        Don't overcomplicate things, complex expressions, like x:tuple[int|None] are not supported.
        """
        for k,v in cls.__annotations__.items():
            if k not in dikt.keys():
                raise ValueError(f"Missing parameter: '{k}'")

            def handle_nested_dicts(key,specified_type, nested_dikt):
                try:
                    specified_type.validate_dict_types(nested_dikt)
                except BaseException as e:
                    raise ValueError(f"Can't parse dict for parameter {key}, type: {specified_type} because of '{e}'") from e

            # first handle nested dicts
            if isinstance(dikt[k], dict) and dict != v:
                handle_nested_dicts(k,v, dikt[k])
            elif get_origin(v) is Literal:
                if dikt[k] not in get_args(v):
                    raise ValueError(f"Expected Literal[{get_args(v)}] for parameter '{k}' but got type {type(dikt[k])}")
            elif get_origin(v) in [set, tuple, list]:
                if not isinstance(dikt[k], get_origin(v)):
                    raise ValueError(f"Expected {get_origin(v)}[{get_args(v)}] for parameter '{k}' but got type {type(dikt[k])}")
                if all([ isinstance(x, dict) for x in dikt[k] ]):
                    for d in dikt[k]:
                        handle_nested_dicts(k,get_args(v)[0], d)
                elif any([ not isinstance(x, get_args(v)[0]) for x in dikt[k] ]):
                    raise ValueError(f"Expected {get_origin(v)}[{get_args(v)}] for parameter '{k}' but got type {type(dikt[k])}")
            elif (type_origin := get_origin(v)) is Union or type_origin is types.UnionType:  # x: typing.Union[int,float] != x: int|float
                if not any([ isinstance(dikt[k], e) for e in get_args(v) ]):
                    raise ValueError(f"Expected Union[{get_args(v)}] for parameter '{k}' but got '{type(dikt[k])}'")
            else:
                if not isinstance(dikt[k], v):
                    raise ValueError(f"Expected type '{v}' for parameter '{k}' but got '{type(dikt[k])}'")

        if len( difference := set(dikt.keys()).symmetric_difference(set(cls.__annotations__.keys())) ) != 0:
            raise ValueError(f"Recieved {len(difference)} additional parameters which can't be handled: {difference}")
    @abstractmethod
    def validate(self):
        """
        Check whether the recieved values contain obvious erros. Raise exception!
        """
        pass

    @classmethod
    def from_json(cls, path):
        with open(path, "r") as f:
            content = json.load(f)
        return cls.from_dict(content)

    @classmethod
    def from_yaml(cls, path):
        with open(path, "r") as f:
            content = yaml.safe_load(f)
        return cls.from_dict(content)

    @classmethod
    def from_dict(cls, dikt:dict):
        cls.validate_dict_types(dikt)
        return dataclass_from_dict(cls, dikt)

    def __repr__(self) -> str:
        return str(dataclasses.asdict(self))


def exception_notification(e: Exception):
    e_msg = str(e)
    e_msg = e_msg.replace('because of', 'because of:<br><b>') + '</br>'
    ui.notify(e_msg, type='negative', close_button=True, position='bottom', timeout=0, html=False)


def dataclass_from_dict(klass, dikt):
    """
    Generic function to create a specified dataclass from a dict. Not good at handling 'None'...
    """
    try:
        fieldtypes = klass.__annotations__
        return klass(**{f: dataclass_from_dict(fieldtypes[f], dikt[f]) for f in dikt})
    except AttributeError:
        if isinstance(dikt, (tuple, list)):
            return [dataclass_from_dict(klass.__args__[0], f) for f in dikt]
        return dikt

def expand_dict(dd):
    """
    expands dictionaries with lists to a list of multiple dictionaries.
    { 'a':[1,2], 'b':"b", 'c':{'C':[3,4]} }
    => [
        {'a':1, 'b':"b", 'c':{'C':3}},
        {'a':2, 'b':"b", 'c':{'C':3}},
        {'a':1, 'b':"b", 'c':{'C':4}},
        {'a':2, 'b':"b", 'c':{'C':4}}
        ]
    """
    q = queue.Queue()
    q.put(dd)
    ret = []
    while not q.empty():
        d = q.get()
        for k,v in d.items():
            # print(f"{k} :: {v}")
            if isinstance(v, dict) and len(v.keys())>=1:
                expanded = expand_dict(v)
                # print(f"{expanded}")
                if len(expanded) > 1:
                    [q.put({**d,k:x}) for x in expanded]
                    break
                if len(expanded) == 1:
                    d[k] = expanded[0]
            if isinstance(v, list):
                if len(v) >= 1:
                    [q.put({**d,k:x}) for x in d[k]]
                    break
        else:
            ret.append(d)
    return ret




TDD_Pattern = namedtuple("TDD_Pattern", ["tdd_period", "tdd_ratio"])
TDD_SlotsAndSymbols = namedtuple("TDD_SlotsAndSymbols", ["dl_slots","dl_symbols","ul_slots","ul_symbols"])
Number_of_runs = {"data":"0"}

@dataclasses.dataclass(frozen=True)
class MeasurementTDDConfig(Baseclass):
    tdd_ratio: int
    tdd_period: int
    # tdd_flex_slots: int
    tdd_dl_slots: int
    tdd_dl_symbols: int
    tdd_ul_slots: int
    tdd_ul_symbols: int

    @staticmethod
    def from_ratio_and_period(tdd_period, tdd_ratio):
        if tdd_period not in (5,10,20):
            raise ValueError(f"Unsupported tdd_period: {tdd_period}")
        if tdd_ratio not in (1,2,4):
            raise ValueError(f"Unsupported tdd_ratio: {tdd_ratio}")

        patterns = MeasurementTDDConfig.defined_tdd_patterns()
        dl_slots,dl_symbols,ul_slots,ul_symbols = patterns[TDD_Pattern(tdd_period=tdd_period, tdd_ratio=tdd_ratio)]
        return MeasurementTDDConfig(tdd_period=tdd_period, tdd_ratio=tdd_ratio, tdd_dl_slots=dl_slots,
                                    tdd_ul_slots=ul_slots, tdd_dl_symbols=dl_symbols, tdd_ul_symbols=ul_symbols)
    @staticmethod
    def defined_tdd_patterns():
        rules = {
                TDD_Pattern(5,1) : TDD_SlotsAndSymbols(2,7,2,6),
                TDD_Pattern(5,2) : TDD_SlotsAndSymbols(3,5,1,8),
                TDD_Pattern(5,4) : TDD_SlotsAndSymbols(3,13,1,0),
                TDD_Pattern(10,1) : TDD_SlotsAndSymbols(4,13,5,0),
                TDD_Pattern(10,2) : TDD_SlotsAndSymbols(6,8,3,4),
                TDD_Pattern(10,4) : TDD_SlotsAndSymbols(7,13,2,0),
                TDD_Pattern(20,1) : TDD_SlotsAndSymbols(9,13,10,0),
                TDD_Pattern(20,2) : TDD_SlotsAndSymbols(13,5,6,8),
                TDD_Pattern(20,4) : TDD_SlotsAndSymbols(15,13,4,0),
                }
        return rules

    def validate(self):
        if self.tdd_period not in [5,10,20]:
            raise ValueError("Invalid tdd_period")
        if self.tdd_ratio not in [1,2,4]:
            raise ValueError("Invalid tdd_ratio")
        # TODO: validate patterns vs predefined list?


@dataclasses.dataclass(frozen=True)
class MeasurementTrafficConfig(Baseclass):
    # TODO: type definitions
    traffic_type: Literal["iperfthroughput", "scapyudpping", "idle"]

    # all
    traffic_duration: int
    count: str
    direction: Literal["Ul" , "Dl", "UlDl"]
    target_ip: str
    target_port: str

    # iperf
    proto: Literal["udp" , "tcp"]
    rate: str

    # ping
    dist: Literal["det"]
    iat: str
    size: Literal["small", "big", "none"]
    burst: str

    @staticmethod
    def from_sparse_definition(**d):
        if d["traffic_type"] == "iperfthroughput":
            return MeasurementTrafficConfig.minimum_def_iperfthroughpt(d)
        elif d["traffic_type"] == "scapyudpping":
            return MeasurementTrafficConfig.minimum_def_scapyudpping(d)
        elif d["traffic_type"] == "idle":
            return MeasurementTrafficConfig.minimum_def_idle(d)
        else:
            raise ValueError(f"Unknown traffic_type: {d["traffic_type"]}")

    @staticmethod
    def minimum_def_idle(d):
        assert( "traffic_duration" in d.keys() )
        assert( "traffic_type" in d.keys() )
        ret = MeasurementTrafficConfig(
            traffic_type=d["traffic_type"],
            traffic_duration=d["traffic_duration"],
            direction="UlDl",
            target_ip="",
            target_port="0",
            proto="tcp",
            rate="0",

            count="0",
            dist="det",
            iat="0",
            size="none",
            burst="1"
                )
        ret.validate()
        return ret
    @staticmethod
    def minimum_def_iperfthroughpt(d):
        req = ["traffic_type", "traffic_duration", "direction", "target_ip", "target_port", "proto", "rate"]
        for r in req:
            assert(r in d.keys())
        ret = MeasurementTrafficConfig(
            traffic_type=d["traffic_type"],
            traffic_duration=d["traffic_duration"],
            direction=d["direction"],
            target_ip=d["target_ip"],
            target_port=d["target_port"],
            proto=d["proto"],
            rate=d["rate"],

            count="0",
            dist="det",
            iat="0",
            size="none",
            burst="1"
                )
        ret.validate()
        return ret
    @staticmethod
    def minimum_def_scapyudpping(d):
        req = ["traffic_type", "traffic_duration", "target_ip", "target_port", "dist", "size", "burst", "iat"]
        for r in req:
            assert(r in d.keys())
        ret = MeasurementTrafficConfig(
            traffic_type=d["traffic_type"],
            traffic_duration=d["traffic_duration"],
            target_ip=d["target_ip"],
            target_port=d["target_port"],
            dist=d["dist"],
            size=d["size"],
            burst=d["burst"],
            iat=d["iat"],

            count=str( int(float(d["traffic_duration"]) / float(d["iat"])) ),

            rate="0",
            proto="udp",
            direction="UlDl",
                )
        ret.validate()
        return ret



    def validate(self):
        if self.traffic_duration <= 0:
            raise ValueError(f"Invalid traffic duration")
        try:
            _ = int(self.count)
        except BaseException as e:
            raise ValueError("Can't parse pkt count") from e
        try:
            _ = int(self.target_port)
        except BaseException as e:
            raise ValueError("Can't parse target_port") from e

        if self.traffic_type == "iperfthroughput":
            if self.direction not in ["Ul" , "Dl"]:
                raise ValueError(f"Invalid direction for 'iperfthroughput'")
            rate = self.rate
            if self.rate[-1] in ["K", "M", "G"]:
                rate = self.rate[:-1]
            try:
                _ = float(rate)
            except BaseException as e:
                raise ValueError("Can't parse rate") from e
        elif self.traffic_type == "scapyudpping":
            if self.direction not in ["UlDl"]:
                raise ValueError(f"Invalid direction for 'scapyudpping'")
            try:
                _ = float(self.iat)
            except BaseException as e:
                raise ValueError("Can't parse iat") from e
            try:
                _ = int(self.burst)
            except BaseException as e:
                raise ValueError("Can't parse burst") from e
        elif self.traffic_type == "idle":
            pass
        else:
            raise ValueError(f"Unknown traffic_type: {self.traffic_type}")



@dataclasses.dataclass(frozen=True)
class MeasurementRunGNBDefinition(Baseclass):
    type: Literal["OAI", "srsRAN"]
    uhd_version: Literal["UHD-4.0", "UHD-3.15.LTS"]
    version: Literal["v2.3.0", "v2.2.0", "v2.1.0", "release_24_10", "release_24_04"]
    commit: str

    @staticmethod
    def from_versions_numbers(uhd_version: str, gnb_version: str):
        commit_map = {
                "v2.3.0": "8bf6d5d7da8c0a8384e4022fd4872e6d3d550921",
                "v2.2.0": "68191088ab4cdcd47d6c0764ac5cf2483f4b3d29",
                "v2.1.0": "9fab2124417cfe67fe09b1eab5e377e26c5cf3a5",
                "release_24_10": "9d5dd742a70e82c0813c34f57982f9507f1b6d5d",
                "release_24_04": "c33cacba7d940e734ac7bad08935cbc35578fad9",
                }

        gnb_type, version = gnb_version.split(" ")
        commit = commit_map[version]
        gnbdef = MeasurementRunGNBDefinition(type=gnb_type, uhd_version=uhd_version, version=version, commit=commit)
        gnbdef.validate()
        return gnbdef


    def validate(self):
        if type == "OAI":
            if self.version not in ["v2.3.0", "v2.2.0", "v2.1.0"]:
                raise ValueError("Unknown version for OAI")
        if type == "srsRAN":
            if self.version not in ["release_24_10", "release_24_04"]:
                raise ValueError("Unknown version for srsRAN")

        if len(self.commit) != 40:
            raise ValueError("Invalid commit hash")




@dataclasses.dataclass(frozen=True)
class MeasurementRunDefinition(Baseclass):
    identifier: str
    run: int
    gnb_bandwidth: str
    dl_mcs: int|str
    ul_mcs: int|str
    dockerization: bool
    rx_gain: float|None
    tx_gain: float|None

    gnb_version: MeasurementRunGNBDefinition
    traffic_config: MeasurementTrafficConfig
    tdd_config: MeasurementTDDConfig

    def validate(self):
        if self.run < 0:
            raise ValueError("Run number cannot be negative!")
        self.gnb_version.validate()
        self.traffic_config.validate()
        self.tdd_config.validate()


@dataclasses.dataclass(frozen=True)
class MeasurementRunFixedParams(Baseclass):
    distance_floor: float
    distance_nearest_wall: float
    location: Literal["B205","?"]
    distance_horizontal_in_m: float
    distance_vertical_in_m: float
    gnb_antenna_inclanation_in_degree: int | float
    gnb_antenna_rotation_in_degree: int | float
    ue_antenna_inclanation_in_degree: int | float
    ue_antenna_rotation_in_degree: int | float
    modem: Literal["SIM8200EA-M2 5G HAT", "Quectel RM520N-GL"]
    interface_ue: str
    interface_gnb: str
    jammer: bool
    sdr: Literal["B210", "X310"]
    performance_tuning: bool

    def validate(self):
        # TODO: no important validations
        return


@dataclasses.dataclass(frozen=True)
class MeasurementRunSystem(Baseclass):
    pcap_dump: str
    identifier: str
    fixed_params: MeasurementRunFixedParams

    def validate(self):
        self.fixed_params.validate()



@dataclasses.dataclass(frozen=True)
class MeasurementSeriesDefinition(Baseclass):
    description: str
    description_short: str
    system: MeasurementRunSystem
    run_definitions: list[MeasurementRunDefinition]

    def validate(self):
        self.system.validate()
        for r in self.run_definitions:
            r.validate()

    @classmethod
    def _from_dict(cls, dikt):
        assert("system" in dikt.keys())
        # system = MeasurementRunSystem._from_dict()

        assert("run_definitions" in dikt.keys())
        assert(isinstance(dikt["system"], list))



class NiceGUIInputMask():
    heading_classes = "font-bold text-3xl"
    semi_heading_classes = "font-bold text-lg"
    label_classes = "text-lg"
    layout_grid_classes = 'items-center m-0 pl-4 p-2 w-224 border-solid border-4 border-blue-400 rounded-xl'
    LAST_DEF_CACHE_RUNS = "last_run.json"
    LAST_DEF_CACHE_SYSTEM = "last_system.json"
    DEFAULT_DEF_CACHE_RUNS = "default_run.json"
    DEFAULT_DEF_CACHE_SYSTEM = "default_system.json"

    def __init__(self, init_type:Literal["auto", "default", "last_run"] = "auto") -> None:# {{{
        self.system = {"fixed_params":{}}
        self.runs = {}

        if init_type == "auto":
            try:
                with open(self.DEFAULT_DEF_CACHE_SYSTEM, "r") as f:
                    self.system = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open system param cache: {e}")

            try:
                with open(self.LAST_DEF_CACHE_SYSTEM, "r") as f:
                    self.system = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open system param cache: {e}")

            try:
                with open(self.DEFAULT_DEF_CACHE_RUNS, "r") as f:
                    self.runs = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open run param cache: {e}")

            try:
                with open(self.LAST_DEF_CACHE_RUNS, "r") as f:
                    self.runs = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open run param cache: {e}")
        elif init_type == "default":
            try:
                with open(self.DEFAULT_DEF_CACHE_SYSTEM, "r") as f:
                    self.system = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open system param cache: {e}")
            try:
                with open(self.DEFAULT_DEF_CACHE_RUNS, "r") as f:
                    self.runs = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open run param cache: {e}")
        elif init_type == "last_run":
            try:
                with open(self.LAST_DEF_CACHE_SYSTEM, "r") as f:
                    self.system = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open system param cache: {e}")
            try:
                with open(self.LAST_DEF_CACHE_RUNS, "r") as f:
                    self.runs = json.loads(f.read())
            except BaseException as e:
                print(f"Can't open run param cache: {e}")# }}}

    def __remove_last_run(self):
        if os.path.isfile(self.LAST_DEF_CACHE_RUNS):
            os.remove(self.LAST_DEF_CACHE_RUNS)
        if os.path.isfile(self.LAST_DEF_CACHE_SYSTEM):
            os.remove(self.LAST_DEF_CACHE_SYSTEM)


    def __create_chips_elements(self, chips_data_container, data_entry, options, label):# {{{
        """ Create a row-container with a dropdown to select from as well as chips which reflect the selections """
        chip_lookups = {}
        if data_entry not in chips_data_container.keys():
            chips_data_container[data_entry] = []
        def __chip_rm_callback(key):
            print(f"Removing {key}")
            chips_data_container[data_entry] = [c for c in chips_data_container[data_entry] if c != key]
            chip_lookups[key].delete()
            chip_lookups.pop(key)
            print(chip_lookups)
            self.build_final_run_definition()
        def __add_chip():

            if recieved_input.value == None or recieved_input.value in chips_data_container[data_entry]:
                recieved_input.value = None
                return
            with chips_row:
                print(f"Adding {recieved_input.value}")
                for k,_ in list(chip_lookups.items()):
                    chip_lookups[k].delete()
                    chip_lookups.pop(k)
                chips_data_container[data_entry] += [recieved_input.value]
                recieved_input.value = None
                for e in natsorted(chips_data_container[data_entry]):
                    chip_lookups[e] = ui.chip(str(e),
                                              # icon='leaderboard',
                                              color="#8babf1",
                                              removable=True, on_value_change=lambda key=e :__chip_rm_callback(key)) \
                        .classes("m-0")
                self.build_final_run_definition()
        with ui.element("div").classes("max-w-144 overflow-x-auto pl-4 border-solid border-2 border-blue-200 rounded-xl"):
            with ui.row().classes("flex-nowrap items-center gap-1 p-0") as chips_row:
                recieved_input = ui.select(options, label=label, on_change=__add_chip).classes("w-24 text-sm")# }}}
                if len(chip_lookups) == 0:
                    for e in natsorted(chips_data_container[data_entry]):
                        chip_lookups[e] = ui.chip(str(e), color="#8babf1",
                              removable=True, on_value_change=lambda key=e :__chip_rm_callback(key)).classes("m-0")
                print(f"Initial rendering of {data_entry}")

    def print_system(self):
        print("System:")
        print(self.system)
        try:
            s = copy.deepcopy(self.system)
            s.pop("description")
            s.pop("description_short")
            x = MeasurementRunSystem.from_dict({ "identifier": "xyz", **s })
            print("Correct")
        except Exception as e:
            print(f"Incorrect: {e}")
            exception_notification(e)
            # raise e
        print()


    def __handle_dl(self):
        try:
            fully_parsed = self.build_final_run_definition(safe=True)
            yaml_def = yaml.dump(dataclasses.asdict(fully_parsed), sort_keys=False)
            ui.download.content(yaml_def, "run_definition.yaml")
        except Exception as e:
            exception_notification(e)


    def render_page(self):
        self.render_system_params()
        self.render_run_params()
        with ui.footer().classes('bg-gray-600'):
            ui.button("Download", icon="file_download", on_click=self.__handle_dl).bind_text_from(Number_of_runs, "data", backward=lambda a : f"Download ({a} runs)")
            ui.button("Reset to defaults", icon="manage_history", on_click=lambda: (self.__remove_last_run(), ui.run_javascript('location.reload();')))

    def render_system_params(self):
        with ui.card():
            ui.label("System Parameters").classes(self.heading_classes)
            with ui.grid(columns="192px auto").classes(self.layout_grid_classes):# {{{
                ui.label("Short description").classes(self.label_classes)
                ui.input(label="desc", value="").bind_value(self.system, "description_short").on_value_change(self.print_system)
                ui.label("Full description").classes(self.label_classes)
                ui.input(label="desc", value="").bind_value(self.system, "description").on_value_change(self.print_system)

                ui.label("PCAP dump").classes(self.label_classes)
                ui.input(label="path").bind_value(self.system, "pcap_dump").on_value_change(self.print_system)

                ui.label("Lab location").classes(self.label_classes)
                ui.select(["B205"], label="Lab location", value="B205").bind_value(self.system["fixed_params"], "location").on_value_change(self.print_system)

                ui.label("Distance floor").classes(self.label_classes)
                ui.number(label="distance floor m").bind_value(self.system["fixed_params"], "distance_floor").on_value_change(self.print_system)

                ui.label("Distance walls").classes(self.label_classes)
                ui.number(label="distance wall m").bind_value(self.system["fixed_params"], "distance_nearest_wall").on_value_change(self.print_system)

                ui.label("Distance horizontal").classes(self.label_classes)
                ui.number(label="distance horizontal in m").bind_value(self.system["fixed_params"], "distance_horizontal_in_m").on_value_change(self.print_system)

                ui.label("Distance vertical").classes(self.label_classes)
                ui.number(label="distance vertical in m").bind_value(self.system["fixed_params"], "distance_vertical_in_m").on_value_change(self.print_system)

                ui.label("GNB inclanation").classes(self.label_classes)
                ui.number(label="gnb inclanation").bind_value(self.system["fixed_params"], "gnb_antenna_inclanation_in_degree").on_value_change(self.print_system)

                ui.label("GNB rotation").classes(self.label_classes)
                ui.number(label="gnb rotation").bind_value(self.system["fixed_params"], "gnb_antenna_rotation_in_degree").on_value_change(self.print_system)

                ui.label("UE inclanation").classes(self.label_classes)
                ui.number(label="ue inclanation").bind_value(self.system["fixed_params"], "ue_antenna_inclanation_in_degree").on_value_change(self.print_system)

                ui.label("UE rotation").classes(self.label_classes)
                ui.number(label="ue rotation").bind_value(self.system["fixed_params"], "ue_antenna_rotation_in_degree").on_value_change(self.print_system)

                ui.label("Modem").classes(self.label_classes)
                ui.select(["SIM8200EA-M2 5G HAT", "Quectel RM520N-GL"], label="Modem").bind_value(self.system["fixed_params"], "modem").on_value_change(self.print_system)

                ui.label("Interface name UE").classes(self.label_classes)
                ui.input(label="interface ue").bind_value(self.system["fixed_params"], "interface_ue").on_value_change(self.print_system)


                ui.label("Interface name gNB").classes(self.label_classes)
                ui.input(label="interface gnb").bind_value(self.system["fixed_params"], "interface_gnb").on_value_change(self.print_system)


                ui.label("Jammer").classes(self.label_classes)
                ui.checkbox("jammer").bind_value(self.system["fixed_params"], "jammer").on_value_change(self.print_system)

                ui.label("Software Defined Radio").classes(self.label_classes)
                ui.select(["B210", "X310"], label="SDR").bind_value(self.system["fixed_params"], "sdr").on_value_change(self.print_system)

                ui.label("Performance tuning").classes(self.label_classes)
                ui.checkbox("performance_tuning").bind_value(self.system["fixed_params"], "performance_tuning").on_value_change(self.print_system)
                # }}}

    def render_run_params(self):
        with ui.card():
            ui.label("Measurement Run Configurations").classes(self.heading_classes)

            ui.label("General").classes(self.semi_heading_classes)
            with ui.grid(columns="192px auto").classes(self.layout_grid_classes):
                ui.label("Number of run repetitions").classes(self.label_classes)
                ui.number(label="Repetitions").bind_value(self.runs, "run").on_value_change(self.print_runs)
                ui.label("Bandwidth").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs, data_entry="gnb_bandwidth", options=["20", "40"], label="bandwidth")
                ui.label("fixed DL MCS").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs, data_entry="dl_mcs", options=["None", *range(28)], label="DL MCS")
                # ui.label("DL MCS max").classes(self.label_classes)
                # self.__create_chips_elements(chips_data_container=self.runs, data_entry="dl_mcs_max", options=["None", *range(28)], label="DL MCS")
                ui.label("fixed UL MCS").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs, data_entry="ul_mcs", options=["None", *range(28)], label="UL MCS")
                # ui.label("UL MCS max").classes(self.label_classes)
                # self.__create_chips_elements(chips_data_container=self.runs, data_entry="ul_mcs_max", options=["None", *range(28)], label="UL MCS")
                ui.label("Dockerization").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs, data_entry="dockerization", options=[True, False], label="docker")

            ui.label("gNB Version").classes(self.semi_heading_classes)
            if "gnb_version" not in self.runs.keys():# {{{
                self.runs["gnb_version"] = {}
            with ui.grid(columns="192px auto").classes(self.layout_grid_classes):
                ui.label("UHD Version").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs["gnb_version"], data_entry="uhd_version", options=["UHD-3.15.LTS", "UHD-4.0"], label="UHD")
                ui.label("gNB Version").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs["gnb_version"], data_entry="gnb_version", options=["OAI v2.3.0", "OAI v2.2.0", "OAI v2.1.0", "srsRAN release_24_10", "srsRAN release_24_04"], label="version")
                # }}}

            ui.label("Traffic Config").classes(self.semi_heading_classes)
            with ui.card().classes(self.layout_grid_classes + " p-4"):# {{{
                if "traffic_config_iperf" not in self.runs.keys():
                    self.runs["traffic_config_iperf"] = {}
                with ui.row().classes("items-center"):
                    ui.checkbox().bind_value(self.runs["traffic_config_iperf"], "enabled").on_value_change(self.print_runs)
                    ui.label("Iperf").classes(self.semi_heading_classes)
                with ui.grid(columns="192px auto").classes("items-center m-0 pl-4 p-2 w-208 border-solid border-3 border-blue-400 rounded-xl"):
                    ui.label("duration").classes(self.label_classes)
                    self.__create_chips_elements(chips_data_container=self.runs["traffic_config_iperf"], data_entry="traffic_duration", options=[10,30,45,60,90,120], label="duration")
                    # ui.label("count")
                    ui.label("direction").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_iperf"], data_entry="direction", options=["Ul", "Dl"], label="direction")
                    ui.label("protocol").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_iperf"], data_entry="proto", options=["udp", "tcp"], label="proto")
                    ui.label("rate").classes(self.label_classes)
                    rate_selections = [f"{i}M" for i in range(20,210,10)] + [f"{i}M" for i in range(2,20,2)]
                    rate_selections = natsorted(rate_selections)
                    self.__create_chips_elements(self.runs["traffic_config_iperf"], data_entry="rate", options=rate_selections, label="rate")
                    ui.label("target ip").classes(self.label_classes)
                    ui.input(label="target IP").bind_value(self.runs["traffic_config_iperf"], "target_ip").on_value_change(self.print_runs)
                    ui.label("target port").classes(self.label_classes)
                    ui.input(label="target port").bind_value(self.runs["traffic_config_iperf"], "target_port").on_value_change(self.print_runs)

                if "traffic_config_idle" not in self.runs.keys():
                    self.runs["traffic_config_idle"] = {}
                with ui.row().classes("items-center"):
                    ui.checkbox().bind_value(self.runs["traffic_config_idle"], "enabled").on_value_change(self.print_runs)
                    ui.label("Idle").classes(self.semi_heading_classes)
                with ui.grid(columns="192px auto").classes("items-center m-0 pl-4 p-2 w-208 border-solid border-3 border-blue-400 rounded-xl"):
                    ui.label("duration").classes(self.label_classes)
                    self.__create_chips_elements(chips_data_container=self.runs["traffic_config_idle"], data_entry="traffic_duration", options=[10,30,45,60,90,120], label="duration")

                if "traffic_config_scapyping" not in self.runs.keys():
                    self.runs["traffic_config_scapyping"] = {}
                with ui.row().classes("items-center"):
                    ui.checkbox().bind_value(self.runs["traffic_config_scapyping"], "enabled").on_value_change(self.print_runs)
                    ui.label("Scapy UDP Ping").classes(self.semi_heading_classes)
                with ui.grid(columns="192px auto").classes("items-center m-0 pl-4 p-2 w-208 border-solid border-3 border-blue-400 rounded-xl"):
                    ui.label("duration").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_scapyping"], data_entry="traffic_duration", options=[10,30,45,60,90,120], label="duration")
                    ui.label("pkt size").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_scapyping"], data_entry="size", options=["small", "big"], label="size")
                    ui.label("burst size").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_scapyping"], data_entry="burst", options=["1"], label="burst")
                    ui.label("distribution").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_scapyping"], data_entry="dist", options=["det"], label="distribution")
                    ui.label("iat").classes(self.label_classes)
                    self.__create_chips_elements(self.runs["traffic_config_scapyping"], data_entry="iat", options=["0.1", "0.01", "0.001", "0.0001"], label="duration")
                    ui.label("target ip").classes(self.label_classes)
                    ui.input(label="target IP").bind_value(self.runs["traffic_config_scapyping"], "target_ip").on_value_change(self.print_runs)
                    ui.label("target port").classes(self.label_classes)
                    ui.input(label="target port").bind_value(self.runs["traffic_config_scapyping"], "target_port").on_value_change(self.print_runs)# }}}

            ui.label("TDD Config").classes(self.semi_heading_classes)
            if "tdd_config" not in self.runs.keys():# {{{
                self.runs["tdd_config"] = {}
            with ui.grid(columns="192px auto").classes(self.layout_grid_classes):
                ui.label("TDD Config - Ratio").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs["tdd_config"], data_entry="tdd_ratio", options=[1, 2, 4], label="Dl:Ul")

                ui.label("TDD Config - Period").classes(self.label_classes)
                self.__create_chips_elements(chips_data_container=self.runs["tdd_config"], data_entry="tdd_period", options=[5, 10, 20], label="period")# }}}

    def build_final_run_definition(self, safe=False):
        # first, add missing params
        d = construct_full_dict(self.runs)
        # then distribute inner lists to form a list of dicts
        run_definitions = expand_dict(d)
        # For multiple parts, the definitions are not complete, i.e. only ratio&period for tdd, fix that
        completed_definitions = cast_dicts(run_definitions)

        fixed_param_hash = dict_to_hash(self.system["fixed_params"], 4)
        for cd in completed_definitions:
            d = copy.deepcopy(cd)
            assert(isinstance(d,dict))
            d.pop("run")
            d.pop("identifier")
            traffic_naming = ""
            if d["traffic_config"]["traffic_type"] == "idle":
                traffic_naming = "idle---"
            elif d["traffic_config"]["traffic_type"] == "scapyudpping":
                traffic_naming = "scaping"
            elif d["traffic_config"]["traffic_type"] == "iperfthroughput":
                traffic_naming = "iperf"+d["traffic_config"]["direction"]
            else:
                raise ValueError(f"Unknown type of traffic: { d["traffic_config"] }")

            gnb_naming = d["gnb_version"]["type"]
            tdd_naming = f"TDD{d["tdd_config"]["tdd_period"]}-{d["tdd_config"]["tdd_ratio"]}"
            bw_naming = f"Bw{d["gnb_bandwidth"]}"

            cd["identifier"] = f"{fixed_param_hash}__{traffic_naming}_{gnb_naming}" + \
                                f"_{tdd_naming}_{bw_naming}__{cd["run"]:03d}_{dict_to_hash(d, 8)}"

        # now construct both parts,
        final = {
                "description": "",
                "description_short": "",
                "system": {
                    "pcap_dump":"/some/path",
                    "identifier": fixed_param_hash,
                    **self.system
                },
                "run_definitions":completed_definitions
            }
        final["description"] = final["system"]["description"]
        final["description_short"] = final["system"]["description_short"]
        final["system"].pop("description")
        final["system"].pop("description_short")

        print(final)

        fully_parsed = MeasurementSeriesDefinition.from_dict(final)
        # no assertion has been raised so save the config for the next visit
        if safe:
            with open(self.LAST_DEF_CACHE_RUNS, "w") as f:
                f.write(json.dumps(self.runs))
            with open(self.LAST_DEF_CACHE_SYSTEM, "w") as f:
                f.write(json.dumps(self.system))
        global Number_of_runs
        Number_of_runs["data"] = f"{len(completed_definitions)}"
        return fully_parsed

    def print_runs(self):
        self.build_final_run_definition()
        print("Runs:")
        print(self.runs)
        print("-")
        print(construct_full_dict(self.runs))
        print("-")
        print(expand_dict(construct_full_dict(self.runs)))
        print("-")
        print(cast_dicts(expand_dict(construct_full_dict(self.runs))))
        print("-")
        ccc = cast_dicts(expand_dict(construct_full_dict(self.runs)))
        print([dataclass_from_dict(MeasurementRunDefinition,d) for d in ccc])
        print("-")
        print()
        print()


def construct_full_dict(runs):
    constructed_dict = {
        "identifier": "dummy",
        "rx_gain": None,
        "tx_gain": None,
        **runs
            }

    constructed_dict["run"] = [i for i in range(int(constructed_dict["run"]))]
    constructed_dict.pop("traffic_config_iperf")
    constructed_dict.pop("traffic_config_idle")
    constructed_dict.pop("traffic_config_scapyping")
    traffic_configs = []
    if runs["traffic_config_iperf"]["enabled"]:
        this_traffic_configs = {**runs["traffic_config_iperf"]}
        this_traffic_configs.pop("enabled")
        this_traffic_configs["traffic_type"] = "iperfthroughput"
        traffic_configs.extend( expand_dict(this_traffic_configs) )
    if runs["traffic_config_idle"]["enabled"]:
        this_traffic_configs = {**runs["traffic_config_idle"]}
        this_traffic_configs.pop("enabled")
        this_traffic_configs["traffic_type"] = "idle"
        traffic_configs.extend( expand_dict(this_traffic_configs) )
    if runs["traffic_config_scapyping"]["enabled"]:
        this_traffic_configs = {**runs["traffic_config_scapyping"]}
        this_traffic_configs.pop("enabled")
        this_traffic_configs["traffic_type"] = "scapyudpping"
        traffic_configs.extend( expand_dict(this_traffic_configs) )
    constructed_dict["traffic_config"] = traffic_configs
    return constructed_dict

def cast_dicts(inp: list[dict]):
    ret = []
    for d in inp:
        parsed_gnb = MeasurementRunGNBDefinition.from_versions_numbers(**d["gnb_version"])
        parsed_gnb.validate()
        d["gnb_version"] = dataclasses.asdict(parsed_gnb)

        parsed_traf = MeasurementTrafficConfig.from_sparse_definition(**d["traffic_config"])
        parsed_traf.validate()
        d["traffic_config"] = dataclasses.asdict(parsed_traf)

        parsed_tdd = MeasurementTDDConfig.from_ratio_and_period(**d["tdd_config"])
        parsed_tdd.validate()
        d["tdd_config"] = dataclasses.asdict(parsed_tdd)

        ret.append(d)

    return ret

def dict_to_hash(d: dict, n: int) -> str:
    d_str = yaml.dump(d)
    return hashlib.sha256(d_str.encode()).hexdigest()[:n]









# f_eg = "./test.yaml"
# x = MeasurementSeriesDefinition.from_yaml(f_eg)

page = NiceGUIInputMask()
page.render_page()

ui.run(port=11111,show=False)

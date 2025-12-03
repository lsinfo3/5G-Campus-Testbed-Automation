#!/bin/env python3

from enum import Enum
import os
import time
from abc import ABC, abstractmethod
from typing import List
from tinkerforge.ip_connection import IPConnection
from tinkerforge.bricklet_voltage_current_v2 import BrickletVoltageCurrentV2
import argparse

class DataType(str, Enum):
    VOLTAGE = "voltage"
    CURRENT = "current"
    POWER = "power"

class ModuleBase(ABC):
    def module_name(self):
        return self.__class__.__name__

    @abstractmethod
    def start(self, mid: str):
        pass

    @abstractmethod
    def stop(self) -> List[str]:
        pass

    @abstractmethod
    def dumpconfig(self) -> str:
        pass

    @abstractmethod
    def cleanup(self):
        pass

class TinkerforgeModule(ModuleBase):
    def __init__(self, host: str, uid: str, port: int = 4223):
        self.host = host
        self.uid = uid
        self.port = port
        self._ipcon = IPConnection()
        self.vc = BrickletVoltageCurrentV2(self.uid, self._ipcon)
        self._ipcon.connect(self.host, self.port)
        self.vc.set_configuration(0, 0, 0)
        self._target = None
        self.vc.register_callback(
            BrickletVoltageCurrentV2.CALLBACK_VOLTAGE,
            lambda x: self._write(x, DataType.VOLTAGE),
        )
        self.vc.register_callback(
            BrickletVoltageCurrentV2.CALLBACK_CURRENT,
            lambda x: self._write(x, DataType.CURRENT),
        )
        self.vc.register_callback(
            BrickletVoltageCurrentV2.CALLBACK_POWER,
            lambda x: self._write(x, DataType.POWER),
        )
        self.mid = None

    def _write(self, data: int, type: DataType):
        if not self._target:
            raise ValueError("Called before start")
        self._target.write(f"{time.time()},{type.value},{data}\n")

    def start(self, mid: str):
        os.makedirs(os.path.dirname(mid), exist_ok=True)
        self.mid = mid
        self._target = open(f"{mid}", "w")
        self._target.write(f"TIME,TYPE,VAL\n")
        self.vc.set_power_callback_configuration(1, False, "x", 0, 0)
        self.vc.set_voltage_callback_configuration(1, False, "x", 0, 0)
        self.vc.set_current_callback_configuration(1, False, "x", 0, 0)

    def stop(self) -> List[str]:
        if not self.mid:
            raise ValueError("Called before start")
        mid = self.mid
        self.mid = None
        self.vc.set_power_callback_configuration(0, False, "x", 0, 0)
        self.vc.set_voltage_callback_configuration(0, False, "x", 0, 0)
        self.vc.set_current_callback_configuration(0, False, "x", 0, 0)
        self._target.close()
        self._target = None
        return [f"{mid}"]

    def dumpconfig(self) -> str:
        raise NotImplementedError

    def cleanup(self):
        self._ipcon.disconnect()


def main(host:str, uid:str, dir:str, duration:int|float):
    handler = TinkerforgeModule(host=host, uid=uid)
    handler.start(mid=dir)
    time.sleep(duration)
    handler.stop()
    handler.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Tinkerforge Power Measurement")
    parser.add_argument("--host", help="IP of tinkerforge masterbrick", required=True)
    parser.add_argument("--uid", help="UID of the Voltage/Current Bricklet", required=True)
    parser.add_argument("--path", help="Storage path for .csv", required=True)
    parser.add_argument("--duration", help="How long the measurement should run", type=float, required=True)
    args = parser.parse_args()
    main(host=args.host, uid=args.uid, duration=args.duration, dir=args.path)


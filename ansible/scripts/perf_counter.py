from datetime import datetime, timezone
import os
from subprocess import Popen
from typing import List, Optional
import time
from abc import ABC, abstractmethod

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

class PerfModule(ModuleBase):
    def __init__(
        self,
        pid: Optional[List[str]] = None,
        interval: int = 100,
        events: List[str] = [
            "cycles",
            "ref-cycles",
            "instructions",
            "cache-misses",
            "dTLB-load-misses",
        ],
    ):
        self.pid = pid
        self.interval = interval
        self.events = events

    def start(self, mid: str):
        self.curmid = mid
        time = datetime.now(timezone.utc)
        command = [
            "/usr/bin/perf",
            "stat",
            "-I",
            f"{self.interval}",
            "-e",
            ",".join(self.events),
            "-x",
            ";",
            "-o",
            f"{mid}/{time.timestamp()}-perfdata",
        ]

        if self.pid:
            pids = []
            for pid in self.pid:
                pids.append(int(os.popen(f"/usr/bin/pgrep {pid}").read()))
            self.file = f"{mid}/{time.timestamp()}-{','.join(self.pid)}-perfdata"
            command[-1] = self.file
            command += ["-p", ",".join(map(str, pids))]
        else:
            self.file = f"{mid}/{time.timestamp()}-perfdata"
        print(" ".join(command))
        self.process = Popen(command)

    def stop(self) -> List[str]:
        if self.process:
            self.process.terminate()
        files = [self.file]
        self.curmid = None
        self.file = None
        return files

    def dumpconfig(self) -> str:
        return f"pid: {self.pid}, interval: {self.interval}, events: {self.events}"

    def cleanup(self):
        pass




def main(host:str, uid:str, dir:str, duration:int|float):
    handler = PerfModule(host=host, uid=uid)
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



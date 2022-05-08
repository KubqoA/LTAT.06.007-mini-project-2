from time import sleep
import rpyc
import _thread
from typing import Callable, Dict, List, Literal, Optional, TypeVar

R = TypeVar("R")
Property = Literal["id", "role", "majority", "state"]
Role = Literal["primary", "secondary"]
Order = Literal["attack", "retreat"]
State = Literal["faulty", "non-faulty"]


def rpyc_exec(port: int, fn: Callable[["GeneralServiceType"], R]) -> R:
    conn = rpyc.connect("localhost", port)
    return fn(conn)


# Class to help with typing when referencing to the exposed functions
class GeneralServiceType:
    root: "GeneralService"


class GeneralService(rpyc.Service):  # type: ignore
    def __init__(self, general: "General") -> None:
        super().__init__()
        self.general = general

    def exposed_list(self, properties: List[Property]) -> str:
        return self.general.list(properties)

    def exposed_get_id(self) -> int:
        return self.general.id

    def exposed_set_state(self, state: State) -> None:
        self.general.state = state

    def exposed_stop(self) -> None:
        return self.general.stop()


class General:
    def __init__(self, id: int, port: int) -> None:
        self.id = id
        self.port = port
        self.role: Role = "secondary"
        self.order: Optional[Order] = None
        self.majority: Optional[Order] = None
        self.state: State = "non-faulty"

        self.server: Optional[rpyc.ThreadedServer] = None

    def start(self) -> None:
        _thread.start_new_thread(self.run, ())

    def stop(self) -> None:
        _thread.start_new_thread(self.kill, ())

    def run(self) -> None:
        self.server = rpyc.ThreadedServer(GeneralService(self), port=self.port)
        self.server.start()

    def kill(self) -> None:
        if self.server is None:
            return
        sleep(1)
        self.server.close()

    def list(self, properties: List[Property]) -> str:
        getters: Dict[Property, Callable[[], str]] = {
            "id": lambda: f"G{self.id}",
            "role": lambda: self.role,
            "majority": lambda: "majority=undefined"
            if self.majority is None
            else f"majority={self.majority}",
            "state": lambda: "state=F" if self.state == "faulty" else "state=NF",
        }
        return ", ".join(map(lambda property: getters[property](), properties))

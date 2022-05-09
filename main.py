import sys
from time import sleep
import rpyc
import _thread
from typing import Callable, Dict, List, Literal, Optional, TypeVar, cast

general_ports: List[int] = []

R = TypeVar("R")
Property = Literal["id", "role", "majority", "state"]
Role = Literal["primary", "secondary"]
Order = Literal["attack", "retreat"]
State = Literal["faulty", "non-faulty"]

Message = Literal["election", "ok", "coordinator"]


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

    def exposed_is_alive(self) -> bool:
        return self.general.is_alive()

    def exposed_send_message(
        self, sender_port: int, sender_id: int, message: Message
    ) -> Optional[Message]:
        return self.general.process_message(sender_port, sender_id, message)


class General:
    def __init__(self, id: int, port: int) -> None:
        self.id = id
        self.port = port
        self.role: Role = "secondary"
        self.order: Optional[Order] = None
        self.majority: Optional[Order] = None
        self.state: State = "non-faulty"

        # Leader election related
        self.elections: int = 0
        self.primary_general_port: Optional[int] = None

        self.server: Optional[rpyc.ThreadedServer] = None

    def start(self) -> None:
        _thread.start_new_thread(self._run, ())
        _thread.start_new_thread(self._tick, ())

    def stop(self) -> None:
        _thread.start_new_thread(self._kill, ())

    def _run(self) -> None:
        self.server = rpyc.ThreadedServer(GeneralService(self), port=self.port)
        self.server.start()

    def _tick(self) -> None:
        # Wait for server to become online
        while self.server is None:
            sleep(0.1)

        # While the server is online keep the election process running
        # Run the process once every 5 seconds
        while self.server and self.server.active:
            # If I am not primary general and the primary general port is set
            # check if that general is still alive
            if self.role != "primary" and self.primary_general_port:
                try:
                    primary_general_alive = rpyc_exec(
                        self.primary_general_port,
                        lambda conn: conn.root.exposed_is_alive(),
                    )
                    if not primary_general_alive:
                        self.primary_general_port = None
                except:
                    # If we get an exception assume the primary general is
                    # no longer alive
                    self.primary_general_port = None

            if self.primary_general_port is None:
                self._election()

            sleep(5)

    def _election(self) -> None:
        # Send election message to all other generals
        # If they don't respond or the connection cannot be established
        # consider it as an election vote for this general
        for port in general_ports:
            if port == self.port:
                continue

            try:
                election_response = rpyc_exec(
                    port,
                    lambda conn: conn.root.exposed_send_message(
                        self.port, self.id, "election"
                    ),
                )

                if election_response is None:
                    self.elections += 1

                # If the asked general is already primary then it will send
                # the coordinator response and we set mark the port
                # as the primary_general_port and return
                if election_response == "coordinator":
                    self.primary_general_port = port
                    return
            except:
                self.elections += 1

        # If this general now haves the required election votes propagate
        # the result with coordinator message to other generals
        if self.elections == len(general_ports) - 1:
            self.role = "primary"
            self.primary_general_port = self.port

            for port in general_ports:
                if port == self.port:
                    continue

                try:
                    rpyc_exec(
                        port,
                        lambda conn: conn.root.exposed_send_message(
                            self.port, self.id, "coordinator"
                        ),
                    )
                except:
                    pass

        # Reset the elections counter to 0, for next election cycle
        self.elections = 0

    def _kill(self) -> None:
        if self.server is None:
            return
        server = self.server
        self.server = None

        # Close the server after a half second delay to prevent the client
        # disconnecting in middle of sending the kill message
        sleep(0.5)
        server.close()

    def is_alive(self) -> bool:
        return self.server is not None

    def process_message(
        self, sender_port: int, sender_id: int, message: Message
    ) -> Optional[Message]:
        if not self.server or not self.server.active:
            return None

        if message == "coordinator":
            self.primary_general_port = sender_port
            return None

        if message == "election" and self.role == "primary":
            return "coordinator"

        if message == "election" and sender_id < self.id:
            return "ok"

        return None

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


def create_generals(count: int, base_port: int = 18812) -> None:
    """Create generals and start them"""
    global general_ports

    # Start all the generals
    general_ports = [base_port + id for id in range(0, count)]
    for id, port in enumerate(general_ports):
        General(id + 1, port).start()


def list_generals(properties: List[Property]) -> None:
    """List the generals and their properties from the list of properties"""
    for port in general_ports:
        print(
            rpyc_exec(
                port,
                lambda conn: conn.root.exposed_list(properties),
            )
        )


def actual_order(args: List[str]) -> None:
    # Validate args
    if len(args) != 1 or args[0] not in ["attack", "retreat"]:
        print("Usage: actual-order [attack/retreat]")
        return
    order = cast(Order, args[0])

    # TODO: Order logic

    list_generals(["id", "role", "majority", "state"])


def g_state(args: List[str]) -> None:
    # Validate args
    if len(args) == 0:
        # If no args were passed just list the generals
        list_generals(["id", "role", "state"])
        return
    if (
        len(args) != 2
        or not args[0].isdigit()
        or args[1] not in ["faulty", "non-faulty"]
    ):
        print("Usage: g-state [ID] [faulty/non-faulty]")
        return
    id = int(args[0])
    state = cast(State, args[1])

    for port in general_ports:
        general_id = rpyc_exec(port, lambda conn: conn.root.exposed_get_id())

        if general_id == id:
            rpyc_exec(port, lambda conn: conn.root.exposed_set_state(state))
            list_generals(["id", "state"])
            return

    print(f"General with id {id} doesn't exist")


def g_kill(args: List[str]) -> None:
    global general_ports

    # Validate args
    if len(args) != 1 or not args[0].isdigit():
        print("Usage: g-kill [ID]")
        return
    id = int(args[0])

    for i, port in enumerate(general_ports):
        general_id = rpyc_exec(port, lambda conn: conn.root.exposed_get_id())

        if general_id == id:
            rpyc_exec(port, lambda conn: conn.root.exposed_stop())
            general_ports.pop(i)
            list_generals(["id", "state"])
            return

    print(f"General with id {id} doesn't exist")


def g_add(args: List[str]) -> None:
    global general_ports

    # Validate args
    if len(args) != 1 or not args[0].isdigit():
        print("Usage: g-add [K]")
        return
    k = int(args[0])

    last_id = rpyc_exec(general_ports[-1], lambda conn: conn.root.exposed_get_id())

    for i in range(k):
        general_ports.append(general_ports[-1] + 1)
        General(last_id + i + 1, general_ports[-1]).start()

    list_generals(["id", "role"])


def main() -> None:
    # Check for correctness of provided arguments
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print(f"Usage: {sys.argv[0]} [number_of_processes]", file=sys.stderr)
        sys.exit(1)

    n = int(sys.argv[1])
    if n <= 0:
        print("Number of processes must be greater than 0")
        sys.exit(1)

    print(f"Creating {n} generals")
    create_generals(n)

    # Start= the command line interface
    while True:
        try:
            user_input = input("$ ")
        except EOFError as _:
            # handle Ctrl+d as end of program
            print()
            sys.exit(1)

        args = user_input.split()

        # If no args found, e.g. empty or only whitespace input continue with next cycle
        if len(args) == 0:
            continue

        cmd = args[0]

        # Define the handlers for commands
        handlers: Dict[str, Callable[[List[str]], None]] = {
            "help": lambda _: print(
                "Supported commands: actual-order, g-state, g-kill, g-add, help, whoami, exit"
            ),
            "exit": lambda _: sys.exit(0),
            "whoami": lambda _: print("Jakub Arbet, C20301"),
            "actual-order": actual_order,
            "g-state": g_state,
            "g-kill": g_kill,
            "g-add": g_add,
        }

        # Execute appropriate handler or print error message
        handlers.get(
            cmd,
            lambda _: len(cmd) > 0
            and not cmd.isspace()
            and print(f"{cmd}: command not found"),
        )(args[1:])


if __name__ == "__main__":
    main()

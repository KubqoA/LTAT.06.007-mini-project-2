import general
from general import general_ports
import sys
from typing import Callable, Dict, List, cast


def create_generals(count: int, base_port: int = 18812) -> None:
    """Create generals and start them"""
    global general_ports

    # Start all the generals
    general_ports = [base_port + id for id in range(0, count)]
    for id, port in enumerate(general_ports):
        general.General(id + 1, port).start()


def list_generals(properties: List[general.Property]) -> None:
    """List the generals and their properties from the list of properties"""
    for port in general_ports:
        print(
            general.rpyc_exec(
                port,
                lambda conn: conn.root.exposed_list(properties),
            )
        )


def actual_order(args: List[str]) -> None:
    # Validate args
    if len(args) != 1 or args[0] not in ["attack", "retreat"]:
        print("Usage: actual-order [attack/retreat]")
        return
    order = cast(general.Order, args[0])

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
    state = cast(general.State, args[1])

    for port in general_ports:
        general_id = general.rpyc_exec(port, lambda conn: conn.root.exposed_get_id())

        if general_id == id:
            general.rpyc_exec(port, lambda conn: conn.root.exposed_set_state(state))
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
        general_id = general.rpyc_exec(port, lambda conn: conn.root.exposed_get_id())

        if general_id == id:
            general.rpyc_exec(port, lambda conn: conn.root.exposed_stop())
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

    last_id = general.rpyc_exec(
        general_ports[-1], lambda conn: conn.root.exposed_get_id()
    )

    for i in range(k):
        general_ports.append(general_ports[-1] + 1)
        general.General(last_id + i + 1, general_ports[-1]).start()

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

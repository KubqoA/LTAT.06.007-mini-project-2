import sys
from typing import Callable, Dict, List


def create_generals(n: int, basePort: int = 18812) -> List[int]:
    # Start all the generals
    ports = [basePort + id for id in range(0, n)]
    for id, port in enumerate(ports):
        # TODO: Start generals
        pass
    return ports


def actual_order(args: List[str]):
    pass


def g_state(args: List[str]):
    pass


def g_kill(args: List[str]):
    pass


def g_add(args: List[str]):
    pass


if __name__ == "__main__":
    # Check for correctness of provided arguments
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("Usage: %s [number_of_processes]" % sys.argv[0], file=sys.stderr)
        sys.exit(1)

    n = int(sys.argv[1])
    if n <= 0:
        print("Number of processes must be greater than 0")
        sys.exit(1)

    print("Creating %d generals" % n)
    create_generals(n)

    # Start= the command line interface
    while True:
        try:
            user_input = input("$ ")
        except EOFError as e:
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
                "Supported commands: actual-order, g-state [ID] [state], g-kill [ID], g-add [K], help, whoami, exit"
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
            and print("%s: command not found" % cmd),
        )(args[0:])

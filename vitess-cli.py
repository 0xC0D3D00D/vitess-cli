import argparse
import cmd
import sys

from vtdb import vtgate_client
from vtdb import dbexceptions
from vtdb import grpc_vtgate_client # pylint: disable=unused-import

class SqlForwarder(cmd.Cmd):
    """Forwards the commands from user to vtgate"""
    def __init__(self, vtgate_conn):
        cmd.Cmd.__init__(self)
        self.vtgate_conn = vtgate_conn
        self.prompt = "\033[4;32mvtcli>\033[0m "
        self.tablet_type = "master"

    def default(self, line):
        if line == "q" or line == "quit":
            return True

        if line == "tablet type replica":
            self.tablet_type = "replica"
            return False
        if line == "tablet type master":
            self.tablet_type = "master"
            return False

        try:
            cursor = self.vtgate_conn.cursor(tablet_type=self.tablet_type)
            cursor.execute(line, {})
        
            for row in cursor.fetchall():
                print(row)
            cursor.close()
        except dbexceptions.DatabaseError, e:
            print("\033[1;31mError"+ str(e) + "\033[0m")
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("--version", help="show the program version",
                        action="store_true")
    parser.add_argument("-g", "--vtgate-host", help="vtgate host",
                        dest="vtgate_host", default="localhost")
    parser.add_argument("-p", "--vtgate-port", help="vtgate port",
                        dest="vtgate_port", default="15991")
    parser.add_argument("-C", "--vtctld-host", help="vtctld host",
                        dest="vtctld_host", default="localhost")
    parser.add_argument("-P", "--vtctld-port", help="vtctld port",
                        dest="vtctld_port", default="15999")
    parser.add_argument("-c", "--cell", help="cell", dest="cell")
    args = parser.parse_args()

    if not args.cell:
        return 1

    vtgate_server = args.vtgate_host + ":" + args.vtgate_port
    vtgate_conn = vtgate_client.connect('grpc', vtgate_server, 3600)

    SqlForwarder(vtgate_conn).cmdloop()

    return 0

if __name__ == "__main__":
    sys.exit(main())

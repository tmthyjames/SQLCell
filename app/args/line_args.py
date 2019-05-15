import argparse
import shlex

class ArgHandler(object):
    def __init__(self, line):
        self.parser = argparse.ArgumentParser(description='SQLCell arguments')
        self.parser.add_argument(
            "-e", "--engine", 
            help='Engine param, specify your connection string: --engine=postgresql://user:password@localhost:5432/mydatabase', 
            required=False
        )
        self.parser.add_argument(
            "-v", "--var", 
            help='Variable name to write output to: --var=foo', 
            required=False
        )
        self.parser.add_argument(
            "-b", "--background", 
            help='whether to run query in background or not: --background runs in background', 
            required=False, default=False, action="store_true"
        )
        self.parser.add_argument(
            "-k", "--hook", 
            help='define shortcuts with the --hook param',
            required=False, default=False, action="store_true"
        )
        self.parser.add_argument(
            "-r", "--refresh", 
            help='refresh engines by specifying --refresh flag',
            required=False, default=False, action="store_true"
        )
        self.args = self.parser.parse_args(shlex.split(line))

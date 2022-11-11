import argparse


def read_config_from_command_line():
    """
        Read the "custom" config file from the command line

        Args:

        Returns:
            The full path to the config file
    """
    # Create Parser
    parser = argparse.ArgumentParser(description='parser for yaml config file')

    # Add arguments
    parser.add_argument('Path', metavar='path', type=str,
                        help='the full path to config file')

    # Execute the parse_args() method
    args = parser.parse_args()
    return args.Path


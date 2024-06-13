import sys
import os

from pathlib import Path

# add METdataio directory to path so packages can be found
top_dir = str(Path(__file__).parents[0])
sys.path.insert(0, os.path.abspath(top_dir))

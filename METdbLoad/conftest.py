import pytest
import sys
import os

from pathlib import Path

# add METdataio directory to path so packages can be found
top_dir = str(Path(__file__).parents[1])
sys.path.insert(0, os.path.abspath(top_dir))

@pytest.fixture
def get_xml_loadfile():
    def load_and_read_xml():
        from METdataio.METdbLoad.ush.read_load_xml import XmlLoadFile

        XML_FILE = '/Users/venita.hagerty/metviewer/testloadv10fewp3.xml'
        XML_LOADFILE = XmlLoadFile(XML_FILE)
        XML_LOADFILE.read_xml()
        return XML_LOADFILE

    return load_and_read_xml

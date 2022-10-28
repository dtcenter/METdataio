import re
import pandas as pd
from METdataio.METdbLoad.ush import constants as CN


if __name__ == "__main__":
    # filename = '/Volumes/d1/minnawin/feature_121_met_reformatter/data/expected/ensemble/ens_ss.data'
    filename = '/Volumes/d1/minnawin/feature_121_met_reformatter/data/point_stat/point_stat_FV3_GFS_v15p2_CONUS_25km_NDAS_ADPSFC_010000L_20190615_010000V.stat'

    # Suggestion from SO, to create a single column dataframe, then split on ','
    # (this is for a csv file with multiple, varying number of columns)
    # df = pd.read_fwf('<filename>.csv', header=None)
    # df[0].str.split(',', expand=True)

    with open(filename, 'r') as fh:
        header = fh.readline()
        # Replace one space separator with a comma
        columns = re.split('\s+', header)
    hdr_names = []
    for col in columns:
        hdr_names.append(col.lower())
    print(hdr_names)

    hdr_names_df = pd.read_csv(filename, engine='python', delim_whitespace=True)
    hdr_names_upper = list(hdr_names_df.columns.values)
    hdr_names = [x.lower() for x in hdr_names_upper]

    ptstat = pd.read_csv(filename, delim_whitespace=True,
                           names=hdr_names, skiprows=1,
                           keep_default_na=False, na_values='', low_memory=False)
    print(ptstat)



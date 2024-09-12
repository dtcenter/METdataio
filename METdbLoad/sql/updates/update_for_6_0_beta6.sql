DELIMITER |


ALTER TABLE line_data_mpr
    RENAME COLUMN climo_mean TO obs_climo_mean |
    RENAME COLUMN climo_stdev TO obs_climo_stdev |
    RENAME COLUMN climo_cdf TO obs_climo_cdf |
    ADD COLUMN fcst_climo_mean DOUBLE |
    ADD COLUMN fcst_climo_stdev DOUBLE |

ALTER TABLE line_data_orank
    RENAME COLUMN climo_mean TO obs_climo_mean |
    RENAME COLUMN climo_stdev TO obs_climo_stdev |
    ADD COLUMN fcst_climo_mean DOUBLE |
    ADD COLUMN fcst_climo_stdev DOUBLE |

ALTER TABLE line_data_cts
    ADD COLUMN lodds DOUBLE DEFAULT -9999 |
    ADD COLUMN lodds_ncl DOUBLE DEFAULT -9999 |
    ADD COLUMN lodds_ncu DOUBLE DEFAULT -9999|
    ADD COLUMN lodds_bcl DOUBLE DEFAULT -9999|
    ADD COLUMN lodds_bcu DOUBLE DEFAULT -9999|
    ADD COLUMN orss DOUBLE DEFAULT -9999|
    ADD COLUMN orss_ncl DOUBLE DEFAULT -9999|
    ADD COLUMN orss_ncu DOUBLE DEFAULT -9999|
    ADD COLUMN orss_bcl DOUBLE DEFAULT -9999|
    ADD COLUMN orss_bcu DOUBLE DEFAULT -9999|
    ADD COLUMN eds DOUBLE DEFAULT -9999|
    ADD COLUMN eds_ncl DOUBLE DEFAULT -9999|
    ADD COLUMN eds_ncu DOUBLE DEFAULT -9999|
    ADD COLUMN eds_bcl DOUBLE DEFAULT -9999|
    ADD COLUMN eds_bcu DOUBLE DEFAULT -9999|
    ADD COLUMN seds DOUBLE DEFAULT -9999|
    ADD COLUMN seds_ncl DOUBLE DEFAULT -9999|
    ADD COLUMN seds_ncu DOUBLE DEFAULT -9999|
    ADD COLUMN seds_bcl DOUBLE DEFAULT -9999|
    ADD COLUMN seds_bcu DOUBLE DEFAULT -9999|
    ADD COLUMN edi DOUBLE DEFAULT -9999|
    ADD COLUMN edi_ncl DOUBLE DEFAULT -9999|
    ADD COLUMN edi_ncu DOUBLE DEFAULT -9999|
    ADD COLUMN edi_bcl DOUBLE DEFAULT -9999|
    ADD COLUMN edi_bcu DOUBLE DEFAULT -9999|
    ADD COLUMN sedi DOUBLE DEFAULT -9999 | 
    ADD COLUMN sedi_ncl DOUBLE DEFAULT -9999 | 
    ADD COLUMN sedi_ncu DOUBLE DEFAULT -9999 | 
    ADD COLUMN sedi_bcl DOUBLE DEFAULT -9999 | 
    ADD COLUMN sedi_bcu DOUBLE DEFAULT -9999 | 
    ADD COLUMN bagss DOUBLE DEFAULT -9999 |
    ADD COLUMN bagss_bcl DOUBLE DEFAULT -9999 |
    ADD COLUMN bagss_bcu DOUBLE DEFAULT -9999 |

|

DELIMITER ;

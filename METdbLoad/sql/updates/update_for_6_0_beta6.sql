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

ALTER TABLE line_data_seeps
    RENAME COLUMN s12 TO odfl |
    RENAME COLUMN s13 TO odfh |
    RENAME COLUMN s21 TO olfd |
    RENAME COLUMN s23 TO olfh |
    RENAME COLUMN s31 TO ohfd |
    RENAME COLUMN s32 TO ohfl |

|

DELIMITER ;

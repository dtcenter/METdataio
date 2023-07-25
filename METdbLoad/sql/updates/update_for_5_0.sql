DELIMITER |


ALTER TABLE line_data_ctc
    ADD COLUMN ec_value   DOUBLE DEFAULT 0.5 |

ALTER TABLE line_data_cts
    ADD COLUMN hss_ec DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_cts
    ADD COLUMN hss_ec_bcl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_cts
    ADD COLUMN hss_ec_bcu DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_cts
    ADD COLUMN ec_value DOUBLE DEFAULT 0.5 |

ALTER TABLE line_data_val1l2
    ADD COLUMN fa_speed_bar DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN oa_speed_bar DOUBLE |

ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_ncl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_ncu DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_bcl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_bcu DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_uncntr DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_uncntr_bcl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_vcnt
    ADD COLUMN anom_corr_uncntr_bcu DOUBLE DEFAULT -9999 |

UPDATE line_data_mctc
    SET ec_value= IF (ec_value=-9999,1/n_cat,ec_value )  |
UPDATE line_data_mcts
    SET ec_value= IF (ec_value=-9999,1/n_cat,ec_value ) |

ALTER TABLE  line_data_ecnt
    ADD COLUMN crps_emp_fair DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN spread_md DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN mae DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN mae_oerr DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN bias_ratio DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN n_ge_obs INTEGER DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN me_ge_obs DOUBLE DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN n_lt_obs INTEGER DEFAULT -9999 |
ALTER TABLE  line_data_ecnt
    ADD COLUMN me_lt_obs DOUBLE DEFAULT -9999 |

DROP TABLE IF EXISTS line_data_seeps|
CREATE TABLE line_data_seeps
(
    stat_header_id INT UNSIGNED NOT NULL,
    data_file_id   INT UNSIGNED NOT NULL,
    line_num       INT UNSIGNED,
    fcst_lead      INT,
    fcst_valid_beg DATETIME,
    fcst_valid_end DATETIME,
    fcst_init_beg  DATETIME,
    obs_lead       INT UNSIGNED,
    obs_valid_beg  DATETIME,
    obs_valid_end  DATETIME,
    total          INT UNSIGNED,
    s12            DOUBLE,
    s13            DOUBLE,
    s21            DOUBLE,
    s23            DOUBLE,
    s31            DOUBLE,
    s32            DOUBLE,
    pf1            DOUBLE,
    pf2            DOUBLE,
    pf3            DOUBLE,
    pv1            DOUBLE,
    pv2            DOUBLE,
    pv3            DOUBLE,
    mean_fcst      DOUBLE,
    mean_obs       DOUBLE,
    seeps          DOUBLE,

    CONSTRAINT line_data_seeps_data_file_id_pk
        FOREIGN KEY (data_file_id)
            REFERENCES data_file (data_file_id),
    CONSTRAINT line_data_seeps_stat_header_id_pk
        FOREIGN KEY (stat_header_id)
            REFERENCES stat_header (stat_header_id),
    INDEX stat_header_id_idx (stat_header_id)
) ENGINE = MyISAM
  CHARACTER SET = latin1|

DROP TABLE IF EXISTS line_data_seeps_mpr|
CREATE TABLE line_data_seeps_mpr
(
    stat_header_id INT UNSIGNED NOT NULL,
    data_file_id   INT UNSIGNED NOT NULL,
    line_num       INT UNSIGNED,
    fcst_lead      INT,
    fcst_valid_beg DATETIME,
    fcst_valid_end DATETIME,
    fcst_init_beg  DATETIME,
    obs_lead       INT UNSIGNED,
    obs_valid_beg  DATETIME,
    obs_valid_end  DATETIME,
    obs_sid        VARCHAR(32),
    obs_lat        DOUBLE,
    obs_lon        DOUBLE,
    fcst           DOUBLE,
    obs            DOUBLE,
    obs_qc         VARCHAR(32),
    fcst_cat       INT UNSIGNED,
    obs_cat        INT UNSIGNED,
    p1             DOUBLE,
    p2             DOUBLE,
    t1             DOUBLE,
    t2             DOUBLE,
    seeps          DOUBLE,

    CONSTRAINT line_data_seeps_mpr_data_file_id_pk
        FOREIGN KEY (data_file_id)
            REFERENCES data_file (data_file_id),
    CONSTRAINT line_data_seeps_mpr_stat_header_id_pk
        FOREIGN KEY (stat_header_id)
            REFERENCES stat_header (stat_header_id),
    INDEX stat_header_id_idx (stat_header_id)
) ENGINE = MyISAM
  CHARACTER SET = latin1|

DROP TABLE IF EXISTS line_data_tcdiag|
CREATE TABLE line_data_tcdiag
(
    line_data_id   INT UNSIGNED NOT NULL,
    tcst_header_id INT UNSIGNED NOT NULL,
    data_file_id   INT UNSIGNED NOT NULL,
    line_num       INT UNSIGNED NOT NULL,
    fcst_lead      INT,
    fcst_valid     DATETIME,
    fcst_init      DATETIME,
    total          INT UNSIGNED,
    index_pair     DOUBLE,
    diag_source    VARCHAR(20),
    track_source   VARCHAR(20),
    field_source   VARCHAR(20),
    n_diag         INT UNSIGNED,

    PRIMARY KEY (line_data_id),
    CONSTRAINT line_data_tcdiag_data_file_id_pk
        FOREIGN KEY (data_file_id)
            REFERENCES data_file (data_file_id),
    CONSTRAINT line_data_tcdiag_stat_header_id_pk
        FOREIGN KEY (tcst_header_id)
            REFERENCES tcst_header (tcst_header_id),
    INDEX tcst_header_id_idx (tcst_header_id)
) ENGINE = MyISAM
  CHARACTER SET = latin1|

DROP TABLE IF EXISTS line_data_tcdiag_diag|
CREATE TABLE line_data_tcdiag_diag
(
    line_data_id INT UNSIGNED NOT NULL,
    i_value      INT UNSIGNED NOT NULL,
    diag_i       VARCHAR(20),
    value_i      DOUBLE,

    PRIMARY KEY (line_data_id, i_value),
    CONSTRAINT line_data_tcdiag_id_pk
        FOREIGN KEY (line_data_id)
            REFERENCES line_data_tcdiag (line_data_id)
) ENGINE = MyISAM
  CHARACTER SET = latin1|


DELIMITER ;
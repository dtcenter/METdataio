DELIMITER |


ALTER TABLE line_data_mcts
    ADD COLUMN hss_ec DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_mcts
    ADD COLUMN hss_ec_bcl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_mcts
    ADD COLUMN hss_ec_bcu DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_mcts
    ADD COLUMN ec_value DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_mctc
    ADD COLUMN ec_value DOUBLE DEFAULT -9999 |

ALTER TABLE line_data_cnt
    ADD COLUMN si DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_cnt
    ADD COLUMN si_bcl DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_cnt
    ADD COLUMN si_bcu DOUBLE DEFAULT -9999 |

ALTER TABLE line_data_dmap
    ADD COLUMN g DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_dmap
    ADD COLUMN gbeta DOUBLE DEFAULT -9999 |
ALTER TABLE line_data_dmap
    ADD COLUMN beta_value DOUBLE DEFAULT -9999 |

ALTER TABLE stat_header
    MODIFY model VARCHAR(80) |


DROP TABLE IF EXISTS line_data_ssidx|
CREATE TABLE line_data_ssidx
(
    stat_header_id   INT UNSIGNED NOT NULL,
    data_file_id     INT UNSIGNED NOT NULL,
    line_num         INT UNSIGNED,
    fcst_lead        INT,
    fcst_valid_beg   DATETIME,
    fcst_valid_end   DATETIME,
    fcst_init_beg    DATETIME,
    obs_lead         INT UNSIGNED,
    obs_valid_beg    DATETIME,
    obs_valid_end    DATETIME,
    alpha            DOUBLE,
    fcst_model       VARCHAR(40),
    ref_model        VARCHAR(40),
    n_init           INT UNSIGNED,
    n_term           INT UNSIGNED,
    v_vld            INT UNSIGNED,
    ss_index         DOUBLE DEFAULT -9999,

    CONSTRAINT line_data_ssidx_data_file_id_pk
        FOREIGN KEY (data_file_id)
            REFERENCES data_file (data_file_id),
    CONSTRAINT line_data_ssidx_stat_header_id_pk
        FOREIGN KEY (stat_header_id)
            REFERENCES stat_header (stat_header_id),
    INDEX stat_header_id_idx (stat_header_id)
) ENGINE = MyISAM
  CHARACTER SET = latin1|

ALTER TABLE line_data_orank CHANGE rank obs_rank INT|

DELIMITER ;
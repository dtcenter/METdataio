DELIMITER |


ALTER TABLE line_data_vcnt
    ADD COLUMN dir_me DOUBLE |
ALTER TABLE line_data_vcnt
    ADD COLUMN dir_me_bcl DOUBLE |
ALTER TABLE line_data_vcnt
    ADD COLUMN dir_me_bcu DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mae DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mae_bcl DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mae_bcu DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mse DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mse_bcl DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_mse_bcu DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dir_rmse DOUBLE |
ALTER TABLE line_data_val1l2 
    ADD COLUMN dir_rmse_bcl DOUBLE |
ALTER TABLE line_data_val1l2 
    ADD COLUMN dir_rmse_bcu DOUBLE |

ALTER TABLE line_data_val1l2
    ADD COLUMN dira_me DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dira_mae DOUBLE |
ALTER TABLE line_data_val1l2
    ADD COLUMN dira_mse DOUBLE |

ALTER TABLE line_data_vl1l2
    ADD COLUMN dir_me DOUBLE |
ALTER TABLE line_data_vl1l2
    ADD COLUMN dir_mae DOUBLE |
ALTER TABLE line_data_vl1l2
    ADD COLUMN dir_mse DOUBLE |

ALTER TABLE line_data_ecnt
    ADD COLUMN ign_conv_oerr DOUBLE |
ALTER TABLE  line_data_ecnt
    ADD COLUMN ign_corr_oerr DOUBLE |



DELIMITER ;

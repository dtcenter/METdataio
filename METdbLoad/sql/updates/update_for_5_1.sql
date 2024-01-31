DELIMITER |


ALTER TABLE line_data_tcmpr ADD COLUMN num_members   INT UNSIGNED|
ALTER TABLE line_data_tcmpr ADD COLUMN track_spread   DOUBLE |
ALTER TABLE line_data_tcmpr ADD COLUMN track_stdev   DOUBLE |
ALTER TABLE line_data_tcmpr ADD COLUMN mslp_stdev   DOUBLE |
ALTER TABLE line_data_tcmpr ADD COLUMN max_wind_stdev   DOUBLE |

DELIMITER ;
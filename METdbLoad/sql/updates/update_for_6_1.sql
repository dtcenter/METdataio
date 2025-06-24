DELIMITER |


ALTER TABLE line_data_tcmpr RENAME COLUMN line_number TO line_num |
ALTER TABLE line_data_tcmpr MODIFY level VARCHAR(10) |
ALTER TABLE line_data_tcmpr MODIFY watch_warn VARCHAR(10) |


DELIMITER ;
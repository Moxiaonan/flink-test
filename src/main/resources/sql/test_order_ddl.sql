CREATE TABLE `test_order_tab` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `order_no` varchar(100) NOT NULL,
  `order_status` tinyint(4) NOT NULL DEFAULT '0',
  `ctime` int(11) NOT NULL,
  `mtime` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_order_no` (`order_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
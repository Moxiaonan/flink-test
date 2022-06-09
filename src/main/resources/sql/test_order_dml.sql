-- 生成订单
INSERT INTO test_order_tab
(order_no, order_status, ctime, mtime)
VALUES(CONCAT('OD' ,LPAD(UNIX_TIMESTAMP() ,10,'0')), 0, UNIX_TIMESTAMP(), UNIX_TIMESTAMP());

-- 修改状态
UPDATE test_order_tab
SET order_status=2, mtime=UNIX_TIMESTAMP()
ORDER BY ID DESC LIMIT 1
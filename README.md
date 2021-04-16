# gravity-adapter-mysql

Gravity adapter for MySQL

---

### Check binlog is enabled
``` bash
mysql> show variables like 'log_bin';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| log_bin       | ON    |
+---------------+-------+
1 row in set (0.00 sec)

mysql> show variables like 'binlog_format';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| binlog_format | ROW   |
+---------------+-------+
1 row in set (0.00 sec)

```

### Enable binlog MySQL 8.0
``` bash
vim /etc/mysql/mysql.conf.d/mysqld.cnf

[mysqld]
...
log-bin=/var/lib/mysql/binlog
binlog-format=row

```

### Enable binlog MySQL 5.7
``` bash
vim /etc/mysql/mysql.conf.d/mysqld.cnf

[mysqld]
...
server-id=1
log-bin=/var/lib/mysql/mysql-bin.log
binlog-format=row
max_allowed_packet=100M
```

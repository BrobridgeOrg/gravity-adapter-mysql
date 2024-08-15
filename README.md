# gravity-adapter-mysql

Gravity adapter for MySQL

---

## config.toml 說明
##### configs/config.toml example
```
[gravity]
domain = "default"
host = "192.168.8.227"
port = 32803
pingInterval = 10
maxPingsOutstanding = 3
maxReconnects = -1
accessToken = ""

[source]
config = "./settings/sources.json"

[store]
enabled = true
path = "./statestore"
```

|參數|說明|
|---|---|
|gravity.domain| 設定gravity domain |
|gravity.host | 設定 gravity 的 nats ip |
|gravity.port | 設定 gravity 的 nats port |
|gravity.pingInterval | 設定 gravity 的 pingInterval |
|gravity.maxPingsOutstanding | 設定 gravity 的 maxPingOutstanding |
|gravity.maxReconnects | 設定 gravity 的 maxReconnects |
|gravity.accessToken | 設定 gravity 的 accessToken (for auth) |
|source.config |設定 Adapter 的 來源設定檔位置 |
|store.enabled |是否掛載 presistent volume (記錄狀態) |
|store.path | 設定 presistent volume 掛載點 (記錄狀態) |


> **INFO**
>
 config.toml 設定可由環境變數帶入，其環境變數命名如下：
 **GRAVITY\_ADAPTER\_MYSQL** + \_ + **[section]** + \_ + **key**
 其中所有英文字母皆為大寫並使用_做連接。
>
 YAML 由環境變數帶入 gravity.host 範例:
>
```
env:
- name: GRAVITY_ADAPTER_MYSQL_GRAVITY_HOST
  value: 192.168.8.227
```

## settings.json 說明
##### settings/sources.json example
```
{
	"sources": {
		"mysql_example": {
			"disabled": false,
			"host": "192.168.8.227",
			"port": 3306,
			"username": "root",
			"password": "1qaz@WSXROOT",
			"dbname": "gravity",
			"initialLoad": false,
			"tables": {
				"accounts":{
					"event": {
						"snapshot": "accountInitialized",
						"create": "accountCreated",
						"update": "accountUpdated",
						"delete": "accountDeleted"
					}
				}
			}
		}
	}
}
```

|參數|說明 |
|---|---|
| sources.SOURCE_NAME.disabled | 是否停用這個source |
| sources.SOURCE_NAME.host |設定postgresql server ip |
| sources.SOURCE_NAME.port |設定postgresql server port |
| sources.SOURCE_NAME.username |設定 postgresql 登入帳號 |
| sources.SOURCE_NAME.password |設定 postgresql 登入密碼 |
| sources.SOURCE_NAME.dbname | 設定 postgresql database name |
| sources.SOURCE_NAME.initialLoad |  是否同步既有 record （在初始化同步時禁止對該資料表進行操作） |
| sources.SOURCE_NAME.tables.TABLE\_NAME | 設定要捕獲事件的 table 名稱|
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.snapshot | 設定 initialLoad event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.create | 設定 create event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.update | 設定 update event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.delete | 設定 delete event name |

> **INFO**
>
 資料庫的連線密碼可由環境變數帶入(需要使用工具做 AES 加密)，其環境變數如下：
  **[SOURCE_NAME] + \_ + PASSWORD**
>
>
 settings.json 設定可由環境變數帶入，其環境變數如下：
 **GRAVITY\_ADAPTER\_MYSQL\_SOURCE\_SETTINGS**
>
 YAML 由環境變數帶入範例:
>
```
env:
- name: GRAVITY_ADAPTER_MYSQL_SOURCE_SETTINGS
      value: |
        {
            "sources": {
                "mysql_example": {
                    "disabled": false,
                    "host": "192.168.8.227",
                    "port": 3306,
                    "username": "root",
                    "password": "1qaz@WSXROOT",
                    "dbname": "gravity",
                    "initialLoad": false,
                    "tables": {
                        "accounts":{
                            "event": {
                                "snapshot": "accountInitialized",
                                "create": "accountCreated",
                                "update": "accountUpdated",
                                "delete": "accountDeleted"
                            }
                        }
                    }
                }
            }
        }
```

---

> **補充**
>
> 設定 Log 呈現的 Level 可由環境變數帶入:
其設定可使用 **debug**, **info**, **error**
>
```
env:
- name: GRAVITY_DEBUG
  value: debug
```

---

## Build
```
podman buildx build --platform linux/amd64 --build-arg="AES_KEY=**********" -t docker.io/brobridgehub/gravity-adapter-mysql:v3.0.0 -f build/docker/Dockerfile .
```

---

## Enable Database CDC

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

---

## License

Licensed under the MIT License

## Authors

Copyright(c) 2020 Fred Chien <<fred@brobridge.com>>  
Copyright(c) 2020 Jhe Sue <<jhe@brobridge.com>>

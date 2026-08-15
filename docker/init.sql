-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE DATABASE IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.events;

CREATE TABLE analytics.events
(
    id BIGINT NOT NULL,
    kind VARCHAR(32) NULL,
    score INT NOT NULL,
    amount DECIMALV3(18, 2) NULL,
    event_date DATE NOT NULL,
    event_ts DATETIMEV2(6) NOT NULL,
    active BOOLEAN NULL,
    payload STRING NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO analytics.events VALUES
    (1, 'alpha', 5, 10.25, '2026-01-03', '2026-01-03 01:02:03.123456', true, 'one'),
    (2, 'beta', 15, NULL, '2026-01-10', '2026-01-10 02:03:04.000001', false, 'two'),
    (3, 'alpha', 25, -3.50, '2026-02-02', '2026-02-02 03:04:05.999999', true, 'three'),
    (4, NULL, 35, 0.00, '2026-02-14', '2026-02-14 04:05:06.000000', false, 'four'),
    (5, 'gamma', 45, 999999.99, '2026-03-01', '2026-03-01 05:06:07.100000', true, 'five'),
    (6, 'beta', 55, 1.01, '2026-03-09', '2026-03-09 06:07:08.200000', false, 'six'),
    (7, 'alpha', 65, NULL, '2026-03-20', '2026-03-20 07:08:09.300000', true, 'seven'),
    (8, 'delta', 75, 42.42, '2026-03-31', '2026-03-31 08:09:10.400000', false, 'eight');

DROP TABLE IF EXISTS analytics.empty_events;

CREATE TABLE analytics.empty_events LIKE analytics.events;

DROP TABLE IF EXISTS analytics.wide_events;

CREATE TABLE analytics.wide_events
(
    id BIGINT NOT NULL,
    payload STRING NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO analytics.wide_events VALUES
    (1, REPEAT('x', 4096)),
    (2, REPEAT('x', 4096)),
    (3, REPEAT('x', 4096)),
    (4, REPEAT('x', 4096)),
    (5, REPEAT('x', 4096)),
    (6, REPEAT('x', 4096)),
    (7, REPEAT('x', 4096)),
    (8, REPEAT('x', 4096));

DROP TABLE IF EXISTS analytics.type_matrix;

CREATE TABLE analytics.type_matrix
(
    id BIGINT NOT NULL,
    boolean_value BOOLEAN NULL,
    tinyint_value TINYINT NOT NULL,
    smallint_value SMALLINT NOT NULL,
    int_value INT NOT NULL,
    bigint_value BIGINT NOT NULL,
    float_value FLOAT NOT NULL,
    double_value DOUBLE NOT NULL,
    decimal_value DECIMALV3(18, 2) NOT NULL,
    char_value CHAR(4) NOT NULL,
    varchar_value VARCHAR(32) NOT NULL,
    string_value STRING NOT NULL,
    json_value JSON NOT NULL,
    date_value DATE NOT NULL,
    datetime_value DATETIMEV2(6) NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO analytics.type_matrix VALUES
    (
        1, true, -8, -1600, -320000, -6400000000,
        1.25, -2.5, 12345.67,
        'xy', 'alpha', 'payload', '{"key": 1}', '2026-04-01',
        '2026-04-01 01:02:03.123456'
    );

DROP TABLE IF EXISTS analytics.unsupported_types;

CREATE TABLE analytics.unsupported_types
(
    id INT NOT NULL,
    largeint_value LARGEINT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE USER IF NOT EXISTS 'daft_reader' IDENTIFIED BY 'reader-password';
GRANT SELECT_PRIV ON analytics.events TO 'daft_reader';

CREATE USER IF NOT EXISTS 'daft_no_access' IDENTIFIED BY 'no-access-password';
GRANT LOAD_PRIV ON analytics.events TO 'daft_no_access';

DROP TABLE IF EXISTS analytics.write_events;

CREATE TABLE analytics.write_events
(
    id BIGINT NOT NULL,
    kind VARCHAR(32) NULL,
    score INT NOT NULL,
    payload STRING NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS analytics.unique_events;

CREATE TABLE analytics.unique_events
(
    id BIGINT NOT NULL,
    kind VARCHAR(32) NULL,
    score INT NOT NULL,
    payload STRING NOT NULL
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS analytics.partial_events;

CREATE TABLE analytics.partial_events
(
    id BIGINT NOT NULL,
    kind VARCHAR(32) NULL,
    score INT NULL,
    payload STRING NULL
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

DROP TABLE IF EXISTS analytics.aggregate_events;

CREATE TABLE analytics.aggregate_events
(
    id BIGINT NOT NULL,
    score BIGINT SUM,
    payload STRING REPLACE
)
AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

GRANT SELECT_PRIV, LOAD_PRIV ON analytics.write_events TO 'daft_reader';
GRANT SELECT_PRIV, LOAD_PRIV ON analytics.unique_events TO 'daft_reader';
GRANT SELECT_PRIV, LOAD_PRIV ON analytics.partial_events TO 'daft_reader';
GRANT SELECT_PRIV, LOAD_PRIV ON analytics.aggregate_events TO 'daft_reader';

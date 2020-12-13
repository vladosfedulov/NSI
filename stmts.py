
STMT_REFBOOK = """
SELECT NSI_REF_BOOKS.ID, NSI_REF_BOOK_VERSION.version
FROM NSI_REF_BOOKS
    INNER JOIN NSI_REF_BOOK_VERSION ON NSI_REF_BOOKS.ID = NSI_REF_BOOK_VERSION.master_id
    WHERE NSI_REF_BOOKS.ID = {ID};
    """

STMT_INSERT_VERSION = """
INSERT INTO NSI_REF_BOOK_VERSION (master_id, version, createDateTime)
VALUES ({ID}, '{version}', '{create_date_time}');
"""

STMT_INSERT_REFBOOK = """
INSERT INTO NSI_REF_BOOKS (ID, code, name, OID)
VALUES ({ID}, '{code}', '{name}', '{OID}');
"""

STMT_CREATE_REFBOOK = """
CREATE TABLE IF NOT EXISTS `{table_name}` (
id               int auto_increment primary key,
code             text                  not null comment 'Код',
name             text                  not null comment 'Наименовение',
version          text                  not null comment 'Версия',
ref_id           int                   not null comment 'NSI_REF_BOOKS.ID'
)
comment '{ref_book_name}';
"""

STMT_ALTER_TABLE = """
ALTER TABLE `{table_name}`
ADD COLUMN IF NOT EXISTS `{col_name}` {col_type} null comment '{comment}';
"""

STMT_INSERT_VALUES = """
INSERT INTO `{table_name}` ({cols})
VALUES ({values});
"""
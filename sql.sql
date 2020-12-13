

CREATE TABLE NSI_REF_BOOKS (
 id               int primary key,
 code             text                  not null comment 'Код',
 name             text                  not null comment 'Название справочника',
 OID              text                  not null comment 'OID справочника'
)
comment 'Список справочников НСИ';

CREATE TABLE NSI_REF_BOOK_VERSION (
 id               int auto_increment primary key,
 master_id        int                  not null comment 'Справочник {NSI_REF_BOOKS}',
 version          text                 not null comment 'Версия',
 createDateTime   datetime             not null comment 'Дата создания версии спраочника'
)
comment 'Список версий справочников НСИ';

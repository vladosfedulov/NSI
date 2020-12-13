# -*- coding: utf-8 -*-

import logging

import os
from suds.client import Client
from datetime import datetime

from appendix.nsi.database import select_data, update_data

from appendix.nsi.stmts import STMT_REFBOOK, STMT_INSERT_VERSION, STMT_INSERT_REFBOOK, STMT_CREATE_REFBOOK, \
    STMT_ALTER_TABLE, STMT_INSERT_VALUES


class Service(object):

    map_db_types = {
        'NUMBER': 'INT',
        'VARCHAR2': "TEXT",
        'DATE': 'DATE',
        'DATETIME': "DATETIME",
        'BOOLEAN': 'TINYINT'
    }

    map_db_cols = {
        "S_NAME": "name",
        "NAME_RUS": "name",
        "ID": "code",
        "NAME": "name",
        "DESCR": "name",
        "CODE": "code",
        "STATUS": "name",
        "S_DESCRIPTION": "name",
        "id": "code",
    }

    ref_ignore_list = [
        "1.2.643.5.1.13.13.11.1519",
        "1.2.643.5.1.13.13.99.2.114",
        "1.2.643.5.1.13.13.11.1367"
    ]

    def __init__(self):
        self.serv = Client("https://nsi.rosminzdrav.ru/wsdl/SOAP-server.v2.php?wsdl")
        self.serv.options.cache.clear()
        self.token = u''  # enter your token here

    def get_ref_book_list(self):
        request_s = self.serv.service.getRefbookList(self.token)
        fld = [self.convert_items_to_dict(it) for it in request_s.item]
        return fld

    def get_ref_versions(self, ref_oid):
        request_s = self.serv.service.getVersionList(self.token, ref_oid)
        versions = [self.convert_items_to_dict(it) for it in request_s.item]
        return versions

    def convert_items_to_dict(self, items):
        return dict([(filed.key, filed.value) for filed in items.children.item if hasattr(filed, 'value')])

    def get_ref_book_structure(self, ref_oid):
        request_s = self.serv.service.getRefbookStructure(self.token, ref_oid)
        ref_structure = [self.convert_items_to_dict(it) for it in request_s.item]
        return ref_structure

    def get_ref_book_parts(self, ref_oid, version):
        request_s = self.serv.service.getRefbookParts(self.token, ref_oid, version)
        val = request_s.item[0].value
        return int(val)

    def get_ref_book_part(self, ref_oid, version, part):
        request_s = self.serv.service.getRefbookPartial(self.token, ref_oid, version, part)
        ref_parts = [self.convert_items_to_dict(it) for it in request_s.item]
        return ref_parts

    def store_ref_records(self, ref_oid, version, ref_id):

        parts = self.get_ref_book_parts(ref_oid, version)
        for part_num in range(1, parts + 1):
            ref_part = self.get_ref_book_part(ref_oid, version, str(part_num))
            self.insert_values_into_ref(ref_oid, version, ref_part, ref_id)

    def to_variant(self, value, type):

        type = str(type.lower())

        if type == 'int':
            result = str(value)
        elif type == 'text':
            result = "\'{v}\'".format(v=str(value).replace("'", '\'').replace('"', '\"'))
        elif type == 'datetime':
            result = datetime.strptime(value, '%d.%m.%Y %H:%M')
            result = "\'{v}\'".format(v=str(result.strftime("%Y-%m-%d %H:%M:%S")))
        elif type == 'date':
            result = datetime.strptime(value, '%d.%m.%Y')
            result = "\'{v}\'".format(v=str(result.strftime("%Y-%m-%d")))
        elif type == 'tinyint':
            result = '1' if value == 'TRUE' else '0'
        else:
            result = value
        return result

    def insert_values_into_ref(self, ref_oid, version, values, ref_id):
        for value in values:
            keys_to_insert = list()
            val_to_insert = list()
            ref_book_cols = dict([(str(self.map_db_cols.get(ref_col.get('name'), ref_col.get('name'))), str(self.map_db_types.get(ref_col.get('type'), ref_col.get('type'))))
                                  for ref_col in self.get_ref_book_structure(ref_oid=ref_oid)])
            for key, val in value.items():
                if self.map_db_cols.get(key, key) in ref_book_cols.keys() and str(val) != 'None' and val and self.map_db_cols.get(key, key) not in keys_to_insert:
                    keys_to_insert.append("`" + self.map_db_cols.get(key, key) + "`")
                    val_to_insert.append(self.to_variant(val, ref_book_cols.get((self.map_db_cols.get(key, key)))))

            if keys_to_insert and val_to_insert:
                keys_to_insert.append('version')
                keys_to_insert.append('ref_id')

                val_to_insert.append(self.to_variant(version, 'text'))
                val_to_insert.append(self.to_variant(ref_id, 'int'))

                id, err = update_data(STMT_INSERT_VALUES.format(
                    table_name=ref_oid,
                    cols=', '.join(keys_to_insert),
                    values=', '.join(val_to_insert)
                ))

    def update_ref_book_cols(self, ref_oid):
        structure = self.get_ref_book_structure(ref_oid)

        for col in structure:
            title = col.get('title')
            type = col.get('type')
            name = col.get('name')
            update_data(stmt=STMT_ALTER_TABLE.format(table_name=ref_oid,
                                                     col_name=self.map_db_cols.get(name, name),
                                                     col_type=self.map_db_types.get(type),
                                                     comment=title))

    def create_new_ref_book(self, ref):

        ref_oid = ref.get('OID')
        ref_id = ref.get('ID')
        code = ref.get('S_CODE')
        name = ref.get('S_NAME')

        update_data(stmt=STMT_INSERT_REFBOOK.format(ID=ref_id, OID=ref_oid, code=code, name=name))
        update_data(stmt=STMT_CREATE_REFBOOK.format(table_name=ref_oid, ref_book_name=name))

        self.update_ref_book_cols(ref_oid)

    def check_and_store_version(self, ref_oid, ref_id, versions, ref_records):
        stored_versions = [rec.get('version') for rec in ref_records]
        if ref_oid in self.ref_ignore_list:
            versions = versions[-1:]
        for verion in versions:
            v = verion.get('S_VERSION')
            create_date_time = verion.get('V_DATE')
            create_date_time = datetime.strptime(create_date_time, '%d.%m.%Y %H:%M')
            create_date_time = create_date_time.strftime("%Y-%m-%d %H:%M:%S")
            if v not in stored_versions:
                update_data(stmt=STMT_INSERT_VERSION.format(ID=str(ref_id), version=v, create_date_time=create_date_time))
                self.store_ref_records(ref_oid, v, ref_id)

    def store_ref(self, ref, versions):
        ref_oid = ref.get('OID')
        ref_id = ref.get('ID')
        ref_records, err = select_data(stmt=STMT_REFBOOK.format(ID=ref_id))
        if not ref_records:
            self.create_new_ref_book(ref)

        self.check_and_store_version(ref_oid, ref_id, versions, ref_records)


def main():
    s = Service()
    fld = s.get_ref_book_list()

    for f in fld:
        ref_oid = f.get('OID')
        vers = s.get_ref_versions(ref_oid)
        s.store_ref(f, vers)


if __name__ == '__main__':
    main()

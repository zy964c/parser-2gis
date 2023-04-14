from __future__ import annotations

import logging
from typing import Any


from pymongo.errors import DuplicateKeyError

from ...logger import logger
from .file_writer import FileWriter
from pymongo import MongoClient


class MongoDbWriter(FileWriter):
    """Writer to JSON file."""
    def __enter__(self) -> MongoDbWriter:
        super().__enter__()
        self._wrote_count = 0
        self.items: dict = {}
        self.client = MongoClient('mongodb://db_writer:xYUNmNMpGGCY@10.10.9.104:27017/?tls=false&authMechanism=DEFAULT&authSource=2gis')
        self.db = self.client["2gis"]
        self.collection = getattr(self.db, self._options.mongo.collection)
        return self

    def __exit__(self, *exc_info) -> None:
        # try:
        #     inserted = self.collection.insert_many(list(self.items.values()))
        #     self._wrote_count += len(inserted.inserted_ids)
        #     logger.info(f'Inserted {inserted}. Number of inserted: {self._wrote_count}')
        # except Exception as e:
        #     logger.warning(f'{e}')
        # finally:
        self.client.close()
        super().__exit__(*exc_info)

    def _writedoc(self, catalog_doc: Any) -> None:
        """Write a `catalog_doc` into JSON document."""
        item = catalog_doc['result']['items'][0]
        item['_id'] = item['id'].split('_')[0]
        # if item['_id'] in self.items:
        #     return
        # self.items[item['_id']] = item
        if self._options.verbose:
            try:
                name = item['name_ex']['primary']
            except KeyError:
                name = '...'

            logger.info('Парсинг [%d] > %s', self._wrote_count + 1, name)
        try:
            shop_id = self.collection.insert_one(item).inserted_id
        except DuplicateKeyError as e:
            logger.warning(f'Duplicate found: {e}')
        else:
            self._wrote_count += 1
            logger.info(f'Inserted {shop_id =}. Number of inserted: {self._wrote_count}')

    def write(self, catalog_doc: Any) -> None:
        """Write Catalog Item API JSON document down to JSON file.

        Args:
            catalog_doc: Catalog Item API JSON document.
        """
        if not self._check_catalog_doc(catalog_doc):
            return

        self._writedoc(catalog_doc)

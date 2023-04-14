from __future__ import annotations

import json
from typing import Any

from ...logger import logger
from .file_writer import FileWriter
from pymongo import MongoClient


class MongoDbWriter(FileWriter):
    """Writer to JSON file."""
    def __enter__(self) -> MongoDbWriter:
        super().__enter__()
        self._wrote_count = 0
        self.client = MongoClient('mongodb://db_writer:xYUNmNMpGGCY@10.10.9.104:27017/?tls=false&authMechanism=DEFAULT&authSource=2gis')
        self.db = self.client["2gis"]
        self.collection = getattr(self.db, self._options.mongo.collection)
        return self

    def __exit__(self, *exc_info) -> None:
        self.client.close()
        super().__exit__(*exc_info)

    def _writedoc(self, catalog_doc: Any) -> None:
        """Write a `catalog_doc` into JSON document."""
        item = catalog_doc['result']['items'][0]
        item['_id'] = item['id'].split('_')[0]

        if self._options.verbose:
            try:
                name = item['name_ex']['primary']
            except KeyError:
                name = '...'

            logger.info('Парсинг [%d] > %s', self._wrote_count + 1, name)

        shop_id = self.collection.insert_one(item).inserted_id
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

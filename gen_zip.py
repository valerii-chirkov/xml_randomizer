from datetime import datetime
from zipfile import ZipFile

import os
import csv
import threading
import xml.etree.ElementTree as ET

from utils import get_objects, get_list_of_unique_str, get_value
from template import XML_TEMPLATE


class XMLFilesFactory:
    """ Class to generate 'quantity' amount of xml files
    e.g. XMLFilesFactory(quantity=100, path='~/')
    """

    def __init__(self, quantity: int = 100, path: str = None) -> None:
        self.quantity = quantity
        self.path = path if path else ''
        self.ids: list[str] = get_list_of_unique_str(amount=self.quantity)

    def __call__(self, *args, **kwargs) -> list[str]:
        return [self.get_file(path=self.path, _id=i, level=get_value()) for i in self.ids]

    @staticmethod
    def get_file(path: str, _id: str, level: int) -> str:
        objects = XML_TEMPLATE.format(_id, level, get_objects())
        filename = "".join([_id, str(level), '.xml'])
        file_path = os.path.join(path, filename)
        with open(file_path, "w") as ff:
            ff.write(objects)
        return file_path


class ZipsFactory:
    """ Class to generate 'zip_amount' of zip archives
    containing 'xml_per_zip' of xml files
    """

    def __init__(self, path: str = '', zip_amount: int = 50, xml_per_zip: int = 100) -> None:
        self.root: str = path if path else os.path.join(os.getcwd(), 'zips')
        self.check_root(self.root)
        self.zip_amount: int = zip_amount
        self.xml_factory: object = XMLFilesFactory(quantity=xml_per_zip, path=self.root)

    def __call__(self, *args, **kwargs) -> None:
        [self._get_zip() for _ in range(self.zip_amount)]

    def _get_zip_path(self) -> str:
        date_now = str(datetime.now())
        zip_name = "".join([date_now, '.zip'])
        os.chdir(self.root)
        return zip_name

    def _get_zip(self) -> None:
        with ZipFile(self._get_zip_path(), 'w') as zp:
            for file in self.xml_factory():
                zp.write(filename=file, arcname=os.path.basename(file))
                os.remove(file)

    @staticmethod
    def check_root(root: str) -> None:
        if not os.path.exists(root):
            os.mkdir(root)


class ProceedZipDir:
    """ Class to proceed zip files and files in them
    to make two csv files with 'levels' and 'objects' data
    """
    def __init__(self, path: str = None, workers: int = 5) -> None:
        self.path = path if path else os.getcwd()
        self.rlock = threading.RLock()
        self.workers = workers

    def __call__(self, *args, **kwargs) -> None:
        threads = []
        for zip_name in os.listdir(self.path):
            thread = threading.Thread(target=self._proceed_zip, args=(os.path.join(self.path, zip_name),))
            thread.start()
            threads.append(thread)
        [thread.join() for thread in threads]

    def _proceed_zip(self, path: str) -> None:
        with ZipFile(path) as zp:
            threads = []
            for f_chunk in self.get_chunk(zp.namelist(), self.workers):
                for filename in f_chunk:
                    thread = threading.Thread(target=self._proceed_file, args=(zp, filename))
                    thread.start()
                    threads.append(thread)
                [thread.join() for thread in threads]

    def _proceed_file(self, zp: ZipFile, filename: str) -> None:
        with zp.open(filename) as ff:
            root = ET.fromstring(ff.read().decode('utf-8'))
            _id, level, objects = self.get_attrs_xml(root)
            with self.rlock:
                self.write_csv_levels(_id, level)
            with self.rlock:
                self.write_csv_objects(_id, objects)

    @staticmethod
    def get_attrs_xml(root: ET) -> tuple[str, str, list[str]]:
        root_content: list = [i.attrib for i in root.iter()]
        _id: str = root_content[1].get("value")
        level: str = root_content[2].get("value")
        objects: list = [obj.get('name') for obj in root_content[4:]]
        return _id, level, objects

    @staticmethod
    def write_csv_levels(_id: str, level: str) -> None:
        with open('../levels.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow((_id, level))

    @staticmethod
    def write_csv_objects(_id: str, objects: list) -> None:
        with open('../objects.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)
            [writer.writerow((_id, obj)) for obj in objects]

    @staticmethod
    def get_chunk(files: list, workers: int):
        for i in range(0, len(files), workers):
            yield files[i:i + workers]


def speedtest(func):
    def wrapper(*args, **kwargs):
        import time
        t1 = time.time()
        func(*args, **kwargs)
        print(time.time()-t1)
    return wrapper


def set_init(path: str):
    import shutil
    shutil.rmtree(path)
    os.remove('../levels.csv')
    os.remove('../objects.csv')


@speedtest
def main(path: str, zips: int, xmls: int, workers: int) -> None:
    get_zips = ZipsFactory(path=path, zip_amount=zips, xml_per_zip=xmls)
    get_csv_reports = ProceedZipDir(path=path, workers=workers)
    get_zips()
    get_csv_reports()


if __name__ == '__main__':
    path = os.path.join(os.getcwd(), 'zips')
    main(path=path, zips=50, xmls=100, workers=5)
    set_init(path)

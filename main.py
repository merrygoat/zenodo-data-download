import hashlib
from pathlib import Path
import urllib.request
import zipfile
from typing import Union

import tqdm
import yaml


def main():
    data_locations = read_data_yaml("zenodo_URLs.yaml")["tensile_tests"]

    data_folder = Path('data')

    data_file = get_file_from_url(data_folder, **data_locations)

    unzip_file(data_file, data_folder)


def get_file_from_url(data_folder: Union[str, Path], url: str, name: str, md5: str = None) -> Path:
    """Download a file from a URL if it is not already present in `data_folder`. Return
    the local path to the downloaded file.
    """
    data_folder = Path(data_folder)
    if not data_folder.is_dir():
        data_folder.mkdir()
    dest_path = data_folder / name
    out_path = dest_path
    if not dest_path.exists():
        tqdm_description = f"Downloading file \"{name}\""
        with tqdm.tqdm(desc=tqdm_description, unit="bytes", unit_scale=True) as t:
            urllib.request.urlretrieve(url, dest_path, reporthook=tqdm_hook(t))

    if md5:
        if not validate_checksum(dest_path, md5):
            raise AssertionError('MD5 does not match: workflow file is corrupt. '
                                 'Delete workflow file and retry download.')
        else:
            print("MD5 validated. Download complete.")

    return out_path


def unzip_file(file_path: Union[str, Path], destination_path: Union[str, Path]):
    """Unzip the file at `file_path` to a folder at `destination_path`."""
    print('Unzipping...', end='')
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(destination_path)
    print('complete.')


def tqdm_hook(t: tqdm.tqdm):
    """Wraps tqdm progress bar to provide update hook method for `urllib.urlretrieve`."""
    last_b = [0]

    def update_to(b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            t.total = tsize
        t.update((b - last_b[0]) * bsize)
        last_b[0] = b

    return update_to


def validate_checksum(file_path: Path, valid_md5: str) -> bool:
    with open(file_path, 'rb') as binary_zip:
        md5_hash = hashlib.md5()
        md5_hash.update(binary_zip.read())
        digest = md5_hash.hexdigest()
        if digest == valid_md5:
            return True
        else:
            return False


def read_data_yaml(data_yaml_path: str) -> dict:
    path = Path(data_yaml_path)
    with open(path) as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


if __name__ == "__main__":
    main()

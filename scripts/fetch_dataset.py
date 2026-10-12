"""Download the UCI source, verify the project's exact SHA-256, then save it."""
import argparse
import hashlib
import io
from pathlib import Path
import urllib.request
import zipfile

URL='https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip'
EXPECTED='4259c9d7ece5dbee9ab8d53682baac68d791c864f0f64a52b4043cb3b90894b7'

def main():
    root=Path(__file__).resolve().parents[1]
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=root.parent/'household_power_consumption.txt')
    args=parser.parse_args()
    if args.output.exists():
        with args.output.open('rb') as source: current=hashlib.file_digest(source,'sha256').hexdigest()
        if current==EXPECTED: print(f'Dataset already verified: {args.output}'); return
        raise SystemExit('Existing output differs from the expected dataset. Choose another --output path.')
    with urllib.request.urlopen(URL,timeout=120) as response: archive=response.read()
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        names=[n for n in z.namelist() if Path(n).name=='household_power_consumption.txt']
        if len(names)!=1: raise SystemExit('Archive does not contain one expected dataset file.')
        data=z.read(names[0])
    if hashlib.sha256(data).hexdigest()!=EXPECTED: raise SystemExit('Dataset checksum differs; no output was written.')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('xb') as target: target.write(data)
    print(f'Downloaded and verified: {args.output}')

if __name__=='__main__': main()

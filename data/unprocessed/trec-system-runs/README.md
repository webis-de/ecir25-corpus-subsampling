# TREC Run Files

For the experiments, we need the run files for the TREC Web tracks on the ClueWeb09/ClueWeb12, for the Robust04 track, and for the deep learning tracks from 2019/2020.

The run files are expected to be in this directory, as they are password protected, please download them via the provided `trec-results-downloader.py` directory into this directory.

The `./trec-results-downloader.py --help` command provides shows the options:
```
usage: trec-results-downloader.py [-h] --password PASSWORD --user USER --seed SEED

Download Trec-System-Runs.

options:
  -h, --help           show this help message and exit
  --password PASSWORD  The password to access the protected area
  --user USER          The user to access the protected area
  --seed SEED          The seed url to start crawling runs from.
```

I.e., download all required data via:
```
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec28/deep.passages.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec29/deep.passages.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec23/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec22/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec21/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec20/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec19/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec18/web.adhoc.input.html
./trec-results-downloader.py  --user USER --password PASSWORD --seed https://trec.nist.gov/results/trec13/robust.predictions.html
```

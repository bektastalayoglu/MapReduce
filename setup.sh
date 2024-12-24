#!/bin/bash
python3 -m venv venv
source venv/bin/activate
pip install setuptools
pip install -r requirements.txt
python3 -m nltk.downloader stopwords  
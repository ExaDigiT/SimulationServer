#!/bin/bash
set -e

mkdir data

raps download --system lassen --dest ./data/lassen
mv ./data/lassen/Lassen-Supercomputer-Job-Dataset/* ./data/lassen
rm -rf ./data/lassen/Lassen-Supercomputer-Job-Dataset
python3 ./scripts/preprocess_lassen.py ./data/lassen

raps download --system marconi100 --dest ./data/marconi100

raps download --system fugaku --dest ./data/fugaku
python3 ./scripts/preprocess_fugaku.py ./data/fugaku

raps download --system adastraMI250 --dest ./data/adastraMI250

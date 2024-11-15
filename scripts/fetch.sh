set -e

mkdir data
cd data

# lassen
git clone https://github.com/LLNL/LAST/ lassen-repo
cd lassen-repo
git lfs pull
cd ..
mkdir lassen
mv lassen-repo/Lassen-Supercomputer-Job-Dataset/*.csv lassen
rm -rf lassen-repo
python3 ../scripts/preprocess_lassen.py lassen

# marconi
wget https://zenodo.org/api/records/10127767/files-archive -O marconi100.zip
unzip marconi100.zip -d marconi100
rm marconi100.zip

# fugaku
wget https://zenodo.org/api/records/11467483/files-archive -O fugaku.zip
unzip fugaku.zip -d fugaku 
rm fugaku/*.csv

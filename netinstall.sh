#!/usr/bin/env bash
REPO="https://github.com/rposudnevskiy"
PROJECT="xcpng-storage-libs"
BRANCH="master"

cd ~
wget -q "$REPO/$PROJECT/archive/$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip ~/$PROJECT-temp.zip -d ~
cd ~/$PROJECT-$BRANCH/install

sh ./$PROJECT.sh install $1

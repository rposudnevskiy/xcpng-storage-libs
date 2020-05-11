#!/usr/bin/env bash
REPO="https://github.com/rposudnevskiy"
PROJECT="xcpng-storage-libs"
BRANCH="master"

cd ~
wget -q "$REPO/$PROJECT/archive/$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip -qq ~/$PROJECT-temp.zip -d ~
cd ~/$PROJECT-$BRANCH

sh ./install/$PROJECT.sh install $1

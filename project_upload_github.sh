#!/usr/bin/env sh

cd ~/Pinterest-Data-Processing-Pipeline

git checkout main
git pull https://github.com/Simeon94/Pinterest-Data-Processing-Pipeline.git
git push origin main #push changes to my main branch

echo "enter name of new branch"
read branchName

git branch $branchName
git checkout $branchName

eval "$(conda shell.bash hook)"

#conda activate AiCore

#jupyter notebook

git add . #Add any changes to Simeon\aicore\Data-Engineering

git commit -m draft

git push origin $branchName #push chnages from the branch to origin;remote git-hub.
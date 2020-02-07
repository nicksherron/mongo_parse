#!/usr/bin/env bash

set -eou pipefail

version="$(git tag | sort --version-sort -r | head -1)"

last_tag_ref=$(git log --decorate | grep $version | cut -d' ' -f 2 | head -c 7)

last_commit_ref="$(git log -1 --oneline | cut -d' ' -f 1)"

commit=$(git rev-parse HEAD)
build_date=$( date '+%Y-%m-%d-%H:%M:%S')

echo $version $last_tag_ref $last_commit_ref

if [ ! $last_tag_ref == $last_commit_ref ]; then
	echo "last commit is not last tag"
	echo "last tag: $version"
fi

rm -rf dist.bk/
[ -d dist/ ] && mv dist/ dist.bk/

export GO111MODULE=on

xgo \
    -out="mongo_parse-${version}" \
    --targets="windows/*,darwin/amd64,linux/386,linux/amd64" \
    --dest=dist \
    -v -x \
    github.com/nicksherron/mongo_parse


sudo chown -R $USER: dist/


darwin_amd64=mongo_parse_${version}_darwin_amd64
linux_386=mongo_parse_${version}_linux_386
linux_amd64=mongo_parse_${version}_linux_amd64
windows_386=mongo_parse_${version}_windows_386
windows_amd64=mongo_parse_${version}_windows_amd64

mkdir dist/{$darwin_amd64,$linux_386,$linux_amd64,$windows_386,$windows_amd64}

pushd dist

mv mongo_parse-${version}-darwin-10.6-amd64 ${darwin_amd64}/mongo_parse \
  && tar -czvf ${darwin_amd64}.tar.gz ${darwin_amd64}

mv mongo_parse-${version}-linux-386 ${linux_386}/mongo_parse \
  && tar czvf ${linux_386}.tar.gz ${linux_386}

mv mongo_parse-${version}-linux-amd64 ${linux_amd64}/mongo_parse \
  && tar czvf ${linux_amd64}.tar.gz ${linux_amd64}

mv mongo_parse-${version}-windows-4.0-386.exe ${windows_386}/mongo_parse \
  && zip -r ${windows_386}.zip ${windows_386}

mv mongo_parse-${version}-windows-4.0-amd64.exe ${windows_amd64}/mongo_parse \
  && zip -r ${windows_amd64}.zip ${windows_amd64}

rm -rf {$darwin_amd64,$linux_386,$linux_amd64,$windows_386,$windows_amd64}

shasum -a 256 * > mongo_parse_${version}_checksums.txt

popd

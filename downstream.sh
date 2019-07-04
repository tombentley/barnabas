#!/bin/bash
DOWNSTREAM=downstream
set -e
set -x
rm -rf "${DOWNSTREAM}"
mkdir -p "${DOWNSTREAM}" "${DOWNSTREAM}/html/images"
cp -r documentation/common ${DOWNSTREAM}/
cp -r documentation/book ${DOWNSTREAM}/
cp -r documentation/images ${DOWNSTREAM}/
find ${DOWNSTREAM} -type f -name '*.adoc' -print0 | xargs -0 \
sed -Ei -e 's/\<a Kubernetes\>/an OpenShift/g' \
    -e 's/\<A Kubernetes\>/An OpenShift/g' \
    -e 's/\<Kubernetes\>/OpenShift/g' \
    -e 's/\<kubectl taint\>/oc adm taint/g' \
    -e 's/\<kubectl\>/oc/g'

RELEASE_VERSION=0.13
GITHUB_VERSION=master
cp -vrL ${DOWNSTREAM}/book/images ${DOWNSTREAM}/html/images
asciidoctor -v --failure-level WARN -t -dbook \
    -a ProductVersion=${RELEASE_VERSION} \
    -a GithubVersion=${GITHUB_VERSION} \
    ${DOWNSTREAM}/book/master.adoc \
    -o ${DOWNSTREAM}/html/index.html
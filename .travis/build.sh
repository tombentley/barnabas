#!/usr/bin/env bash
set -e

# The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ ${JAVA_MAJOR_VERSION} -gt 1 ] ; then
  export JAVA_VERSION=${JAVA_MAJOR_VERSION}
fi

if [ ${JAVA_MAJOR_VERSION} -eq 1 ] ; then
  # sme parts of the workflow should be done only one on the main build which is currently Java 8
  export MAIN_BUILD="TRUE"
fi

export PULL_REQUEST=${PULL_REQUEST:-true}
export BRANCH=${BRANCH:-master}
export TAG=${TAG:-latest}
export DOCKER_ORG=${DOCKER_ORG:-strimzici}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}
export DOCKER_TAG=$COMMIT

make docu_check
make spotbugs

make crd_install
make helm_install
make docker_build

if [ ! -e  documentation/book/appendix_crds.adoc ] ; then
  exit 1
fi

CHANGED_DERIVED=$(git diff --name-status -- install/ helm-charts/ documentation/book/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles)
if [ -n "$CHANGED_DERIVED" ] ; then
  echo "ERROR: Uncommitted changes in derived resources:"
  echo "$CHANGED_DERIVED"
  echo "Run the following to add up-to-date resources:"
  echo "  mvn clean verify -DskipTests -DskipITs \\"
  echo "    && git add install/ helm-charts/ documentation/book/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles"
  echo "    && git commit -s -m 'Update derived resources'"
  exit 1
fi

# Push to the real docker org
if [ "$PULL_REQUEST" != "false" ] || [[ "$TRAVIS_REPO_SLUG" == strimzi/* ]] ; then
    make docu_html
    make docu_htmlnoheader
    echo "Building pull request or forked repo - nothing to push"
elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ]; then
    make docu_html
    make docu_htmlnoheader
    echo "Not in master branch and not in release tag - nothing to push"
else
    if [ "${MAIN_BUILD}" = "TRUE" ] ; then
        echo "Login into Docker Hub ..."
        docker login -u $DOCKER_USER -p $DOCKER_PASS

        export DOCKER_ORG=strimzi
        export DOCKER_TAG=$TAG
        echo "Pushing to docker org $DOCKER_ORG"
        make docker_push
        if [ "$BRANCH" = "master" ]; then
            make docu_pushtowebsite
        fi
        make pushtonexus
    fi
fi

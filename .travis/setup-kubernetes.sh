#!/usr/bin/env bash
set -x

rm -rf ~/.kube

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/amd64/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/bin
}

function install_nsenter {
    # Pre-req for helm
    curl https://mirrors.edge.kernel.org/pub/linux/utils/util-linux/v${TEST_NSENTER_VERSION}/util-linux-${TEST_NSENTER_VERSION}.tar.gz | tar -zxf-
    cd util-linux-${TEST_NSENTER_VERSION}
    ./configure --without-ncurses
    make nsenter
    sudo cp nsenter /usr/bin
}

function install_helm {
    install_nsenter
    # Set `TEST_HELM_VERSION` to `latest` to get latest version
    HELM_INSTALL_DIR=/usr/bin
    curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > get_helm.sh
    chmod 700 get_helm.sh
    sudo ./get_helm.sh --version ${TEST_HELM_VERSION}
    helm init --client-only
}

function wait_for_minikube {
    i="0"

    while [ $i -lt 60 ]
    do
        # The role needs to be added because Minikube is not fully prepared for RBAC.
        # Without adding the cluster-admin rights to the default service account in kube-system
        # some components would be crashing (such as KubeDNS). This should have no impact on
        # RBAC for Strimzi during the system tests.
        kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
        if [ $? -ne 0 ]
        then
            sleep 1
        else
            return 0
        fi
        i=$[$i+1]
    done

    return 1
}

function label_node {

    if [ "$TEST_CLUSTER" = "minikube" ]; then
        echo $(kubectl get nodes)
        kubectl label node minikube rack-key=zone
    elif [ "$TEST_CLUSTER" = "minishift" ]; then
        oc label node minishift rack-key=zone
    elif [ "$TEST_CLUSTER" = "oc" ]; then
        oc label node localhost rack-key=zone
    fi
}

if [ "$TEST_CLUSTER" = "minikube" ]; then
    date +%F_%T
    install_kubectl
    date +%F_%T
    install_helm
    date +%F_%T
    if [ "${TEST_MINIKUBE_VERSION:-latest}" = "latest" ]; then
        TEST_MINIKUBE_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    else
        TEST_MINIKUBE_URL=https://github.com/kubernetes/minikube/releases/download/${TEST_MINIKUBE_VERSION}/minikube-linux-amd64
    fi
    curl -Lo minikube ${TEST_MINIKUBE_URL} && chmod +x minikube
    sudo cp minikube /usr/bin
    date +%F_%T
    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true
    
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

    export KUBECONFIG=$HOME/.kube/config
    sudo -E minikube start --vm-driver=none --kubernetes-version=v1.15.0 \
      --insecure-registry=localhost:5000 --extra-config=apiserver.authorization-mode=RBAC
    sudo chown -R travis: /home/travis/.minikube/
    sudo -E minikube addons enable default-storageclass

    wait_for_minikube

    if [ $? -ne 0 ]
    then
        echo "Minikube failed to start or RBAC could not be properly set up"
        exit 1
    fi
    date +%F_%T
elif [ "$TEST_CLUSTER" = "minishift" ]; then
    #install_kubectl
    MS_VERSION=1.13.1
    curl -Lo minishift.tgz https://github.com/minishift/minishift/releases/download/v$MS_VERSION/minishift-$MS_VERSION-linux-amd64.tgz && tar -xvf minishift.tgz --strip-components=1 minishift-$MS_VERSION-linux-amd64/minishift && rm minishift.tgz && chmod +x minishift
    sudo cp minishift /usr/bin

    #export MINIKUBE_WANTUPDATENOTIFICATION=false
    #export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINISHIFT_HOME=$HOME
    #export CHANGE_MINIKUBE_NONE_USER=true
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

    export KUBECONFIG=$HOME/.kube/config
    sudo -E minishift start
    sudo -E minishift addons enable default-storageclass
elif [ "$TEST_CLUSTER" = "oc" ]; then
    mkdir -p /tmp/openshift
    wget https://github.com/openshift/origin/releases/download/v3.7.0/openshift-origin-client-tools-v3.7.0-7ed6862-linux-64bit.tar.gz -O openshift.tar.gz
    tar xzf openshift.tar.gz -C /tmp/openshift --strip-components 1
    sudo cp /tmp/openshift/oc /usr/bin
else
    echo "Unsupported TEST_CLUSTER '$TEST_CLUSTER'"
    exit 1
fi

if [ "$TRAVIS" = false ]; then
    label_node
fi
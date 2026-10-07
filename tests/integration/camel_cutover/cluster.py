"""Isolated Camel K test cluster; never reads or changes default kubeconfig.

Run only during the reserved integration-test resource window. Teardown targets
only the exact cluster/registry/network names below, not other local clusters.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

NAME='flowbridge-camel-proof'
REGISTRY=NAME+'-registry'
ROOT=Path(__file__).resolve().parents[3]
STATE=ROOT/'state/camel-proof'
ENV={**os.environ,'DOCKER_CONFIG':str(ROOT.parent/'local-workers/state/docker'),'DOCKER_CONTEXT':'colima-dil','KIND_EXPERIMENTAL_DOCKER_NETWORK':NAME}


def run(args, **kwargs):
    return subprocess.run(args,env=ENV,text=True,check=True,**kwargs)


def kube(*args, **kwargs):
    return run(['kubectl','--kubeconfig',str(STATE/'kubeconfig'),*args],**kwargs)


def apply(value):
    return kube('apply','--server-side','-f','-',input=json.dumps(value))


def create():
    STATE.mkdir(parents=True,exist_ok=True,mode=0o700)
    run(['docker','network','create',NAME])
    run(['docker','run','-d','--name',REGISTRY,'--network',NAME,'--memory','128m','--cpus','0.5','--security-opt','no-new-privileges:true','registry:2'])
    config={'kind':'Cluster','apiVersion':'kind.x-k8s.io/v1alpha4','nodes':[{'role':'control-plane'}],'containerdConfigPatches':['[plugins."io.containerd.grpc.v1.cri".registry]\n  config_path = "/etc/containerd/certs.d"']}
    (STATE/'kind.json').write_text(json.dumps(config))
    run(['kind','create','cluster','--name',NAME,'--image','kindest/node:v1.33.4','--config',str(STATE/'kind.json'),'--kubeconfig',str(STATE/'kubeconfig'),'--wait','120s'])
    run(['docker','update','--memory','3g','--memory-swap','3g','--cpus','3',NAME+'-control-plane'])
    save=subprocess.Popen(['docker','image','save','--platform','linux/arm64','apache/camel-k:2.11.0'],env=ENV,stdout=subprocess.PIPE)
    subprocess.run(['docker','exec','-i',NAME+'-control-plane','ctr','-n','k8s.io','images','import','--platform','linux/arm64','-'],env=ENV,stdin=save.stdout,check=True)
    save.stdout.close()
    if save.wait():raise RuntimeError('operator_image_load_failed')
    install()


def install():
    registry_ip=json.loads(run(['docker','inspect',REGISTRY],capture_output=True).stdout)[0]['NetworkSettings']['Networks'][NAME]['IPAddress']
    registry=registry_ip+':5000'
    host_path='/etc/containerd/certs.d/'+registry
    run(['docker','exec',NAME+'-control-plane','mkdir','-p',host_path])
    hosts='[host."http://'+registry+'"]\n  capabilities = ["pull", "resolve", "push"]\n'
    run(['docker','exec','-i',NAME+'-control-plane','tee',host_path+'/hosts.toml'],input=hosts,capture_output=True)
    install=run(['kubectl','kustomize','https://github.com/apache/camel-k/install/overlays/own-namespace?ref=v2.11.0'],capture_output=True).stdout
    kube('apply','--server-side','-f','-',input=install)
    kube('set','resources','deployment/camel-k-operator','--requests=cpu=100m,memory=128Mi','--limits=cpu=1,memory=512Mi')
    kube('rollout','status','deployment/camel-k-operator','--timeout=180s')
    apply({'apiVersion':'camel.apache.org/v1','kind':'IntegrationPlatform','metadata':{'name':'camel-k'},'spec':{'build':{'registry':{'address':registry,'insecure':True}}}})
    (STATE/'registry.json').write_text(json.dumps({'registry':registry,'cluster':NAME}))
    print(json.dumps({'cluster':NAME,'kubeconfig':str(STATE/'kubeconfig'),'registry':registry}))


def delete():
    run(['kind','delete','cluster','--name',NAME,'--kubeconfig',str(STATE/'kubeconfig')])
    run(['docker','rm','-f',REGISTRY])
    run(['docker','network','rm',NAME])


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['create','install','delete']);args=parser.parse_args()
    {'create':create,'install':install,'delete':delete}[args.action]()

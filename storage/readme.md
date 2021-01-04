# 0-fs csi implementation

## TODO

### now

- [ ] sidecar containers
- [ ] RBAC rules
- [ ] Deploymentscripts and setup
- [ ] integrate zerofs-fuse
- [ ] docs
- [ ] check if a configmap is a better way to configure the csi plugin: [cephfs](https://github.com/ceph/ceph-csi/blob/master/examples/README.md#creating-csi-configuration)
- [ ] Integrate docker build in makefile
- [ ] publish to an image repository 

## libs that might be useful

- "k8s.io/utils/exec"
- "kubernetes/mount-utils"

### later

- volume extension online
- snapshots
- clone

## references

- [Container Storage Interface (CSI)](https://github.com/container-storage-interface/spec/blob/master/spec.md)
- [CSI Volume Plugins in Kubernetes Design Doc](https://github.com/kubernetes/community/blob/master/contributors/design-proposals/storage/container-storage-interface.md#csi-volume-plugins-in-kubernetes-design-doc)
- [Developing CSI Driver for Kubernetes](https://kubernetes-csi.github.io/docs/developing.html#developing-csi-driver-for-kubernetes)


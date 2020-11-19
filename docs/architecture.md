- VDC website is a 3bot with vdc package installed
- we have a kuberentes cluster for mgmt purposes where we deploy the 3bot per each vdc
- VDC is (kubernetes + minio) in the same network
  - cluster is exposed using edge network node from a farm providing public IPs 
  - monitoring runs in kubernetes
  - minio runs outside of the kubernetes cluster (there's no ip6 support and minio needs to reach to zdbs on public ipv6)
  - we run a reverse proxy (in the cluster) to reach out to minio
    - this reverse proxy is exposed on the ingress
  
 
- we deploy a 3bot that can
  - extend resources of the vdc when needed
  - scale out the zdbs once reaching a specific threshold maybe 70% 

![vdc arch for a single user user A](./img/arch.png)

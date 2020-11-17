# Public IPv4
## Expected Procedure
- User create an IP reservation using (any) node from the farm. The user creates the reservation with a free IP from the farmer pool
- The IP reservation will be fulfilled from the farm. If the IP is already taken, the reservation will fail
- Once the reservation is fulfilled, the user can use this reservation with his (next) K8S reservation
- The user creates a new k8s reservation with an extra config (public-ip) that points to the public-ip reservation id. the k8s reservation does not have to use the same node-id for the public-ip, since that one is only used to link to the farm
- k8s is deployed.

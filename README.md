# Configuration Notes

In order to run elasticsearch in development mode,
a couple of operating system limits need to be increased

- Run the following command as root to increase the limit on mmap counts: 
  - `sysctl -w vm.max_map_count=262144`
- Add the following line to `/etc/security/limits.conf` to increase the max number of file descriptors:
  - `user         -       nofile          65535`

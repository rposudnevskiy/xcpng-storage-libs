datacenter = "dc1"
bind_addr = "{{ GetInterfaceIP \"xenbr0\" }}"
client_addr = "{{ GetInterfaceIP \"xenbr0\" }} 127.0.0.1"
data_dir = "/opt/consul"

performance {
  raft_multiplier = 1
}
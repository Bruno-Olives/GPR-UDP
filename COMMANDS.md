listening to TCP:
`python3 tcp_chat.py --local-port 9002 --remote-host 192.168.1.204 --remote-port 9001`

listening to UDP:
`python3 reliable_udp_chat.py --local-port 9002 --remote-host 192.168.1.204 --remote-port 9001`

add delay to raspberry:
`sudo tc qdisc replace dev eth0 root netem loss 5% delay 50ms 10ms`

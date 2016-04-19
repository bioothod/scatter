# scatter
Distributed messaging framework

Scatter allows to easily write p2p, client-server and server-server messaging systems.
Servers are organized into DHT-like ring with dynamic route table, new servers take load
from older ones, clients automatically detect new servers and reconnect.
Client joins broadcast groups on servers similar to how multicast groups are formed.
Servers are stateless, they only maintain global route table and forward requests to
servers which have to handle given request according to current route table snapshot.

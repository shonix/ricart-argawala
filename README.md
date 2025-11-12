# assignment 4: our implementation of ricart agrawala distributed mutual exclusionn

we have implemented the ricart agrawala algorithm for distributed mutual exclusion by using grpc and go.

So we implement distributed mutual exclusion so that several nodes can share a cs without any conflicts, and we use lamport timestampts for ordering the requests.

## build

you of course need go installed, and the n you do:
bash
cd client
go build -o client.exe

Then you need at least 3 nodes, so you open up 3 terminals and then run the following in the specific terminal:

Terminal 1:
bash
cd client
./client.exe -id node1 -addr :50052

Terminal 2:
bash
cd client
./client.exe -id node2 -addr :50053

Terminal3:
bash
cd client
./client.exe -id node3 -addr :50054

And then the nodes will connect to each other and start requesteing access to the cs. You can see the logs to see the algorithm "performing"/working.

## proto regeneration

if you modify grpc/proto.proto then you can regenerate the code with:
bash
protoc --go_out=. --go-grpc_out=. grpc/proto.proto
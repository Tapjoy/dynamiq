package app

import (
	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/memberlist"
	"sort"
	"strconv"
)

func InitMemberList(name string, port int, seedServers []string, seedPort int) (*memberlist.Memberlist, int, error) {
	conf := memberlist.DefaultLANConfig()
	conf.Name = name
	conf.BindPort = port

	list, err := memberlist.Create(conf)

	if err != nil {
		logrus.Fatal(err)
	}

	myName := name + ":" + strconv.Itoa(seedPort)
	// TODO Possibly examine # of nodes joined, if under a threshold... take action?
	prioritizedServers := prioritizeSeedServers(myName, seedServers)
	nodesJoined, err := list.Join(prioritizedServers)

	if err != nil {
		logrus.Error(err)
	}

	for _, member := range list.Members() {
		logrus.Printf("Member %s %s\n", member.Name, member.Addr)
	}

	return list, nodesJoined, err
}

func prioritizeSeedServers(name string, seedServers []string) []string {
	// P-list will be the current list minus our node
	if len(seedServers) == 0 {
		logrus.Fatal("The list of seedservers was empty")
	}

	if len(seedServers) == 1 {
		if seedServers[0] == name {
			logrus.Fatal("The list of seedservers only contained a single entry, which was the current node.")
		} else {
			// If the list is only one long, and doesn't contain the current node, then we're fine as-is
			return seedServers
		}
	}

	// Sort them, so we have a consistent ordering
	sort.Strings(seedServers)

	myPos := 0
	// Find our pos, this will matter later
	// There is no great way to do this in golang, you need to iterate :(
	for i, elem := range seedServers {
		if elem == name {
			myPos = i
		}
	}

	// Split the array on our position to get a pre- and post- set of slices
	preSlice := seedServers[0:myPos]
	postSlice := seedServers[myPos+1 : len(seedServers)]

	// Return the post slice (after our pos) followed by the pre slice (prior to our pos)
	// This removes us from the array, and puts the elements immediately following us ahead of the ones
	// that used to be infront of us. This way, each node always tries to hit the next node
	// instead of all of them trying the same node, or a random shuffle which could be the same node
	return append(postSlice, preSlice...)
}

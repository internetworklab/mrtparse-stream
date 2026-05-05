package model

import (
	"net"
)

func mustParseCIDR(s string) net.IPNet {
	_, ipNet, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return *ipNet
}

func GetExampleMRTEntries() []*MRTEntry {
	return []*MRTEntry{
		{
			Prefix:              mustParseCIDR("192.168.1.0/24"),
			Peer:                net.ParseIP("10.0.0.1"),
			PeerAS:              64512,
			ASPath:              []uint32{64512, 64513, 64514, 64515, 64516, 64517, 64518},
			Communities:         []uint32{0x01020304, 0x05060708},
			LargeCommunities:    [][3]uint32{{64512, 1, 2}, {64513, 3, 4}},
			ExtendedCommunities: []uint64{0x0102030405060708},
			NextHop:             net.ParseIP("172.16.0.1"),
		},
		{
			Prefix:              mustParseCIDR("10.0.0.0/8"),
			Peer:                net.ParseIP("10.0.0.2"),
			PeerAS:              64514,
			ASPath:              []uint32{64514, 64515, 64516, 64517, 64518, 64519, 64520, 64521},
			Communities:         []uint32{0x0A0B0C0D},
			LargeCommunities:    [][3]uint32{{64514, 100, 200}},
			ExtendedCommunities: []uint64{0xAABBCCDDEEFF0011, 0x1122334455667788},
			NextHop:             net.ParseIP("172.16.0.2"),
		},
		{
			Prefix:              mustParseCIDR("172.16.5.0/24"),
			Peer:                net.ParseIP("10.0.0.3"),
			PeerAS:              64517,
			ASPath:              []uint32{64517, 64518, 64519, 64520, 64521, 64522},
			Communities:         []uint32{},
			LargeCommunities:    [][3]uint32{},
			ExtendedCommunities: []uint64{},
			NextHop:             net.ParseIP("172.16.0.3"),
		},
		{
			Prefix:              mustParseCIDR("203.0.113.0/24"),
			Peer:                net.ParseIP("10.0.0.4"),
			PeerAS:              64523,
			ASPath:              []uint32{64523, 64524, 64525, 64526, 64527, 64528, 64529},
			Communities:         []uint32{0xDEADBEEF, 0xCAFEBABE},
			LargeCommunities:    [][3]uint32{{64523, 10, 20}, {64524, 30, 40}, {64525, 50, 60}},
			ExtendedCommunities: []uint64{0x1234567890ABCDEF},
			NextHop:             net.ParseIP("172.16.0.4"),
		},
		{
			Prefix:              mustParseCIDR("198.51.100.0/24"),
			Peer:                net.ParseIP("10.0.0.5"),
			PeerAS:              64530,
			ASPath:              []uint32{64530, 64531, 64532, 64533, 64534, 64535, 64536, 64537},
			Communities:         []uint32{0x00110022},
			LargeCommunities:    [][3]uint32{{64530, 99, 88}},
			ExtendedCommunities: []uint64{0xFEDCBA0987654321, 0x1111111122222222},
			NextHop:             net.ParseIP("172.16.0.5"),
		},
		{
			Prefix:              mustParseCIDR("100.64.0.0/10"),
			Peer:                net.ParseIP("10.0.0.6"),
			PeerAS:              64538,
			ASPath:              []uint32{64538, 64539, 64540, 64541, 64542, 64543},
			Communities:         []uint32{0xAABBCCDD, 0x11223344, 0x55667788},
			LargeCommunities:    [][3]uint32{{64538, 1, 1}, {64539, 2, 2}},
			ExtendedCommunities: []uint64{0x0000000000000001},
			NextHop:             net.ParseIP("172.16.0.6"),
		},
		{
			Prefix:              mustParseCIDR("8.8.8.0/24"),
			Peer:                net.ParseIP("10.0.0.7"),
			PeerAS:              64544,
			ASPath:              []uint32{64544, 64545, 64546, 64547, 64548, 64549, 64550},
			Communities:         []uint32{0xFFFFFFFF},
			LargeCommunities:    [][3]uint32{},
			ExtendedCommunities: []uint64{},
			NextHop:             net.ParseIP("172.16.0.7"),
		},
		{
			Prefix:              mustParseCIDR("1.1.1.0/24"),
			Peer:                net.ParseIP("10.0.0.8"),
			PeerAS:              64551,
			ASPath:              []uint32{64551, 64552, 64553, 64554, 64555, 64556},
			Communities:         []uint32{0x00010002, 0x00030004},
			LargeCommunities:    [][3]uint32{{64551, 77, 88}},
			ExtendedCommunities: []uint64{0xABCDEF0123456789},
			NextHop:             net.ParseIP("172.16.0.8"),
		},
		{
			Prefix:              mustParseCIDR("9.9.9.0/24"),
			Peer:                net.ParseIP("10.0.0.9"),
			PeerAS:              64557,
			ASPath:              []uint32{64557, 64558, 64559, 64560, 64561, 64562, 64563, 64564, 64565},
			Communities:         []uint32{},
			LargeCommunities:    [][3]uint32{{64557, 0, 0}, {64558, 1, 1}},
			ExtendedCommunities: []uint64{0x9999999999999999, 0x8888888888888888},
			NextHop:             net.ParseIP("172.16.0.9"),
		},
		{
			Prefix:              mustParseCIDR("185.199.108.0/22"),
			Peer:                net.ParseIP("10.0.0.10"),
			PeerAS:              64566,
			ASPath:              []uint32{64566, 64567, 64568, 64569, 64570, 64571},
			Communities:         []uint32{0x12341234},
			LargeCommunities:    [][3]uint32{{64566, 42, 42}},
			ExtendedCommunities: []uint64{0x4242424242424242},
			NextHop:             net.ParseIP("172.16.0.10"),
		},
		{
			Prefix:              mustParseCIDR("140.82.112.0/20"),
			Peer:                net.ParseIP("10.0.0.11"),
			PeerAS:              64572,
			ASPath:              []uint32{64572, 64573, 64574, 64575, 64576, 64577, 64578},
			Communities:         []uint32{0xBEEFBEEF, 0xDEADDEAD},
			LargeCommunities:    [][3]uint32{{64572, 7, 7}, {64573, 8, 8}, {64574, 9, 9}},
			ExtendedCommunities: []uint64{},
			NextHop:             net.ParseIP("172.16.0.11"),
		},
		{
			Prefix:              mustParseCIDR("13.107.42.0/24"),
			Peer:                net.ParseIP("10.0.0.12"),
			PeerAS:              64579,
			ASPath:              []uint32{64579, 64580, 64581, 64582, 64583, 64584, 64585},
			Communities:         []uint32{0x01010101},
			LargeCommunities:    [][3]uint32{},
			ExtendedCommunities: []uint64{0x1212121212121212},
			NextHop:             net.ParseIP("172.16.0.12"),
		},
		{
			Prefix:              mustParseCIDR("20.190.159.0/24"),
			Peer:                net.ParseIP("10.0.0.13"),
			PeerAS:              64586,
			ASPath:              []uint32{64586, 64587, 64588, 64589, 64590, 64591},
			Communities:         []uint32{0x22222222, 0x33333333},
			LargeCommunities:    [][3]uint32{{64586, 100, 100}},
			ExtendedCommunities: []uint64{0x1414141414141414, 0x1515151515151515},
			NextHop:             net.ParseIP("172.16.0.13"),
		},
		{
			Prefix:              mustParseCIDR("52.94.76.0/22"),
			Peer:                net.ParseIP("10.0.0.14"),
			PeerAS:              64592,
			ASPath:              []uint32{64592, 64593, 64594, 64595, 64596, 64597, 64598, 64599},
			Communities:         []uint32{},
			LargeCommunities:    [][3]uint32{{64592, 0, 1}, {64593, 0, 2}},
			ExtendedCommunities: []uint64{},
			NextHop:             net.ParseIP("172.16.0.14"),
		},
		{
			Prefix:              mustParseCIDR("104.16.0.0/12"),
			Peer:                net.ParseIP("10.0.0.15"),
			PeerAS:              64600,
			ASPath:              []uint32{64600, 64601, 64602, 64603, 64604, 64605, 64606},
			Communities:         []uint32{0x44444444},
			LargeCommunities:    [][3]uint32{{64600, 255, 255}},
			ExtendedCommunities: []uint64{0x1616161616161616},
			NextHop:             net.ParseIP("172.16.0.15"),
		},
	}
}

package controlapi

import (
	"fmt"
	"testing"
	"time"

	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/ca"
	"github.com/docker/swarmkit/manager/state/store"
	"github.com/docker/swarmkit/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func createClusterSpec(name string) *api.ClusterSpec {
	return &api.ClusterSpec{
		Annotations: api.Annotations{
			Name: name,
		},
		CAConfig: api.CAConfig{
			NodeCertExpiry: ptypes.DurationProto(ca.DefaultNodeCertExpiration),
		},
	}
}

func createClusterObj(id, name string, policy api.AcceptancePolicy, rootCA *ca.RootCA) *api.Cluster {
	spec := createClusterSpec(name)
	spec.AcceptancePolicy = policy

	return &api.Cluster{
		ID:   id,
		Spec: *spec,
		RootCA: api.RootCA{
			CACert:     []byte("-----BEGIN CERTIFICATE-----AwEHoUQDQgAEZ4vGYkSt/kjoHbUjDx9eyO1xBVJEH2F+AwM9lACIZ414cD1qYy8u-----BEGIN CERTIFICATE-----"),
			CAKey:      []byte("-----BEGIN EC PRIVATE KEY-----AwEHoUQDQgAEZ4vGYkSt/kjoHbUjDx9eyO1xBVJEH2F+AwM9lACIZ414cD1qYy8u-----END EC PRIVATE KEY-----"),
			CACertHash: "hash",
			JoinTokens: api.JoinTokens{
				Worker:  ca.GenerateJoinToken(rootCA),
				Manager: ca.GenerateJoinToken(rootCA),
			},
		},
	}
}

func createCluster(t *testing.T, ts *testServer, id, name string, policy api.AcceptancePolicy, rootCA *ca.RootCA) *api.Cluster {
	cluster := createClusterObj(id, name, policy, rootCA)
	assert.NoError(t, ts.Store.Update(func(tx store.Tx) error {
		return store.CreateCluster(tx, cluster)
	}))
	return cluster
}

func TestValidateClusterSpec(t *testing.T) {
	type BadClusterSpec struct {
		spec *api.ClusterSpec
		c    codes.Code
	}

	for _, bad := range []BadClusterSpec{
		{
			spec: nil,
			c:    codes.InvalidArgument,
		},
		{
			spec: &api.ClusterSpec{
				Annotations: api.Annotations{
					Name: "name",
				},
				CAConfig: api.CAConfig{
					NodeCertExpiry: ptypes.DurationProto(29 * time.Minute),
				},
			},
			c: codes.InvalidArgument,
		},
		{
			spec: &api.ClusterSpec{
				Annotations: api.Annotations{
					Name: "name",
				},
				Dispatcher: api.DispatcherConfig{
					HeartbeatPeriod: ptypes.DurationProto(-29 * time.Minute),
				},
			},
			c: codes.InvalidArgument,
		},
	} {
		err := validateClusterSpec(bad.spec)
		assert.Error(t, err)
		assert.Equal(t, bad.c, grpc.Code(err))
	}

	for _, good := range []*api.ClusterSpec{
		createClusterSpec("name"),
	} {
		err := validateClusterSpec(good)
		assert.NoError(t, err)
	}
}

func TestGetCluster(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	_, err := ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpc.Code(err))

	_, err = ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{ClusterID: "invalid"})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, grpc.Code(err))

	cluster := createCluster(t, ts, "name", "name", api.AcceptancePolicy{}, ts.Server.rootCA)
	r, err := ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{ClusterID: cluster.ID})
	assert.NoError(t, err)
	cluster.Meta.Version = r.Cluster.Meta.Version
	// Only public fields should be available
	assert.Equal(t, cluster.ID, r.Cluster.ID)
	assert.Equal(t, cluster.Meta, r.Cluster.Meta)
	assert.Equal(t, cluster.Spec, r.Cluster.Spec)
	assert.Equal(t, cluster.RootCA.CACert, r.Cluster.RootCA.CACert)
	assert.Equal(t, cluster.RootCA.CACertHash, r.Cluster.RootCA.CACertHash)
	// CAKey and network keys should be nil
	assert.Nil(t, r.Cluster.RootCA.CAKey)
	assert.Nil(t, r.Cluster.NetworkBootstrapKeys)
}

func TestGetClusterWithSecret(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	_, err := ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpc.Code(err))

	_, err = ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{ClusterID: "invalid"})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, grpc.Code(err))

	policy := api.AcceptancePolicy{Policies: []*api.AcceptancePolicy_RoleAdmissionPolicy{{Secret: &api.AcceptancePolicy_RoleAdmissionPolicy_Secret{Data: []byte("secret")}}}}
	cluster := createCluster(t, ts, "name", "name", policy, ts.Server.rootCA)
	r, err := ts.Client.GetCluster(context.Background(), &api.GetClusterRequest{ClusterID: cluster.ID})
	assert.NoError(t, err)
	cluster.Meta.Version = r.Cluster.Meta.Version
	assert.NotEqual(t, cluster, r.Cluster)
	assert.NotContains(t, r.Cluster.String(), "secret")
	assert.NotContains(t, r.Cluster.String(), "PRIVATE")
	assert.NotNil(t, r.Cluster.Spec.AcceptancePolicy.Policies[0].Secret.Data)
}

func TestUpdateCluster(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	cluster := createCluster(t, ts, "name", "name", api.AcceptancePolicy{}, ts.Server.rootCA)

	_, err := ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpc.Code(err))

	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{ClusterID: "invalid", Spec: &cluster.Spec, ClusterVersion: &api.Version{}})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, grpc.Code(err))

	// No update options.
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{ClusterID: cluster.ID, Spec: &cluster.Spec})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpc.Code(err))

	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{ClusterID: cluster.ID, Spec: &cluster.Spec, ClusterVersion: &cluster.Meta.Version})
	assert.NoError(t, err)

	r, err := ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	assert.Equal(t, cluster.Spec.Annotations.Name, r.Clusters[0].Spec.Annotations.Name)
	assert.Len(t, r.Clusters[0].Spec.AcceptancePolicy.Policies, 0)

	r.Clusters[0].Spec.AcceptancePolicy = api.AcceptancePolicy{Policies: []*api.AcceptancePolicy_RoleAdmissionPolicy{{Secret: &api.AcceptancePolicy_RoleAdmissionPolicy_Secret{Alg: "bcrypt", Data: []byte("secret")}}}}
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &r.Clusters[0].Spec,
		ClusterVersion: &r.Clusters[0].Meta.Version,
	})
	assert.NoError(t, err)

	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	assert.Equal(t, cluster.Spec.Annotations.Name, r.Clusters[0].Spec.Annotations.Name)
	assert.Len(t, r.Clusters[0].Spec.AcceptancePolicy.Policies, 1)

	r.Clusters[0].Spec.AcceptancePolicy = api.AcceptancePolicy{Policies: []*api.AcceptancePolicy_RoleAdmissionPolicy{{Secret: &api.AcceptancePolicy_RoleAdmissionPolicy_Secret{Alg: "bcrypt", Data: []byte("secret")}}}}
	returnedCluster, err := ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &r.Clusters[0].Spec,
		ClusterVersion: &r.Clusters[0].Meta.Version,
	})
	assert.NoError(t, err)
	assert.NotContains(t, returnedCluster.String(), "secret")
	assert.NotContains(t, returnedCluster.String(), "PRIVATE")
	assert.NotNil(t, returnedCluster.Cluster.Spec.AcceptancePolicy.Policies[0].Secret.Data)

	// Versioning.
	assert.NoError(t, err)
	version := &returnedCluster.Cluster.Meta.Version

	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &r.Clusters[0].Spec,
		ClusterVersion: version,
	})
	assert.NoError(t, err)

	// Perform an update with the "old" version.
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &r.Clusters[0].Spec,
		ClusterVersion: version,
	})
	assert.Error(t, err)
}

func TestUpdateClusterRotateToken(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	cluster := createCluster(t, ts, "name", "name", api.AcceptancePolicy{}, ts.Server.rootCA)

	r, err := ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})

	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	workerToken := r.Clusters[0].RootCA.JoinTokens.Worker
	managerToken := r.Clusters[0].RootCA.JoinTokens.Manager

	// Rotate worker token
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &cluster.Spec,
		ClusterVersion: &cluster.Meta.Version,
		Rotation: api.KeyRotation{
			WorkerJoinToken: true,
		},
	})
	assert.NoError(t, err)

	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	assert.NotEqual(t, workerToken, r.Clusters[0].RootCA.JoinTokens.Worker)
	assert.Equal(t, managerToken, r.Clusters[0].RootCA.JoinTokens.Manager)
	workerToken = r.Clusters[0].RootCA.JoinTokens.Worker

	// Rotate manager token
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &cluster.Spec,
		ClusterVersion: &r.Clusters[0].Meta.Version,
		Rotation: api.KeyRotation{
			ManagerJoinToken: true,
		},
	})
	assert.NoError(t, err)

	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	assert.Equal(t, workerToken, r.Clusters[0].RootCA.JoinTokens.Worker)
	assert.NotEqual(t, managerToken, r.Clusters[0].RootCA.JoinTokens.Manager)
	managerToken = r.Clusters[0].RootCA.JoinTokens.Manager

	// Rotate both tokens
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &cluster.Spec,
		ClusterVersion: &r.Clusters[0].Meta.Version,
		Rotation: api.KeyRotation{
			WorkerJoinToken:  true,
			ManagerJoinToken: true,
		},
	})
	assert.NoError(t, err)

	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
		Filters: &api.ListClustersRequest_Filters{
			NamePrefixes: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Clusters, 1)
	assert.NotEqual(t, workerToken, r.Clusters[0].RootCA.JoinTokens.Worker)
	assert.NotEqual(t, managerToken, r.Clusters[0].RootCA.JoinTokens.Manager)
}

func TestUpdateClusterRotateUnlockKey(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	// create a cluster with extra encryption keys, to make sure they exist
	cluster := createClusterObj("id", "name", api.AcceptancePolicy{}, ts.Server.rootCA)
	expected := make(map[string]*api.EncryptionKey)
	for i := 1; i <= 2; i++ {
		value := fmt.Sprintf("fake%d", i)
		expected[value] = &api.EncryptionKey{Subsystem: value, Key: []byte(value)}
		cluster.UnlockKeys = append(cluster.UnlockKeys, expected[value])
	}
	require.NoError(t, ts.Store.Update(func(tx store.Tx) error {
		return store.CreateCluster(tx, cluster)
	}))

	// we have to get the key from the memory store, since the cluster returned by the API is redacted
	getManagerKey := func() (managerKey *api.EncryptionKey) {
		ts.Store.View(func(tx store.ReadTx) {
			viewCluster := store.GetCluster(tx, cluster.ID)
			// no matter whether there's a manager key or not, the other keys should not have been affected
			foundKeys := make(map[string]*api.EncryptionKey)
			for _, eKey := range viewCluster.UnlockKeys {
				foundKeys[eKey.Subsystem] = eKey
			}
			for v, key := range expected {
				foundKey, ok := foundKeys[v]
				require.True(t, ok)
				require.Equal(t, key, foundKey)
			}
			managerKey = foundKeys[ca.ManagerRole]
		})
		return
	}

	validateListResult := func(expectedLocked bool) api.Version {
		r, err := ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{
			Filters: &api.ListClustersRequest_Filters{
				NamePrefixes: []string{"name"},
			},
		})

		require.NoError(t, err)
		require.Len(t, r.Clusters, 1)
		require.Equal(t, expectedLocked, r.Clusters[0].Spec.EncryptionConfig.AutoLockManagers)
		require.Nil(t, r.Clusters[0].UnlockKeys) // redacted

		return r.Clusters[0].Meta.Version
	}

	// we start off with manager autolocking turned off
	version := validateListResult(false)
	require.Nil(t, getManagerKey())

	// Rotate unlock key without turning auto-lock on - key should still be nil
	_, err := ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &cluster.Spec,
		ClusterVersion: &version,
		Rotation: api.KeyRotation{
			ManagerUnlockKey: true,
		},
	})
	require.NoError(t, err)
	version = validateListResult(false)
	require.Nil(t, getManagerKey())

	// Enable auto-lock only, no rotation boolean
	spec := cluster.Spec.Copy()
	spec.EncryptionConfig.AutoLockManagers = true
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           spec,
		ClusterVersion: &version,
	})
	require.NoError(t, err)
	version = validateListResult(true)
	managerKey := getManagerKey()
	require.NotNil(t, managerKey)

	// Rotate the manager key
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           spec,
		ClusterVersion: &version,
		Rotation: api.KeyRotation{
			ManagerUnlockKey: true,
		},
	})
	require.NoError(t, err)
	version = validateListResult(true)
	newManagerKey := getManagerKey()
	require.NotNil(t, managerKey)
	require.NotEqual(t, managerKey, newManagerKey)
	managerKey = newManagerKey

	// Just update the cluster without modifying unlock keys
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           spec,
		ClusterVersion: &version,
	})
	require.NoError(t, err)
	version = validateListResult(true)
	newManagerKey = getManagerKey()
	require.Equal(t, managerKey, newManagerKey)

	// Disable auto lock
	_, err = ts.Client.UpdateCluster(context.Background(), &api.UpdateClusterRequest{
		ClusterID:      cluster.ID,
		Spec:           &cluster.Spec, // set back to original spec
		ClusterVersion: &version,
		Rotation: api.KeyRotation{
			ManagerUnlockKey: true, // this will be ignored because we disable the auto-lock
		},
	})
	require.NoError(t, err)
	validateListResult(false)
	require.Nil(t, getManagerKey())
}

func TestListClusters(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	r, err := ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Empty(t, r.Clusters)

	createCluster(t, ts, "id1", "name1", api.AcceptancePolicy{}, ts.Server.rootCA)
	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r.Clusters))

	createCluster(t, ts, "id2", "name2", api.AcceptancePolicy{}, ts.Server.rootCA)
	createCluster(t, ts, "id3", "name3", api.AcceptancePolicy{}, ts.Server.rootCA)
	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(r.Clusters))
}

func TestListClustersWithSecrets(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Stop()
	r, err := ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Empty(t, r.Clusters)

	policy := api.AcceptancePolicy{Policies: []*api.AcceptancePolicy_RoleAdmissionPolicy{{Secret: &api.AcceptancePolicy_RoleAdmissionPolicy_Secret{Alg: "bcrypt", Data: []byte("secret")}}}}

	createCluster(t, ts, "id1", "name1", policy, ts.Server.rootCA)
	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r.Clusters))

	createCluster(t, ts, "id2", "name2", policy, ts.Server.rootCA)
	createCluster(t, ts, "id3", "name3", policy, ts.Server.rootCA)
	r, err = ts.Client.ListClusters(context.Background(), &api.ListClustersRequest{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(r.Clusters))
	for _, cluster := range r.Clusters {
		assert.NotContains(t, cluster.String(), policy.Policies[0].Secret)
		assert.NotContains(t, cluster.String(), "PRIVATE")
		assert.NotNil(t, cluster.Spec.AcceptancePolicy.Policies[0].Secret.Data)
	}
}

func TestExpireBlacklistedCerts(t *testing.T) {
	now := time.Now()

	longAgo := now.Add(-24 * time.Hour * 1000)
	justBeforeGrace := now.Add(-expiredCertGrace - 5*time.Minute)
	justAfterGrace := now.Add(-expiredCertGrace + 5*time.Minute)
	future := now.Add(time.Hour)

	cluster := &api.Cluster{
		BlacklistedCertificates: map[string]*api.BlacklistedCertificate{
			"longAgo":         {Expiry: ptypes.MustTimestampProto(longAgo)},
			"justBeforeGrace": {Expiry: ptypes.MustTimestampProto(justBeforeGrace)},
			"justAfterGrace":  {Expiry: ptypes.MustTimestampProto(justAfterGrace)},
			"future":          {Expiry: ptypes.MustTimestampProto(future)},
		},
	}

	expireBlacklistedCerts(cluster)

	assert.Len(t, cluster.BlacklistedCertificates, 2)

	_, hasJustAfterGrace := cluster.BlacklistedCertificates["justAfterGrace"]
	assert.True(t, hasJustAfterGrace)

	_, hasFuture := cluster.BlacklistedCertificates["future"]
	assert.True(t, hasFuture)
}

package e2e_test

import (
	"context"
	"fmt"
	"log"
	mrand "math/rand"
	"strings"
	"testing"

	"github.com/Azure/agentbakere2e/scenario"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"k8s.io/apimachinery/pkg/util/errors"
)

const (
	managedClusterResourceType = "Microsoft.ContainerService/managedClusters"
)

type parameters map[string]string

type parameterCache map[string]parameters

type clusterConfig struct {
	cluster    *armcontainerservice.ManagedCluster
	kube       *kubeclient
	parameters parameters
	subnetId   string
}

// Returns true if the cluster is configured with Azure CNI
func (c clusterConfig) isAzureCNI() (bool, error) {
	if c.cluster.Properties.NetworkProfile != nil {
		return *c.cluster.Properties.NetworkProfile.NetworkPlugin == armcontainerservice.NetworkPluginAzure, nil
	}
	return false, fmt.Errorf("cluster network profile was nil:\n%+v", c.cluster)
}

// Returns the maximum number of pods per node of the cluster's agentpool
func (c clusterConfig) maxPodsPerNode() (int, error) {
	if len(c.cluster.Properties.AgentPoolProfiles) > 0 {
		return int(*c.cluster.Properties.AgentPoolProfiles[0].MaxPods), nil
	}
	return 0, fmt.Errorf("cluster agentpool profiles were nil or empty:\n%+v", c.cluster)
}

func (c clusterConfig) needsPreparation() bool {
	return c.kube == nil || c.parameters == nil || c.subnetId == ""
}

func isExistingResourceGroup(ctx context.Context, cloud *azureClient, resourceGroupName string) (bool, error) {
	rgExistence, err := cloud.resourceGroupClient.CheckExistence(ctx, resourceGroupName, nil)
	if err != nil {
		return false, fmt.Errorf("failed to get RG %q: %w", resourceGroupName, err)
	}

	return rgExistence.Success, nil
}

func ensureResourceGroup(ctx context.Context, t *testing.T, cloud *azureClient, resourceGroupName string) error {
	t.Logf("ensuring resource group %q...", resourceGroupName)

	rgExists, err := isExistingResourceGroup(ctx, cloud, resourceGroupName)
	if err != nil {
		return err
	}

	if !rgExists {
		_, err = cloud.resourceGroupClient.CreateOrUpdate(
			ctx,
			resourceGroupName,
			armresources.ResourceGroup{
				Location: to.Ptr(e2eTestLocation),
				Name:     to.Ptr(resourceGroupName),
			},
			nil)

		if err != nil {
			return fmt.Errorf("failed to create RG %q: %w", resourceGroupName, err)
		}
	}

	return nil
}

func validateExistingClusterState(
	ctx context.Context,
	t *testing.T,
	cloud *azureClient,
	resourceGroupName string,
	clusterModel *armcontainerservice.ManagedCluster) (bool, error) {
	var needRecreate bool
	clusterName := *clusterModel.Name

	cluster, err := cloud.aksClient.Get(ctx, resourceGroupName, clusterName, nil)
	if err != nil {
		if isResourceNotFoundError(err) {
			t.Logf("received ResourceNotFound error when trying to GET test cluster %q", clusterName)
			needRecreate = true
		} else {
			return false, fmt.Errorf("failed to get aks cluster %q: %w", clusterName, err)
		}
	} else {
		// We only need to check the MC resource group + cluster properties if the cluster resource itself exists
		rgExists, err := isExistingResourceGroup(ctx, cloud, *clusterModel.Properties.NodeResourceGroup)
		if err != nil {
			return false, err
		}

		if !rgExists || cluster.Properties == nil || cluster.Properties.ProvisioningState == nil || *cluster.Properties.ProvisioningState == "Failed" {
			t.Logf("deleting test cluster in bad state: %q", clusterName)

			needRecreate = true
			if err := deleteExistingCluster(ctx, cloud, resourceGroupName, clusterName); err != nil {
				return false, fmt.Errorf("failed to delete cluster in bad state: %w", err)
			}
		}
	}

	return needRecreate, nil
}

func createNewCluster(
	ctx context.Context,
	cloud *azureClient,
	resourceGroupName string,
	clusterModel *armcontainerservice.ManagedCluster) (*armcontainerservice.ManagedCluster, error) {
	pollerResp, err := cloud.aksClient.BeginCreateOrUpdate(
		ctx,
		resourceGroupName,
		*clusterModel.Name,
		*clusterModel,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to begin aks cluster creation: %w", err)
	}

	clusterResp, err := pollerResp.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for aks cluster creation %w", err)
	}

	return &clusterResp.ManagedCluster, nil
}

func deleteExistingCluster(ctx context.Context, cloud *azureClient, resourceGroupName, clusterName string) error {
	poller, err := cloud.aksClient.BeginDelete(ctx, resourceGroupName, clusterName, nil)
	if err != nil {
		return fmt.Errorf("failed to start aks cluster %q deletion: %w", clusterName, err)
	}

	_, err = poller.PollUntilDone(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to wait for aks cluster %q deletion: %w", clusterName, err)
	}

	return nil
}

func getClusterSubnetID(ctx context.Context, cloud *azureClient, location, mcResourceGroupName, clusterName string) (string, error) {
	pager := cloud.vnetClient.NewListPager(mcResourceGroupName, nil)

	for pager.More() {
		nextResult, err := pager.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to advance page: %w", err)
		}
		for _, v := range nextResult.Value {
			if v == nil {
				return "", fmt.Errorf("aks vnet id was empty")
			}
			return fmt.Sprintf("%s/subnets/%s", *v.ID, "aks-subnet"), nil
		}
	}

	return "", fmt.Errorf("failed to find aks vnet")
}

func getInitialClusterConfigs(ctx context.Context, t *testing.T, cloud *azureClient, resourceGroupName string) ([]clusterConfig, error) {
	configs := []clusterConfig{}
	pager := cloud.resourceClient.NewListByResourceGroupPager(resourceGroupName, nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to advance page: %w", err)
		}
		for _, resource := range page.Value {
			if strings.EqualFold(*resource.Type, managedClusterResourceType) {
				cluster, err := cloud.aksClient.Get(ctx, resourceGroupName, *resource.Name, nil)
				if err != nil {
					if isNotFoundError(err) {
						log.Printf("get aks cluster %q returned 404 Not Found, continuing to list clusters...", *resource.Name)
						continue
					} else {
						return nil, fmt.Errorf("failed to get aks cluster: %w", err)
					}
				}
				if cluster.Properties == nil {
					return nil, fmt.Errorf("aks cluster properties were nil")
				}

				if *cluster.Properties.ProvisioningState == "Deleting" {
					continue
				}

				t.Logf("found agentbaker e2e cluster: %q", *cluster.Name)
				configs = append(configs, clusterConfig{cluster: &cluster.ManagedCluster})
			}
		}
	}

	return configs, nil
}

func getViableConfigs(scenario *scenario.Scenario, clusterConfigs []clusterConfig) []clusterConfig {
	viableConfigs := []clusterConfig{}
	for _, config := range clusterConfigs {
		if scenario.Config.ClusterSelector(config.cluster) {
			viableConfigs = append(viableConfigs, config)
		}
	}
	return viableConfigs
}

func hasViableConfig(scenario *scenario.Scenario, clusterConfigs []clusterConfig) bool {
	for _, config := range clusterConfigs {
		if scenario.Config.ClusterSelector(config.cluster) {
			return true
		}
	}
	return false
}

func createMissingClusters(
	ctx context.Context,
	t *testing.T,
	r *mrand.Rand,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	scenarios scenario.Table,
	paramCache parameterCache,
	clusterConfigs *[]clusterConfig) error {
	var newConfigs []clusterConfig
	for _, scenario := range scenarios {
		if !hasViableConfig(scenario, *clusterConfigs) && !hasViableConfig(scenario, newConfigs) {
			newClusterModel := getNewClusterModelForScenario(generateClusterName(r), suiteConfig.location, scenario)
			newConfigs = append(newConfigs, clusterConfig{cluster: &newClusterModel})
		}
	}

	var clusterCreateFuncs []func() error
	for i, c := range newConfigs {
		config := c
		idx := i
		createFunc := func() error {
			clusterName := *config.cluster.Name

			log.Printf("creating cluster %q...", clusterName)
			liveCluster, err := createNewCluster(ctx, cloud, suiteConfig.resourceGroupName, config.cluster)
			if err != nil {
				return fmt.Errorf("unable to create new cluster: %w", err)
			}

			if liveCluster.Properties == nil {
				return fmt.Errorf("newly created cluster model has nil properties:\n%+v", liveCluster)
			}

			log.Printf("preparing cluster %q for testing...", clusterName)
			kube, subnetId, clusterParams, err := prepareClusterForTests(ctx, t, cloud, suiteConfig, liveCluster, paramCache)
			if err != nil {
				return fmt.Errorf("unable to prepare viable cluster for testing: %s", err)
			}

			newConfigs[idx].cluster = liveCluster
			newConfigs[idx].kube = kube
			newConfigs[idx].parameters = clusterParams
			newConfigs[idx].subnetId = subnetId
			return nil
		}

		clusterCreateFuncs = append(clusterCreateFuncs, createFunc)
	}

	if err := errors.AggregateGoroutines(clusterCreateFuncs...); err != nil {
		return fmt.Errorf("aggregate cluster result contained errors:\n%w", err)
	}

	*clusterConfigs = append(*clusterConfigs, newConfigs...)
	return nil
}

func mustChooseCluster(
	ctx context.Context,
	t *testing.T,
	r *mrand.Rand,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	scenario *scenario.Scenario,
	paramCache parameterCache,
	clusterConfigs []clusterConfig) clusterConfig {
	var chosenConfig clusterConfig
	for _, viableConfig := range getViableConfigs(scenario, clusterConfigs) {
		var err error
		cluster := viableConfig.cluster
		clusterName := *cluster.Name

		needRecreate, err := validateExistingClusterState(ctx, t, cloud, suiteConfig.resourceGroupName, cluster)
		if err != nil {
			t.Logf("unable to validate state of viable cluster %q: %s", clusterName, err)
			continue
		}

		if needRecreate {
			t.Logf("viable cluster %q is in a bad state, attempting to recreate...", clusterName)

			cluster, err = createNewCluster(ctx, cloud, suiteConfig.resourceGroupName, cluster)
			if err != nil {
				t.Logf("unable to recreate viable cluster %q: %s", clusterName, err)
				continue
			}
		}

		if needRecreate || viableConfig.needsPreparation() {
			kube, subnetId, clusterParams, err := prepareClusterForTests(ctx, t, cloud, suiteConfig, cluster, paramCache)
			if err != nil {
				t.Logf("unable to prepare viable cluster for testing: %s", err)
				continue
			}
			viableConfig.cluster = cluster
			viableConfig.kube = kube
			viableConfig.parameters = clusterParams
			viableConfig.subnetId = subnetId
		}

		chosenConfig = viableConfig
		break
	}

	if chosenConfig.cluster == nil || chosenConfig.needsPreparation() {
		t.Fatalf("unable to successfully choose a cluster for scenario %q", scenario.Name)
	}

	if chosenConfig.cluster.Properties.NodeResourceGroup == nil {
		t.Fatalf("tried to chose a cluster without a node resource group:\n%+v", *chosenConfig.cluster)
	}

	return chosenConfig
}

func prepareClusterForTests(
	ctx context.Context,
	t *testing.T,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	cluster *armcontainerservice.ManagedCluster,
	paramCache parameterCache) (*kubeclient, string, parameters, error) {
	clusterName := *cluster.Name

	subnetID, err := getClusterSubnetID(ctx, cloud, suiteConfig.location, *cluster.Properties.NodeResourceGroup, clusterName)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable get subnet ID of cluster %q: %w", clusterName, err)
	}

	kube, err := getClusterKubeClient(ctx, cloud, suiteConfig.resourceGroupName, clusterName)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable get kube client using cluster %q: %w", clusterName, err)
	}

	if err := ensureDebugDaemonset(ctx, kube); err != nil {
		return nil, "", nil, fmt.Errorf("unable to ensure debug damonset of viable cluster %q: %w", clusterName, err)
	}

	clusterParams, err := getClusterParametersWithCache(ctx, t, kube, clusterName, paramCache)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to get cluster paramters: %w", err)
	}

	return kube, subnetID, clusterParams, nil
}

func getClusterParametersWithCache(
	ctx context.Context,
	t *testing.T,
	kube *kubeclient,
	clusterName string,
	paramCache parameterCache) (parameters, error) {
	cachedParams, ok := paramCache[clusterName]
	if !ok {
		params, err := pollExtractClusterParameters(ctx, t, kube)
		if err != nil {
			return nil, fmt.Errorf("unable to extract cluster parameters from %q: %w", clusterName, err)
		}
		paramCache[clusterName] = params
		return params, nil
	} else {
		return cachedParams, nil
	}
}

func getNewClusterModelForScenario(clusterName, location string, scenario *scenario.Scenario) armcontainerservice.ManagedCluster {
	baseModel := getBaseClusterModel(clusterName, location)
	if scenario.ClusterMutator != nil {
		scenario.ClusterMutator(&baseModel)
	}
	return baseModel
}

func generateClusterName(r *mrand.Rand) string {
	return fmt.Sprintf(testClusterNameTemplate, randomLowercaseString(r, 5))
}

func getBaseClusterModel(clusterName, location string) armcontainerservice.ManagedCluster {
	return armcontainerservice.ManagedCluster{
		Name:     to.Ptr(clusterName),
		Location: to.Ptr(location),
		Properties: &armcontainerservice.ManagedClusterProperties{
			DNSPrefix: to.Ptr(clusterName),
			AgentPoolProfiles: []*armcontainerservice.ManagedClusterAgentPoolProfile{
				{
					Name:         to.Ptr("nodepool1"),
					Count:        to.Ptr[int32](2),
					VMSize:       to.Ptr("Standard_DS2_v2"),
					MaxPods:      to.Ptr[int32](110),
					OSType:       to.Ptr(armcontainerservice.OSTypeLinux),
					Type:         to.Ptr(armcontainerservice.AgentPoolTypeVirtualMachineScaleSets),
					Mode:         to.Ptr(armcontainerservice.AgentPoolModeSystem),
					OSDiskSizeGB: to.Ptr[int32](512),
				},
			},
			NetworkProfile: &armcontainerservice.NetworkProfile{
				NetworkPlugin: to.Ptr(armcontainerservice.NetworkPluginKubenet),
			},
		},
		Identity: &armcontainerservice.ManagedClusterIdentity{
			Type: to.Ptr(armcontainerservice.ResourceIdentityTypeSystemAssigned),
		},
	}
}

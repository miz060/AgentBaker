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

type paramCache map[string]map[string]string

type clusterCreationErrors []error

func (errs clusterCreationErrors) Error() string {
	if len(errs) == 1 {
		return errs[0].Error()
	}

	msg := "encountered multiple cluster creation errors:"
	for _, err := range errs {
		msg += fmt.Sprintf("\n%s", err.Error())
	}
	return msg
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

func listClusters(ctx context.Context, t *testing.T, cloud *azureClient, resourceGroupName string) ([]*armcontainerservice.ManagedCluster, error) {
	clusters := []*armcontainerservice.ManagedCluster{}
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
				clusters = append(clusters, &cluster.ManagedCluster)
			}
		}
	}

	return clusters, nil
}

func getViableClusters(scenario *scenario.Scenario, clusters []*armcontainerservice.ManagedCluster) []*armcontainerservice.ManagedCluster {
	viableClusters := []*armcontainerservice.ManagedCluster{}
	for _, cluster := range clusters {
		if scenario.Config.ClusterSelector(cluster) {
			viableClusters = append(viableClusters, cluster)
		}
	}
	return viableClusters
}

func hasViableCluster(scenario *scenario.Scenario, clusters []*armcontainerservice.ManagedCluster) bool {
	for _, cluster := range clusters {
		if scenario.Config.ClusterSelector(cluster) {
			return true
		}
	}
	return false
}

func createMissingClusters(
	ctx context.Context,
	r *mrand.Rand,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	scenarios scenario.Table,
	existingClusters *[]*armcontainerservice.ManagedCluster) error {
	var newClusters []*armcontainerservice.ManagedCluster
	for _, scenario := range scenarios {
		if !hasViableCluster(scenario, *existingClusters) && !hasViableCluster(scenario, newClusters) {
			newClusterModel := getNewClusterModelForScenario(generateClusterName(r), suiteConfig.location, scenario)
			newClusters = append(newClusters, &newClusterModel)
		}
	}

	var clusterCreateFuncs []func() error
	newLiveClusters := make([]*armcontainerservice.ManagedCluster, len(newClusters))
	for i, c := range newClusters {
		cluster := c
		idx := i
		createFunc := func() error {
			log.Printf("creating cluster %q...", *cluster.Name)
			newCluster, err := createNewCluster(ctx, cloud, suiteConfig.resourceGroupName, cluster)
			if err != nil {
				return fmt.Errorf("unable to create new cluster: %w", err)
			}

			if newCluster.Properties == nil {
				return fmt.Errorf("newly created cluster model has nil properties:\n%+v", cluster)
			}

			newLiveClusters[idx] = newCluster
			return nil
		}

		clusterCreateFuncs = append(clusterCreateFuncs, createFunc)
	}

	aggErr := errors.AggregateGoroutines(clusterCreateFuncs...)

	for _, liveCluster := range newLiveClusters {
		if liveCluster != nil {
			*existingClusters = append(*existingClusters, liveCluster)
		}
	}

	if aggErr != nil {
		return fmt.Errorf("aggregate cluster creation result contains errors:\n%w", aggErr)
	}

	return nil
}

func mustChooseCluster(
	ctx context.Context,
	t *testing.T,
	r *mrand.Rand,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	scenario *scenario.Scenario,
	clusters []*armcontainerservice.ManagedCluster,
	paramCache paramCache) (*kubeclient, *armcontainerservice.ManagedCluster, map[string]string, string) {
	var (
		chosenKubeClient    *kubeclient
		chosenCluster       *armcontainerservice.ManagedCluster
		chosenClusterParams map[string]string
		chosenSubnetID      string
	)

	for _, viableCluster := range getViableClusters(scenario, clusters) {
		var cluster *armcontainerservice.ManagedCluster

		needRecreate, err := validateExistingClusterState(ctx, t, cloud, suiteConfig.resourceGroupName, viableCluster)
		if err != nil {
			t.Logf("unable to validate state of viable cluster %q: %s", *viableCluster.Name, err)
			continue
		}

		if needRecreate {
			t.Logf("viable cluster %q is in a bad state, attempting to recreate...", *viableCluster.Name)

			newCluster, err := createNewCluster(ctx, cloud, suiteConfig.resourceGroupName, viableCluster)
			if err != nil {
				t.Logf("unable to recreate viable cluster %q: %s", *viableCluster.Name, err)
				continue
			}
			cluster = newCluster
		} else {
			cluster = viableCluster
		}

		kube, subnetID, clusterParams, err := prepareClusterForTests(ctx, t, cloud, suiteConfig, cluster, paramCache)
		if err != nil {
			t.Logf("unable to prepare viable cluster for testing: %s", err)
			continue
		}

		chosenCluster = cluster
		chosenKubeClient = kube
		chosenSubnetID = subnetID
		chosenClusterParams = clusterParams
		break
	}

	if chosenCluster == nil {
		t.Fatalf("unable to successfully choose a cluster for scenario %q", scenario.Name)
	}

	if chosenCluster.Properties.NodeResourceGroup == nil {
		t.Fatalf("tried to chose a cluster without a node resource group:\n%+v", *chosenCluster)
	}

	return chosenKubeClient, chosenCluster, chosenClusterParams, chosenSubnetID
}

func prepareClusterForTests(
	ctx context.Context,
	t *testing.T,
	cloud *azureClient,
	suiteConfig *suiteConfig,
	cluster *armcontainerservice.ManagedCluster,
	paramCache paramCache) (*kubeclient, string, map[string]string, error) {
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

func getClusterParametersWithCache(ctx context.Context, t *testing.T, kube *kubeclient, clusterName string, paramCache paramCache) (map[string]string, error) {
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

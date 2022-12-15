/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"time"

	"sigs.k8s.io/cluster-api/controllers/remote"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	infrav1 "github.com/athlonxpgzw/cluster-api-provider-docker/api/v1alpha1"
	"github.com/athlonxpgzw/cluster-api-provider-docker/pkg/container"
	"github.com/athlonxpgzw/cluster-api-provider-docker/pkg/docker"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	bootstrapv1 "sigs.k8s.io/cluster-api/bootstrap/kubeadm/api/v1beta1"
	"sigs.k8s.io/kind/pkg/cluster/constants"
)

// DockerMachineReconciler reconciles a DockerMachine object
type DockerMachineReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ContainerRuntime container.Runtime
	Tracker          *remote.ClusterCacheTracker
}

//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=dockermachines,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=dockermachines/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=dockermachines/finalizers,verbs=update
//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machine/status,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DockerMachine object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.1/pkg/reconcile
func (r *DockerMachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconcile request received")

	ctx = container.RuntimeInto(ctx, r.ContainerRuntime)

	// Fetch the dockerMachine instance
	dockerMachine := &infrav1.DockerMachine{}
	err := r.Client.Get(ctx, req.NamespacedName, dockerMachine)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Fetch the Machine
	machine, err := util.GetOwnerMachine(ctx, r.Client, dockerMachine.ObjectMeta)
	if err != nil {
		logger.Error(err, "failed to get Owner Machine")
		return ctrl.Result{}, err
	}
	if machine == nil {
		logger.Info("Waiting for Machine Controller to set OwnerRef on DockerMachine")
		return ctrl.Result{}, nil
	}

	// Fetch the cluster
	cluster, err := util.GetClusterFromMetadata(ctx, r.Client, dockerMachine.ObjectMeta)
	if err != nil {
		logger.Error(err, "DockerMachine owner Machine is missing Cluster label or Cluster does not exists")
		return ctrl.Result{}, err
	}
	if cluster == nil {
		logger.Info(fmt.Sprintf("Please associate the machine with a cluster using the label %s: <name of cluster>", clusterv1.ClusterLabelName))
		return ctrl.Result{}, nil
	}
	logger = logger.WithValues(cluster.Name)

	// Fetch the DockerCluster
	dockerCluster := &infrav1.DockerCluster{}
	dockerClusterName := client.ObjectKey{
		Namespace: dockerCluster.Namespace,
		Name:      cluster.Spec.InfrastructureRef.Name,
	}
	if err = r.Client.Get(ctx, dockerClusterName, dockerCluster); err != nil {
		logger.Error(err, "failed to get DockerCluster")
		return ctrl.Result{}, err
	}

	// Initalize the patch helper
	patchHelper, err := patch.NewHelper(dockerMachine, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	defer func() {
		if err = patchDockerMachine(ctx, patchHelper, dockerMachine); err != nil {
			logger.Error(err, "failed to patch DockerMachine")
			reterr = err
		}
	}()

	// Return early if the object or cluster is paused
	if annotations.IsPaused(cluster, dockerMachine) {
		logger.Info("dockerMachine or linked cluster is marked as paused, won't reconcile")
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(dockerMachine, infrav1.MachineFinalizer) {
		controllerutil.AddFinalizer(dockerMachine, infrav1.MachineFinalizer)
		return ctrl.Result{}, nil
	}

	extMachine, err := docker.NewMachine(ctx, cluster, machine.Name, nil)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to create helper to managing the externalMachine")
	}

	extLoadBalancer, err := docker.NewLoadBalancer(ctx, cluster, dockerCluster)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to create helper for managing the externalLoadBalancer")
	}

	// Handle deleted machines
	if !dockerMachine.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, cluster, machine, dockerMachine, extMachine, extLoadBalancer)
	}

	// Handle non-delete machines
	return r.reconcileNormal(ctx, cluster, machine, dockerMachine, extMachine, extLoadBalancer)
}

func (r *DockerMachineReconciler) reconcileNormal(ctx context.Context, cluster *clusterv1.Cluster, machine *clusterv1.Machine, dockerMachine *infrav1.DockerMachine, extMachine *docker.Machine, extLoadBalancer *docker.LoadBalancer) (_ ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)

	// Check if infrastructure is ready, otherwise return and wait for the cluster object updated.
	if !cluster.Status.InfrastructureReady {
		logger.Info("Waiting for Docker Cluster controller to create cluster infrastructure")
		return ctrl.Result{}, nil
	}

	// If the machine is already provisioned, return
	if dockerMachine.Spec.ProviderID != nil {
		dockerMachine.Status.Ready = true
		return ctrl.Result{}, nil
	}

	// Make sure bootstrap data is available and populated
	if machine.Spec.Bootstrap.DataSecretName == nil {
		if !util.IsControlPlaneMachine(machine) && !conditions.IsTrue(cluster, clusterv1.ControlPlaneInitializedCondition) {
			logger.Info("Waiting for the control plane to be initialized")
			return ctrl.Result{}, nil
		}
		logger.Info("Waiting for the bootstrap controller to set bootstrap data")
		return ctrl.Result{}, nil
	}

	// Create the docker container hosting the machine
	role := constants.WorkerNodeRoleValue
	if util.IsControlPlaneMachine(machine) {
		role = constants.ControlPlaneNodeRoleValue
	}

	// Create the machine if not existing yet
	if !extMachine.Exists() {
		if err := extMachine.Create(ctx, dockerMachine.Spec.CustomImage, role, machine.Spec.Version, docker.FailureDomainLabel(machine.Spec.FailureDomain), nil); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to create worker DockerMachine")
		}
	}

	// If the machine is a controlplane update the loadbalancer configuration
	// we should only do this once, as reconfiguration more or less ensures node ref setting fails
	if util.IsControlPlaneMachine(machine) && !dockerMachine.Status.LoadBalancerConfigured {
		if err := extLoadBalancer.UpdateConfiguration(ctx); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update DockerCluster.LoadBalancer configurations")
		}
		dockerMachine.Status.LoadBalancerConfigured = true
	}

	if !dockerMachine.Spec.Boostrapped {
		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
		if err := extMachine.CheckForBootstrapSuccess(timeoutCtx, false); err != nil {
			bootstrapData, format, err := r.GetBootstrapData(timeoutCtx, machine)
			if err != nil {
				logger.Error(err, "failed to get bootstrap data")
				return ctrl.Result{}, err
			}

			// Run the bootstrap script. Simulates cloud-init/Ignition.
			if err := extMachine.ExecBootstrap(timeoutCtx, bootstrapData, format); err != nil {
				conditions.MarkFalse(dockerMachine, infrav1.BootstrapExecSucceededCondition, infrav1.BootstrapFailedReason, clusterv1.ConditionSeverityWarning, "Repeating bootstrap")
				return ctrl.Result{}, errors.Wrap(err, "failed to exec DockerMachine bootstrap")
			}

			// Check for bootstrap success
			if err := extMachine.CheckForBootstrapSuccess(timeoutCtx, true); err != nil {
				conditions.MarkFalse(dockerMachine, infrav1.BootstrapExecSucceededCondition, infrav1.BootstrapFailedReason, clusterv1.ConditionSeverityWarning, "Repeating bootstrap")
				return ctrl.Result{}, errors.Wrap(err, "failed to check for existence of bootstrap success file at /run/cluster-api/bootstrap-success.complete")
			}
		}
		dockerMachine.Spec.Boostrapped = true
	}

	if err := setMachineAddress(ctx, dockerMachine, extMachine); err != nil {
		logger.Error(err, "Failed to set the machine address")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	remoteClient, err := r.Tracker.GetClient(ctx, client.ObjectKeyFromObject(cluster))
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to generate workload cluster client")
	}
	if err := extMachine.SetNodeProviderID(ctx, remoteClient); err != nil {
		if errors.As(err, &docker.ContainerNotRunningError{}) {
			return ctrl.Result{}, errors.Wrap(err, "failed to patch the kubernetes node with the machine ProviderID")
		}
		logger.Error(err, "failed to patch the kubernetes node with the machine ProviderID")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	providerID := extMachine.ProviderID()
	dockerMachine.Spec.ProviderID = &providerID

	dockerMachine.Status.Ready = true

	return ctrl.Result{}, nil
}

func (r *DockerMachineReconciler) GetBootstrapData(ctx context.Context, machine *clusterv1.Machine) (string, bootstrapv1.Format, error) {
	if machine.Spec.Bootstrap.DataSecretName == nil {
		return "", "", errors.New("error retriving bootstrap data: linked Machine's bootstrap.dataSecretName is nil")
	}

	s := &corev1.Secret{}
	key := client.ObjectKey{Namespace: machine.GetNamespace(), Name: *machine.Spec.Bootstrap.DataSecretName}
	if err := r.Client.Get(ctx, key, s); err != nil {
		return "", "", errors.Wrapf(err, "failed to retrieve bootstrap data secret for DockerMachine %s", klog.KObj(machine))
	}

	value, ok := s.Data["value"]
	if !ok {
		return "", "", errors.New("error retrieving bootstrap Data: secret value is missing")
	}

	format := s.Data["format"]
	if len(format) == 0 {
		format = []byte(bootstrapv1.CloudConfig)
	}

	return base64.StdEncoding.EncodeToString(value), bootstrapv1.Format(format), nil
}

// setMachineAddress gets the address from the container corresponding to a docker node and sets it on the machine object
func setMachineAddress(ctx context.Context, dockerMachine *infrav1.DockerMachine, extMachine *docker.Machine) error {
	machineAddress, err := extMachine.Address(ctx)
	if err != nil {
		return err
	}

	dockerMachine.Status.Addresses = []clusterv1.MachineAddress{
		{
			Type:    clusterv1.MachineHostName,
			Address: extMachine.ContainerName(),
		},
		{
			Type:    clusterv1.MachineInternalIP,
			Address: machineAddress,
		},
		{
			Type:    clusterv1.MachineExternalIP,
			Address: machineAddress,
		},
	}
	return nil
}

func (r *DockerMachineReconciler) reconcileDelete(ctx context.Context, cluster *clusterv1.Cluster, machine *clusterv1.Machine, dockerMachine *infrav1.DockerMachine, extMachine *docker.Machine, extLoadBalancer *docker.LoadBalancer) (_ ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)

	patchHelper, err := patch.NewHelper(dockerMachine, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	conditions.MarkFalse(dockerMachine, infrav1.ContainerProvisionedCondition, clusterv1.DeletedReason, clusterv1.ConditionSeverityInfo, "")

	if err := patchDockerMachine(ctx, patchHelper, dockerMachine); err != nil && retErr == nil {
		logger.Error(err, "failed to patch dockerMachine")
		retErr = err
	}

	// Delete the machine
	if err := extMachine.Delete(ctx); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to delete dockerMachine")
	}

	// If the deleted machine is a control plane node, remote it from the loadbalancer configuration
	if util.IsControlPlaneMachine(machine) {
		if err := extLoadBalancer.UpdateConfiguration(ctx); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update DockerCluster.loadbalancer configuration ")
		}
	}

	// Machine is deleted so remove the finalizer.
	controllerutil.RemoveFinalizer(dockerMachine, infrav1.MachineFinalizer)

	return ctrl.Result{}, nil
}

func patchDockerMachine(ctx context.Context, patchHelper *patch.Helper, dockerMachine *infrav1.DockerMachine) error {
	conditions.SetSummary(dockerMachine,
		conditions.WithConditions(
			infrav1.ContainerProvisionedCondition,
			infrav1.BootstrapExecSucceededCondition,
		),
		conditions.WithStepCounterIf(dockerMachine.ObjectMeta.DeletionTimestamp.IsZero() && dockerMachine.Spec.ProviderID == nil),
	)
	// Patch the object, ignoring conflicts on the conditions owned by the controller
	return patchHelper.Patch(
		ctx,
		dockerMachine,
		patch.WithOwnedConditions{Conditions: []clusterv1.ConditionType{
			clusterv1.ReadyCondition,
			infrav1.BootstrapExecSucceededCondition,
			infrav1.ContainerProvisionedCondition,
		}},
	)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DockerMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	var (
		controlledType     = &infrav1.DockerMachine{}
		controlledTypeName = reflect.TypeOf(controlledType).Elem().Name()
		controlledTypeGVK  = infrav1.GroupVersion.WithKind(controlledTypeName)
	)

	return ctrl.NewControllerManagedBy(mgr).
		For(controlledType).
		Watches(
			&source.Kind{Type: &clusterv1.Machine{}},
			handler.EnqueueRequestsFromMapFunc(util.MachineToInfrastructureMapFunc(controlledTypeGVK)),
		).Complete(r)
}

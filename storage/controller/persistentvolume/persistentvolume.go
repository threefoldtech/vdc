package persistentvolume

import (
	"context"
	"fmt"
	"strconv"

	ctrl "github.com/threefoldtech/vdc/storage/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ReconcilePersistentVolume reconciles a PersistentVolume object.
type ReconcilePersistentVolume struct {
	client client.Client
	config ctrl.Config
}

var _ reconcile.Reconciler = &ReconcilePersistentVolume{}
var _ ctrl.ContollerManager = &ReconcilePersistentVolume{}

// Init will add the ReconcilePersistentVolume to the list.
func Init() {
	// add ReconcilePersistentVolume to the list
	ctrl.ControllerList = append(ctrl.ControllerList, ReconcilePersistentVolume{})
}

// Add adds the newPVReconciler.
func (r ReconcilePersistentVolume) Add(mgr manager.Manager, config ctrl.Config) error {
	return add(mgr, newPVReconciler(mgr, config))
}

// newReconciler returns a ReconcilePersistentVolume.
func newPVReconciler(mgr manager.Manager, config ctrl.Config) reconcile.Reconciler {
	r := &ReconcilePersistentVolume{
		client: mgr.GetClient(),
		config: config,
	}
	return r
}

func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("persistentvolume-controller", mgr, controller.Options{MaxConcurrentReconciles: 1, Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to PersistentVolumes
	err = c.Watch(&source.Kind{Type: &corev1.PersistentVolume{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}
	return nil
}

func checkStaticVolume(pv *corev1.PersistentVolume) (bool, error) {
	static := false
	var err error

	staticVol := pv.Spec.CSI.VolumeAttributes["staticVolume"]
	if staticVol != "" {
		static, err = strconv.ParseBool(staticVol)
		if err != nil {
			return false, fmt.Errorf("failed to parse preProvisionedVolume: %w", err)
		}
	}
	return static, nil
}

// reconcilePV will extract the image details from the pv spec and regenerates
// the omap data.
func (r ReconcilePersistentVolume) reconcilePV(obj runtime.Object) error {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return nil
	}
	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == r.config.DriverName {
		// requestName := pv.Name
		// imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
		// volumeHandler := pv.Spec.CSI.VolumeHandle
		//secretName := ""
		//secretNamespace := ""
		// check static volume
		static, err := checkStaticVolume(pv)
		if err != nil {
			return err
		}
		// if the volume is static, dont generate OMAP data
		if static {
			return nil
		}
		/* 	if pv.Spec.CSI.ControllerExpandSecretRef != nil {
			secretName = pv.Spec.CSI.ControllerExpandSecretRef.Name
			secretNamespace = pv.Spec.CSI.ControllerExpandSecretRef.Namespace
		} else if pv.Spec.CSI.NodeStageSecretRef != nil {
			secretName = pv.Spec.CSI.NodeStageSecretRef.Name
			secretNamespace = pv.Spec.CSI.NodeStageSecretRef.Namespace
		}

		cr, err := r.getCredentials(secretName, secretNamespace)
		if err != nil {
			util.ErrorLogMsg("failed to get credentials %s", err)
			return err
		}
		defer cr.DeleteCredentials()

		err = rbd.RegenerateJournal(imageName, volumeHandler, pool, journalPool, requestName, cr)
		if err != nil {
			util.ErrorLogMsg("failed to regenerate journal %s", err)
			return err
		} */
	}
	return nil
}

// Reconcile reconciles the PersitentVolume object and creates a new omap entries
// for the volume.
func (r *ReconcilePersistentVolume) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	pv := &corev1.PersistentVolume{}
	err := r.client.Get(context.TODO(), request.NamespacedName, pv)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// Check if the object is under deletion
	if !pv.GetDeletionTimestamp().IsZero() {
		return reconcile.Result{}, nil
	}

	err = r.reconcilePV(pv)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
